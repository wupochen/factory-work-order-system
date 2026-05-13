import streamlit as st
import pandas as pd
import requests
import os
import altair as alt
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import gspread
from google.oauth2.service_account import Credentials
from estimate_tools import render_estimate_tool

# --- 0. Streamlit 頁面設定 (必須在最前面) ---
st.set_page_config(page_title="工廠生產管理系統 V5.2.2 (防限流快取版)", layout="wide")

# --- 1. 系統常數、密碼與時區設定 ---
TAIWAN_TZ = ZoneInfo("Asia/Taipei")

# Google Sheets 是主資料庫，factory_db.csv 只作為本機備份
DB_FILE = 'factory_db.csv' 
DEFAULT_EMPS = [
    "劉信佑", "詹聰寶", "李昱緯", "陳思豪", "林辰諺", 
    "陳俊誠", "吳譽鉫", "陳義棋", "黃聖翔", "吳柏漢", "邱郁琮"
]

# 機台類型與中越對照表
MACHINE_TYPES = ["磨床", "放電機", "快走絲"]
MACHINE_TYPES_BILINGUAL = {
    "磨床": "磨床 / Máy mài",
    "放電機": "放電機 / Máy EDM",
    "快走絲": "快走絲 / Máy cắt dây nhanh"
}

# 生產類型
PROD_TYPES = ["正常生產", "插件", "NG重修", "NG重製", "重製"]
PROD_TYPES_BILINGUAL = {
    "正常生產": "正常生產 / Sản xuất bình thường",
    "插件": "插件 / Công việc chen ngang",
    "NG重修": "NG重修 / Sửa lại NG",
    "NG重製": "NG重製 / Làm lại NG",
    "重製": "重製 / Làm lại"
}

# 暫停原因與中越對照表
PAUSE_REASONS = ["下班未完成", "臨時插件", "等料", "等主管確認", "機台異常", "其他"]
PAUSE_REASONS_BILINGUAL = {
    "下班未完成": "下班未完成 / Tan ca nhưng chưa hoàn thành",
    "臨時插件": "臨時插件 / Công việc chen ngang",
    "等料": "等料 / Chờ vật liệu",
    "等主管確認": "等主管確認 / Chờ quản lý xác nhận",
    "機台異常": "機台異常 / Máy móc bất thường",
    "其他": "其他 / Khác"
}

ADMIN_PASSWORD = "0000"

# STANDARD_COLS (work_orders)
STANDARD_COLS = [
    '工單ID', '日期', '填寫人', '生產類型', '機台類型', '工單號碼', '圖號', '工件數量', '預估工時',
    '實際工時', '開始時間', '結束時間', '工作區間工時',
    '累積工作區間工時', '最後恢復時間', '暫停時間', '暫停原因',
    '時間差異', '狀態', '備註'
]

# NG_COLS (ng_records)
NG_COLS = [
    "NG_ID", "建立時間", "發生日期", "發現人", "責任人", "機台類型", "工單ID", 
    "工單號碼", "圖號", "工件數量", "生產類型", "NG類型", "NG說明", 
    "處理方式", "狀態", "備註", "更新時間"
]

if "is_admin" not in st.session_state:
    st.session_state["is_admin"] = False

# LINE API
try:
    LINE_CHANNEL_ACCESS_TOKEN = st.secrets.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    LINE_TO_ID = st.secrets.get("LINE_TO_ID", "")
except Exception:
    LINE_CHANNEL_ACCESS_TOKEN = LINE_TO_ID = ""

# --- 2. Google Sheets 連線與核心資料存取 ---

@st.cache_resource
def get_gsheet_client():
    if "gcp_service_account" not in st.secrets or "GSHEET_ID" not in st.secrets:
        st.error("❌ Google Sheets 連線失敗，請檢查 Streamlit Secrets。")
        return None
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        skey = st.secrets["gcp_service_account"]
        credentials = Credentials.from_service_account_info(skey, scopes=scopes)
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"❌ Google Sheets 連線失敗：{str(e)}")
        return None

def normalize_db_df(df):
    num_cols = ['工件數量', '預估工時', '實際工時', '工作區間工時', '累積工作區間工時', '時間差異']
    for c in df.columns:
        if c in num_cols:
            if c == '工件數量': df[c] = pd.to_numeric(df[c], errors='coerce').fillna(1).astype(int)
            else: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
        else: df[c] = df[c].fillna("").astype(str)
    return df

def parse_taiwan_time(value):
    try:
        if pd.isna(value) or str(value).strip() == "": return pd.NaT
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt): return pd.NaT
        return dt.tz_localize(TAIWAN_TZ) if dt.tzinfo is None else dt.tz_convert(TAIWAN_TZ)
    except Exception: return pd.NaT

def calculate_work_hours_excluding_lunch(start_dt, end_dt):
    try:
        if pd.isna(start_dt) or pd.isna(end_dt): return 0.0
        start_dt = start_dt.astimezone(TAIWAN_TZ) if start_dt.tzinfo else start_dt.replace(tzinfo=TAIWAN_TZ)
        end_dt = end_dt.astimezone(TAIWAN_TZ) if end_dt.tzinfo else end_dt.replace(tzinfo=TAIWAN_TZ)
        if end_dt <= start_dt: return 0.0
        total_seconds = (end_dt - start_dt).total_seconds()
        lunch_seconds = 0
        current_day = start_dt.date()
        while current_day <= end_dt.date():
            l_start = datetime.combine(current_day, datetime.min.time()).replace(hour=12, minute=0, tzinfo=TAIWAN_TZ)
            l_end = datetime.combine(current_day, datetime.min.time()).replace(hour=13, minute=0, tzinfo=TAIWAN_TZ)
            overlap = (min(end_dt, l_end) - max(start_dt, l_start)).total_seconds()
            if overlap > 0: lunch_seconds += overlap
            current_day += timedelta(days=1)
        return round(max(0.0, (total_seconds - lunch_seconds) / 3600), 2)
    except Exception: return 0.0

def calculate_elapsed_hours(start_dt, end_dt):
    try:
        if pd.isna(start_dt) or pd.isna(end_dt): return 0.0
        start_dt = start_dt.astimezone(TAIWAN_TZ) if start_dt.tzinfo else start_dt.replace(tzinfo=TAIWAN_TZ)
        end_dt = end_dt.astimezone(TAIWAN_TZ) if end_dt.tzinfo else end_dt.replace(tzinfo=TAIWAN_TZ)
        if end_dt <= start_dt: return 0.0
        return round(max(0.0, (end_dt - start_dt).total_seconds() / 3600), 2)
    except Exception: return 0.0

def get_diff_color(x):
    """計算時間差異的圖表顏色"""
    if x > 0:
        return "red"
    elif x < 0:
        return "green"
    else:
        return "gray"

@st.cache_resource
def init_gsheets_once():
    gc = get_gsheet_client()
    if not gc: return
    try:
        sh = gc.open_by_key(st.secrets["GSHEET_ID"])
        # work_orders
        try:
            wo_sheet = sh.worksheet("work_orders")
            h = wo_sheet.row_values(1)
            missing = [c for c in STANDARD_COLS if c not in h]
            if missing: wo_sheet.update(values=[h + missing], range_name='A1')
        except gspread.exceptions.WorksheetNotFound:
            sh.add_worksheet("work_orders", rows="100", cols="20").append_row(STANDARD_COLS)
        
        # ng_records (只補欄位，不刪資料)
        try:
            ng_sheet = sh.worksheet("ng_records")
            h_ng = ng_sheet.row_values(1)
            if not h_ng:
                ng_sheet.append_row(NG_COLS)
            else:
                missing_ng = [c for c in NG_COLS if c not in h_ng]
                if missing_ng:
                    new_h = h_ng + missing_ng
                    ng_sheet.update(values=[new_h], range_name='A1')
        except gspread.exceptions.WorksheetNotFound:
            sh.add_worksheet("ng_records", rows="100", cols="20").append_row(NG_COLS)
            
    except Exception as e:
        st.error(f"初始化 Google Sheets 失敗: {e}")

# --- 資料讀取與寫入 ---
def load_work_orders_raw():
    gc = get_gsheet_client()
    if not gc: return pd.DataFrame(columns=STANDARD_COLS)
    try:
        data = gc.open_by_key(st.secrets["GSHEET_ID"]).worksheet("work_orders").get_all_records()
        df = pd.DataFrame(data) if data else pd.DataFrame(columns=STANDARD_COLS)
        for c in STANDARD_COLS: 
            if c not in df.columns: df[c] = 0.0 if c in ['預估工時','實際工時','工作區間工時','累積工作區間工時','時間差異'] else ""
        return normalize_db_df(df[STANDARD_COLS])
    except: return pd.DataFrame(columns=STANDARD_COLS)

@st.cache_data(ttl=30)
def load_work_orders_cached(): return load_work_orders_raw()

def load_ng_records_raw():
    gc = get_gsheet_client()
    if not gc: return pd.DataFrame(columns=NG_COLS)
    try:
        data = gc.open_by_key(st.secrets["GSHEET_ID"]).worksheet("ng_records").get_all_records()
        df = pd.DataFrame(data) if data else pd.DataFrame(columns=NG_COLS)
        for c in NG_COLS:
            if c not in df.columns: df[c] = ""
        return df[NG_COLS]
    except: return pd.DataFrame(columns=NG_COLS)

@st.cache_data(ttl=30)
def load_ng_records_cached(): return load_ng_records_raw()

def load_employees_raw():
    """從 Google Sheets 讀取最新員工名單 (僅在新增/刪除員工時呼叫)"""
    gc = get_gsheet_client()
    if not gc: return DEFAULT_EMPS
    try:
        sh = gc.open_by_key(st.secrets["GSHEET_ID"])
        worksheet = sh.worksheet("employees")
        data = worksheet.get_all_records()
        
        if not data:
            worksheet.update(values=[["員工名字"]] + [[e] for e in DEFAULT_EMPS], range_name='A1')
            return DEFAULT_EMPS
            
        df = pd.DataFrame(data)
        if "員工名字" not in df.columns:
            worksheet.update(values=[["員工名字"]] + [[e] for e in DEFAULT_EMPS], range_name='A1')
            return DEFAULT_EMPS
            
        emps = df["員工名字"].dropna().astype(str).str.strip().tolist()
        emps = list(dict.fromkeys([e for e in emps if e]))
        
        missing = [e for e in DEFAULT_EMPS if e not in emps]
        if missing:
            emps.extend(missing)
            worksheet.clear()
            worksheet.update(values=[["員工名字"]] + [[e] for e in emps], range_name='A1')
        return emps
    except Exception as e:
        return DEFAULT_EMPS

@st.cache_data(ttl=60)
def load_employees_cached():
    return load_employees_raw()

def save_work_orders(df):
    gc = get_gsheet_client()
    if not gc: return
    try:
        ws = gc.open_by_key(st.secrets["GSHEET_ID"]).worksheet("work_orders")
        ws.clear()
        ws.update(values=[df.columns.tolist()] + df.fillna("").values.tolist(), range_name='A1')
        df.to_csv(DB_FILE, index=False, encoding='utf-8-sig')
        st.cache_data.clear()
    except Exception as e: st.error(f"寫入失敗: {e}")

def save_ng_records(df):
    gc = get_gsheet_client()
    if not gc: return
    try:
        ws = gc.open_by_key(st.secrets["GSHEET_ID"]).worksheet("ng_records")
        ws.clear()
        ws.update(values=[df.columns.tolist()] + df.fillna("").values.tolist(), range_name='A1')
        st.cache_data.clear()
    except Exception as e: st.error(f"NG 寫入失敗: {e}")

def backup_factory_db():
    """主管操作前的獨立備份 (強制讀取最新資料)"""
    df = load_work_orders_raw()
    backup_dir = "backup"
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    now_str = datetime.now(TAIWAN_TZ).strftime('%Y%m%d_%H%M%S')
    backup_filename = f"factory_db_backup_{now_str}.csv"
    backup_path = os.path.join(backup_dir, backup_filename)
    df.to_csv(backup_path, index=False, encoding='utf-8-sig')
    return backup_path

def append_work_order(row_dict):
    gc = get_gsheet_client()
    if not gc: return
    try:
        ws = gc.open_by_key(st.secrets["GSHEET_ID"]).worksheet("work_orders")
        ws.append_row([str(row_dict.get(c, "")) for c in STANDARD_COLS])
        st.cache_data.clear()
    except Exception as e: st.error(f"新增失敗: {e}")

def append_ng_record(row_dict):
    """新增單筆紀錄至 ng_records 工作表"""
    gc = get_gsheet_client()
    if not gc: return
    try:
        ws = gc.open_by_key(st.secrets["GSHEET_ID"]).worksheet("ng_records")
        ws.append_row([str(row_dict.get(c, "")) for c in NG_COLS])
        st.cache_data.clear()
    except Exception as e: st.error(f"NG 紀錄寫入失敗: {e}")

def send_line_message(msg):
    if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_TO_ID: return False, "憑證未設定"
    try:
        resp = requests.post("https://api.line.me/v2/bot/message/push", 
                             headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"},
                             json={"to": LINE_TO_ID, "messages": [{"type": "text", "text": msg}]}, timeout=5)
        return (True, "") if resp.status_code == 200 else (False, f"HTTP {resp.status_code}")
    except Exception as e: return False, str(e)

def send_unfinished_work_orders_reminder(trigger_label="定時檢查"):
    try:
        df = load_work_orders_raw()
        if df.empty: return
        unfinished_df = df[df['狀態'].isin(['進行中', '暫停中'])]
        
        now_dt = datetime.now(TAIWAN_TZ)
        now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

        if unfinished_df.empty:
            msg = f"✅ 工單檢查通知\n提醒類型：{trigger_label}\n目前沒有未結案工單。\n時間：台灣時間 {now_str}"
        else:
            msg = f"🔔 未結案工單提醒\n提醒類型：{trigger_label}\n時間：台灣時間 {now_str}\n\n目前仍有以下工單尚未完成："
            
            count = 1
            for _, row in unfinished_df.iterrows():
                status = row['狀態']
                machine = row.get('機台類型', '磨床')
                wo_no_str = row.get('工單號碼', '未填寫') if row.get('工單號碼') else '未填寫'
                
                if status == '進行中':
                    resume_dt = parse_taiwan_time(row.get('最後恢復時間', ''))
                    if pd.isna(resume_dt): resume_dt = parse_taiwan_time(row['開始時間'])
                    segment_h = 0.0 if pd.isna(resume_dt) else calculate_work_hours_excluding_lunch(resume_dt, now_dt)
                        
                    old_acc = pd.to_numeric(row.get('累積工作區間工時', 0), errors='coerce')
                    if pd.isna(old_acc): old_acc = 0.0
                    current_total_h = round(old_acc + segment_h, 2)
                    
                    msg += (f"\n\n{count}. 人員：{row['填寫人']}\n   機台：{machine}\n   狀態：{status}\n"
                            f"   類型：{row['生產類型']}\n   工單號碼：{wo_no_str}\n   圖號：{row['圖號']}\n"
                            f"   數量：{row.get('工件數量', 1)}\n   開始時間：{row['開始時間']}\n   未結案經過時間：{current_total_h}h")
                            
                elif status == '暫停中':
                    old_acc = pd.to_numeric(row.get('累積工作區間工時', 0), errors='coerce')
                    if pd.isna(old_acc): old_acc = 0.0
                    current_total_h = round(old_acc, 2)
                    pause_reason = row.get('暫停原因', '未填寫')
                    
                    msg += (f"\n\n{count}. 人員：{row['填寫人']}\n   機台：{machine}\n   狀態：{status}\n"
                            f"   類型：{row['生產類型']}\n   工單號碼：{wo_no_str}\n   圖號：{row['圖號']}\n"
                            f"   數量：{row.get('工件數量', 1)}\n   開始時間：{row['開始時間']}\n"
                            f"   暫停原因：{pause_reason}\n   未結案經過時間：{current_total_h}h")
                count += 1

        send_line_message(msg)

    except Exception as e:
        print(f"未結案提醒發生例外錯誤：{e}")

@st.cache_resource
def init_scheduler():
    scheduler = BackgroundScheduler(timezone=TAIWAN_TZ)
    scheduler.add_job(
        send_unfinished_work_orders_reminder,
        CronTrigger(hour=16, minute=55, timezone=TAIWAN_TZ),
        args=["下班前 16:55 檢查"]
    )
    scheduler.add_job(
        send_unfinished_work_orders_reminder,
        CronTrigger(hour=18, minute=0, timezone=TAIWAN_TZ),
        args=["下班後 18:00 檢查"]
    )
    scheduler.add_job(
        send_unfinished_work_orders_reminder,
        CronTrigger(hour=21, minute=0, timezone=TAIWAN_TZ),
        args=["晚上 21:00 加班檢查"]
    )
    scheduler.start()
    return scheduler

init_gsheets_once()
init_scheduler()

# --- 3. 網頁 UI 介面 ---
is_print_mode = False

with st.sidebar:
    st.title("🔐 主管權限")
    if not st.session_state["is_admin"]:
        pwd = st.text_input("主管密碼", type="password")
        if st.button("登入主管模式"):
            if pwd == ADMIN_PASSWORD:
                st.session_state["is_admin"] = True
                st.success("✅ 已進入主管模式")
                st.rerun()
            else: st.error("❌ 密碼錯誤")
    else:
        st.success("✅ 已登入")
        if st.button("登出"): st.session_state["is_admin"] = False; st.rerun()

    st.divider()

    if st.session_state["is_admin"]:
        st.title("⚙️ 系統設定")
        is_print_mode = st.checkbox("🖨️ 開啟列印月報模式", value=False)
        
        if not is_print_mode:
            st.divider()
            with st.expander("👤 人員名單維護"):
                current_list_disp = load_employees_cached()
                new_emp = st.text_input("新增員工姓名").strip()
                if st.button("確認新增"):
                    latest_emps = load_employees_raw()
                    if not new_emp: st.warning("請輸入姓名。")
                    elif new_emp in latest_emps: st.error("姓名已在名單中。")
                    else:
                        latest_emps.append(new_emp)
                        gc = get_gsheet_client()
                        if gc:
                            gc.open_by_key(st.secrets["GSHEET_ID"]).worksheet("employees").clear()
                            gc.open_by_key(st.secrets["GSHEET_ID"]).worksheet("employees").update(values=[["員工名字"]] + [[e] for e in latest_emps], range_name='A1')
                            st.cache_data.clear()
                            st.success(f"已新增：{new_emp}"); st.rerun()
            
            st.divider()
            st.subheader("💬 LINE 通知設定狀態")
            if LINE_CHANNEL_ACCESS_TOKEN: st.write("✅ LINE Token 已設定")
            else: st.write("❌ LINE_CHANNEL_ACCESS_TOKEN 尚未設定")

            if LINE_TO_ID: st.write("✅ LINE_TO_ID 已設定")
            else: st.write("❌ LINE_TO_ID 尚未設定")

            st.write("🕓 未結案提醒：每日 16:55、18:00、21:00 自動推播")

            if st.button("🔔 測試未結案工單提醒"):
                send_unfinished_work_orders_reminder("手動測試")
                st.success("✅ 未結案工單提醒指令已送出！")
    else:
        st.info("🔒 系統設定需主管密碼")

# 集中讀取快取資料給各頁籤共用 (減少 API 讀取次數)
emps = load_employees_cached()
db_df = load_work_orders_cached()

if not is_print_mode:
    tab1, tab2, tab_est, tab_ng, tab3, tab4 = st.tabs([
        "🏗️ 磨床報工",
        "⚡ 放電機/快走絲報工",
        "🧮 預估工時",
        "🚨 NG 管理",
        "📊 主管數據看板",
        "🛠️ 主管後台管理"
    ])
else:
    tab1 = st.empty()
    tab2 = st.empty()
    tab_est = st.empty()
    tab_ng = st.empty()
    tab3 = st.container()
    tab4 = st.empty()

# --- 頁籤 1：磨床報工 ---
with tab1:
    st.header("磨床即時加工報工 / Báo cáo gia công máy mài")
    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            s_name = st.selectbox("填寫人 / Người điền", emps, key="g_name")
            s_type = st.selectbox("生產類型 / Loại sản xuất", PROD_TYPES, format_func=lambda x: PROD_TYPES_BILINGUAL.get(x, x), key="g_type")
        with c2:
            g_wo_no = st.text_input("工單號碼 / Số lệnh sản xuất", key="g_wo_no").strip()
            s_drawing = st.text_input("圖號 / Bản vẽ", key="g_drawing").strip()
        with c3:
            s_qty = st.number_input("工件數量 / Số lượng sản phẩm", min_value=1, value=1, step=1, key="g_qty")
            s_est = st.number_input("預估工時 / TG dự kiến (hrs)", min_value=0.0, step=0.1, key="g_est")

        # --- 動態 NG 欄位 ---
        g_disc = g_resp = g_ng_type = g_ng_note = g_remake_note = ""
        try: def_idx = emps.index(s_name)
        except: def_idx = 0

        if s_type in ["NG重修", "NG重製"]:
            st.divider()
            nc1, nc2, nc3, nc4 = st.columns(4)
            with nc1: g_disc = st.selectbox("發現人 / Người phát hiện", emps, index=def_idx, key="g_disc")
            with nc2: g_resp = st.selectbox("責任人 / Người phụ trách", emps, index=def_idx, key="g_resp")
            with nc3: g_ng_type = st.text_input("NG 類型 / Loại NG", placeholder="例如：尺寸偏大", key="g_ngt").strip()
            with nc4: g_ng_note = st.text_area("NG 說明 / Giải thích NG", key="g_ngn").strip()
        elif s_type == "重製":
            st.divider()
            rc1, rc2, rc3 = st.columns([1, 1, 2])
            with rc1: g_disc = st.selectbox("發現人 / Người phát hiện", emps, index=def_idx, key="g_r_disc")
            with rc2: g_resp = st.selectbox("責任人 / Người phụ trách", emps, index=def_idx, key="g_r_resp")
            with rc3: g_remake_note = st.text_area("重製原因 / Lý do làm lại", key="g_rmn").strip()
        # -------------------

        if st.button("▶️ 開始加工 / Bắt đầu gia công", type="primary", key="g_start"):
            if not s_drawing: st.error("❌ 請輸入圖號！")
            elif s_est <= 0: st.error("❌ 預估工時不可為 0！")
            elif s_type in ["NG重修", "NG重製"] and not g_ng_type: st.error("❌ 請填寫 NG 類型。 / Vui lòng nhập loại NG.")
            elif s_type in ["NG重修", "NG重製"] and not g_ng_note: st.error("❌ 請填寫 NG 說明。 / Vui lòng nhập giải thích NG.")
            elif s_type == "重製" and not g_remake_note: st.error("❌ 請填寫重製原因。 / Vui lòng nhập lý do làm lại.")
            else:
                now = datetime.now(TAIWAN_TZ)
                now_str = now.strftime("%Y-%m-%d %H:%M:%S")
                wo_id = f"WO-{now.strftime('%Y%m%d%H%M%S%f')}"
                
                # 若為 NG / 重製，寫入獨立 ng_records
                if s_type in ["NG重修", "NG重製", "重製"]:
                    ng_id = f"NG-{now.strftime('%Y%m%d%H%M%S%f')}"
                    meth = "重修" if s_type == "NG重修" else ("NG重製" if s_type == "NG重製" else "設變重製")
                    ntype = g_ng_type if s_type != "重製" else ""
                    nnote = g_ng_note if s_type != "重製" else g_remake_note
                    
                    append_ng_record({
                        "NG_ID": ng_id, "建立時間": now_str, "發生日期": now.strftime("%Y-%m-%d"),
                        "發現人": g_disc, "責任人": g_resp, "機台類型": "磨床", "工單ID": wo_id,
                        "工單號碼": g_wo_no, "圖號": s_drawing, "工件數量": s_qty, "生產類型": s_type,
                        "NG類型": ntype, "NG說明": nnote, "處理方式": meth, "狀態": "待處理",
                        "備註": "", "更新時間": now_str
                    })
                    
                    # 發送 LINE 通知
                    lmsg = (f"⚠️ NG / 重製工單通知\n類型：{s_type}\n機台：磨床\n人員：{s_name}\n"
                            f"發現人：{g_disc}\n責任人：{g_resp}\n工單號碼：{g_wo_no}\n圖號：{s_drawing}\n數量：{s_qty}\n")
                    if s_type == "重製":
                        lmsg += f"NG類型：非 NG\nNG說明：{g_remake_note}\n處理方式：{meth}\n"
                    else:
                        lmsg += f"NG類型：{ntype}\nNG說明：{nnote}\n處理方式：{meth}\n"
                    lmsg += f"時間：台灣時間 {now_str}"
                    send_line_message(lmsg)

                # 寫入 work_orders (備註維持空白)
                append_work_order({
                    '工單ID': wo_id, '日期': now.strftime("%Y-%m-%d"), '填寫人': s_name,
                    '生產類型': s_type, '機台類型': '磨床', '工單號碼': g_wo_no, '圖號': s_drawing, '工件數量': s_qty, '預估工時': s_est,
                    '實際工時': 0.0, '開始時間': now_str, '結束時間': "", '工作區間工時': 0.0,
                    '累積工作區間工時': 0.0, '最後恢復時間': now_str, '暫停時間': "", '暫停原因': "",
                    '時間差異': 0.0, '狀態': '進行中', '備註': ""
                })
                st.success("✅ 工單已啟動！"); st.rerun()

    st.subheader("⏳ 進行中的工單查詢")
    ongoing = db_df[(db_df['狀態'] == '進行中') & (db_df['機台類型'] == '磨床')]
    for _, row in ongoing.iterrows():
        wo_no_disp = row.get('工單號碼', '未填寫') if row.get('工單號碼') else '未填寫'
        with st.expander(f"🛠️ {row['填寫人']} | 磨床 | 工單號碼：{wo_no_disp} | 圖號：{row['圖號']} ({row['生產類型']})"):
            res_dt = parse_taiwan_time(row.get('最後恢復時間'))
            if pd.isna(res_dt): res_dt = parse_taiwan_time(row['開始時間'])
            cur_h = round(float(row.get('累積工作區間工時', 0)) + calculate_work_hours_excluding_lunch(res_dt, datetime.now(TAIWAN_TZ)), 2)
            
            st.write(f"**圖號:** {row['圖號']} | **工件數量:** {row.get('工件數量', 1)}")
            st.info(f"⏱️ 系統累積工作區間工時: {cur_h} 小時")
            
            # --- 磨床修正區塊開始 (V5.2.2) ---
            with st.expander("✏️ 填錯資料修正 / Chỉnh sửa thông tin nhập sai"):
                st.info("僅限修正基本資料。如需更改狀態或時間，請聯繫主管。")
                
                with st.form(key=f"form_edit_g_{row['工單ID']}"):
                    safe_emp = str(row['填寫人']).strip()
                    emp_idx = emps.index(safe_emp) if safe_emp in emps else 0
                    new_user = st.selectbox("填寫人 / Người điền", emps, index=emp_idx, key=f"edit_g_user_{row['工單ID']}")
                    
                    safe_type = str(row['生產類型']).strip()
                    type_idx = PROD_TYPES.index(safe_type) if safe_type in PROD_TYPES else 0
                    new_type = st.selectbox(
                        "生產類型 / Loại sản xuất", 
                        PROD_TYPES, 
                        index=type_idx,
                        format_func=lambda x: PROD_TYPES_BILINGUAL.get(x, x),
                        key=f"edit_g_type_{row['工單ID']}"
                    )
                    
                    new_order_no = st.text_input("工單號碼 / Mã đơn hàng", value=str(row['工單號碼']).strip(), key=f"edit_g_order_{row['工單ID']}")
                    new_part_no = st.text_input("圖號 / Mã bản vẽ", value=str(row['圖號']).strip(), key=f"edit_g_part_{row['工單ID']}")
                    
                    safe_qty = pd.to_numeric(row.get('工件數量', 1), errors='coerce')
                    safe_qty = int(safe_qty) if not pd.isna(safe_qty) and safe_qty >= 1 else 1
                    new_qty = st.number_input("工件數量 / Số lượng", value=safe_qty, min_value=1, step=1, key=f"edit_g_qty_{row['工單ID']}")

                    safe_est = pd.to_numeric(row.get('預估工時', 0), errors='coerce')
                    safe_est = float(safe_est) if not pd.isna(safe_est) and safe_est > 0 else 0.1
                    new_est = st.number_input("預估工時 / TG dự kiến (hrs)", value=safe_est, min_value=0.1, step=0.1, key=f"edit_g_est_{row['工單ID']}")

                    st.caption("⚠️ 若生產類型改為「NG重修」、「NG重製」或「重製」，以下為必填欄位：")
                    ng_disc = st.selectbox("發現人 / Người phát hiện (NG/重製必填)", emps, key=f"edit_g_ngdisc_{row['工單ID']}")
                    ng_resp = st.selectbox("責任人 / Người phụ trách (NG/重製必填)", emps, key=f"edit_g_ngresp_{row['工單ID']}")
                    ng_type_input = st.text_input("NG 類型 / Loại NG (重製免填)", key=f"edit_g_ngtype_{row['工單ID']}")
                    ng_note = st.text_area("NG 說明 / 重製原因 / Giải thích", key=f"edit_g_ngnote_{row['工單ID']}")

                    submit_btn = st.form_submit_button("確認修改資料 / Xác nhận sửa")

                if submit_btn:
                    has_error = False
                    if not new_part_no.strip():
                        st.error("❌ 圖號不可空白 / Mã bản vẽ không được để trống")
                        has_error = True
                        
                    if new_type in ["NG重修", "NG重製"] and (not ng_type_input.strip() or not ng_note.strip()):
                        st.error("❌ 選擇 NG重修 / NG重製 時，NG類型與NG說明不可空白！")
                        has_error = True
                        
                    if new_type == "重製" and not ng_note.strip():
                        st.error("❌ 選擇 重製 時，重製原因不可空白！")
                        has_error = True

                    if not has_error:
                        curr = load_work_orders_raw()
                        mask = (curr['工單ID'].astype(str) == str(row['工單ID'])) & (curr['狀態'] == '進行中') & (curr['機台類型'] == '磨床')
                        
                        if not curr[mask].empty:
                            now_dt = datetime.now(TAIWAN_TZ)
                            now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 1. 先準備 work_orders 資料 (先不存)
                            curr.loc[mask, ['填寫人', '生產類型', '工單號碼', '圖號', '工件數量', '預估工時']] = [
                                new_user, new_type, new_order_no.strip(), new_part_no.strip(), new_qty, new_est
                            ]

                            # 2. 先準備 ng_records 資料 (先不存)
                            raw_ng = None
                            ng_modified = False
                            send_line = False
                            meth = ""
                            final_ng_type = ""
                            final_note = ""
                            ng_prepare_ok = True  # 控制是否往下儲存的安全開關
                            
                            try:
                                raw_ng = load_ng_records_raw()
                                ng_mask = raw_ng['工單ID'].astype(str) == str(row['工單ID'])
                                
                                if new_type in ["NG重修", "NG重製", "重製"]:
                                    meth = "重修" if new_type == "NG重修" else "NG重製" if new_type == "NG重製" else "設變重製"
                                    final_ng_type = ng_type_input.strip() if new_type in ["NG重修", "NG重製"] else "非 NG"
                                    final_note = ng_note.strip()
                                    send_line = True
                                    
                                    if raw_ng[ng_mask].empty:
                                        new_ng_id = f"NG-{now_dt.strftime('%Y%m%d%H%M%S%f')}"
                                        new_row = {
                                            'NG_ID': new_ng_id, '建立時間': now_str, 
                                            '發生日期': curr.loc[mask, '日期'].values[0] if '日期' in curr.columns and str(curr.loc[mask, '日期'].values[0]).strip() else now_dt.strftime('%Y-%m-%d'),
                                            '發現人': ng_disc, '責任人': ng_resp, '機台類型': '磨床',
                                            '工單ID': row['工單ID'], '工單號碼': new_order_no.strip(),
                                            '圖號': new_part_no.strip(), '工件數量': new_qty,
                                            '生產類型': new_type, 'NG類型': final_ng_type, 'NG說明': final_note,
                                            '處理方式': meth, '狀態': '待處理', '備註': '', '更新時間': now_str
                                        }
                                        raw_ng = pd.concat([raw_ng, pd.DataFrame([new_row])], ignore_index=True)
                                        ng_modified = True
                                    else:
                                        idx = raw_ng[ng_mask].index[0]
                                        raw_ng.at[idx, '發現人'] = ng_disc
                                        raw_ng.at[idx, '責任人'] = ng_resp
                                        raw_ng.at[idx, '機台類型'] = '磨床'
                                        raw_ng.at[idx, '工單號碼'] = new_order_no.strip()
                                        raw_ng.at[idx, '圖號'] = new_part_no.strip()
                                        raw_ng.at[idx, '工件數量'] = new_qty
                                        raw_ng.at[idx, '生產類型'] = new_type
                                        raw_ng.at[idx, 'NG類型'] = final_ng_type
                                        raw_ng.at[idx, 'NG說明'] = final_note
                                        raw_ng.at[idx, '處理方式'] = meth
                                        if pd.isna(raw_ng.at[idx, '狀態']) or str(raw_ng.at[idx, '狀態']).strip() == "":
                                            raw_ng.at[idx, '狀態'] = '待處理'
                                        raw_ng.at[idx, '更新時間'] = now_str
                                        ng_modified = True

                                elif new_type in ["正常生產", "插件"] and not raw_ng[ng_mask].empty:
                                    idx = raw_ng[ng_mask].index[0]
                                    raw_ng.at[idx, '狀態'] = '已取消'
                                    old_note = str(raw_ng.at[idx, '備註']) if pd.notna(raw_ng.at[idx, '備註']) else ""
                                    append_note = f"工單基本資料修正：生產類型已改為 {new_type}，此 NG/重製紀錄取消。"
                                    raw_ng.at[idx, '備註'] = f"{old_note} | {append_note}" if old_note.strip() else append_note
                                    raw_ng.at[idx, '更新時間'] = now_str
                                    ng_modified = True

                            except Exception as e:
                                st.error(f"❌ 準備 NG 紀錄資料時發生錯誤，已終止更新以保護資料安全: {e}")
                                ng_prepare_ok = False

                            # 3. 如果檢查與準備都通過，才執行儲存
                            if ng_prepare_ok:
                                save_work_orders(curr)
                                if ng_modified and raw_ng is not None:
                                    save_ng_records(raw_ng)
                                
                                # 4. 發送 LINE 通知
                                if send_line:
                                    line_msg = f"\n⚠️ NG / 重製工單資料修正通知\n類型：{new_type}\n機台：磨床\n人員：{new_user}\n發現人：{ng_disc}\n責任人：{ng_resp}\n工單號碼：{new_order_no.strip()}\n圖號：{new_part_no.strip()}\n數量：{new_qty}\nNG類型：{final_ng_type}\nNG說明：{final_note}\n處理方式：{meth}\n時間：台灣時間 {now_str}"
                                    try:
                                        if 'send_line_message' in globals():
                                            send_line_message(line_msg)
                                    except Exception:
                                        pass

                                st.success("✅ 資料已安全更新！請重新整理頁面。")
                                st.rerun()
                        else:
                            st.error("❌ 此工單狀態已變更，無法修改。")
            # --- 磨床修正區塊結束 ---
            
            p_reason = st.selectbox("暫停原因", PAUSE_REASONS, key=f"pr_{row['工單ID']}")
            if st.button("⏸️ 暫停加工", key=f"pb_{row['工單ID']}"):
                curr = load_work_orders_raw()
                mask = (curr['工單ID'] == row['工單ID']) & (curr['狀態'] == '進行中')
                if not curr[mask].empty:
                    curr.loc[mask, ['累積工作區間工時', '狀態', '暫停時間', '暫停原因']] = \
                        [cur_h, '暫停中', datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S"), p_reason]
                    save_work_orders(curr)
                    st.success("⏸️ 工單已暫停！"); st.rerun()
                    
            st.divider()
            
            # 結案區塊：目前工時顯示在按鈕右邊
            finish_col, hour_col = st.columns([1, 2])

            with finish_col:
                finish_clicked = st.button(
                    "✅ 加工完成並結案",
                    key=f"btn_{row['工單ID']}",
                    type="primary",
                    use_container_width=True
                )

            with hour_col:
                st.info(f"⏱️ 目前工時 / Thời gian hiện tại：{cur_h} 小時")

            if finish_clicked:
                curr = load_work_orders_raw()
                mask = (curr['工單ID'] == row['工單ID']) & (curr['狀態'] == '進行中')
                if not curr[mask].empty:
                    curr.loc[mask, ['結束時間', '實際工時', '工作區間工時', '累積工作區間工時', '時間差異', '狀態', '備註']] = \
                        [datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S"), cur_h, cur_h, cur_h, 0.0, '已完成', str(row.get('備註', ''))]
                    save_work_orders(curr)

                    if row['生產類型'] != "正常生產":
                        send_line_message(
                            f"\n⚠️異常結案通知\n機台：磨床\n類型：{row['生產類型']}\n人員：{row['填寫人']}\n"
                            f"工單號碼：{wo_no_disp}\n圖號：{row['圖號']}\n數量：{row.get('工件數量', 1)}\n"
                            f"實際加工：{cur_h}h\n備註：\n{str(row.get('備註', ''))}"
                        )
                    st.success(f"✅ 已結案！工時：{cur_h}h")
                    st.rerun()
    st.subheader("⏸️ 暫停中的工單查詢")
    pause_df = db_df[(db_df['狀態'] == '暫停中') & (db_df['機台類型'] == '磨床')]
    for _, row in pause_df.iterrows():
        with st.container(border=True):
            col_p1, col_p2 = st.columns([3, 1])
            with col_p1:
                st.write(f"**圖號:** {row['圖號']} ({row['生產類型']}) | **人員:** {row['填寫人']}")
                st.error(f"⏸️ 暫停原因: {row.get('暫停原因', '未填寫')} (於 {row.get('暫停時間', '')})")
                st.info(f"⏱️ 累積工作區間工時: {row.get('累積工作區間工時', 0.0)} 小時")
            with col_p2:
                if st.button("▶️ 繼續加工", key=f"r_btn_{row['工單ID']}", type="primary", use_container_width=True):
                    curr = load_work_orders_raw()
                    mask = (curr['工單ID'] == row['工單ID']) & (curr['狀態'] == '暫停中')
                    if not curr[mask].empty:
                        curr.loc[mask, ['狀態', '最後恢復時間']] = ['進行中', datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S")]
                        save_work_orders(curr)
                        st.success("▶️ 已恢復加工！"); st.rerun()

# --- 頁籤 2：放電機/快走絲報工 ---
with tab2:
    st.header("放電機/快走絲報工 / Báo cáo máy EDM / máy cắt dây nhanh")
    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            ew_name = st.selectbox("填寫人 / Người điền", emps, key="ew_name")
            ew_mac = st.selectbox("機台類型", ["放電機", "快走絲"], format_func=lambda x: MACHINE_TYPES_BILINGUAL.get(x, x), key="ew_mac")
        with c2:
            ew_type = st.selectbox("生產類型", PROD_TYPES, format_func=lambda x: PROD_TYPES_BILINGUAL.get(x, x), key="ew_type")
            ew_wo_no = st.text_input("工單號碼", key="ew_wo_no").strip()
        with c3:
            ew_drawing = st.text_input("圖號", key="ew_drawing").strip()
            ew_qty = st.number_input("數量", min_value=1, value=1, key="ew_qty")
            ew_est = st.number_input("預估機台工時 (h)", min_value=0.0, step=0.1, key="ew_est")

        # --- 動態 NG 欄位 ---
        e_disc = e_resp = e_ng_type = e_ng_note = e_remake_note = ""
        try: edef_idx = emps.index(ew_name)
        except: edef_idx = 0

        if ew_type in ["NG重修", "NG重製"]:
            st.divider()
            nc1, nc2, nc3, nc4 = st.columns(4)
            with nc1: e_disc = st.selectbox("發現人", emps, index=edef_idx, key="e_disc")
            with nc2: e_resp = st.selectbox("責任人", emps, index=edef_idx, key="e_resp")
            with nc3: e_ng_type = st.text_input("NG 類型", placeholder="例如：尺寸偏大", key="e_ngt").strip()
            with nc4: e_ng_note = st.text_area("NG 說明", key="e_ngn").strip()
        elif ew_type == "重製":
            st.divider()
            rc1, rc2, rc3 = st.columns([1, 1, 2])
            with rc1: e_disc = st.selectbox("發現人", emps, index=edef_idx, key="e_r_disc")
            with rc2: e_resp = st.selectbox("責任人", emps, index=edef_idx, key="e_r_resp")
            with rc3: e_remake_note = st.text_area("重製原因", key="e_rmn").strip()
        # -------------------

        if st.button("▶️ 開始加工", type="primary", key="ew_start"):
            if not ew_drawing: st.error("❌ 請輸入圖號！")
            elif ew_est <= 0: st.error("❌ 預估工時不可為 0！")
            elif ew_type in ["NG重修", "NG重製"] and not e_ng_type: st.error("❌ 請填寫 NG 類型。")
            elif ew_type in ["NG重修", "NG重製"] and not e_ng_note: st.error("❌ 請填寫 NG 說明。")
            elif ew_type == "重製" and not e_remake_note: st.error("❌ 請填寫重製原因。")
            else:
                now = datetime.now(TAIWAN_TZ)
                now_str = now.strftime("%Y-%m-%d %H:%M:%S")
                wo_id = f"WO-{now.strftime('%Y%m%d%H%M%S%f')}"
                
                # 寫入獨立 ng_records
                if ew_type in ["NG重修", "NG重製", "重製"]:
                    ng_id = f"NG-{now.strftime('%Y%m%d%H%M%S%f')}"
                    meth = "重修" if ew_type == "NG重修" else ("NG重製" if ew_type == "NG重製" else "設變重製")
                    ntype = e_ng_type if ew_type != "重製" else ""
                    nnote = e_ng_note if ew_type != "重製" else e_remake_note
                    
                    append_ng_record({
                        "NG_ID": ng_id, "建立時間": now_str, "發生日期": now.strftime("%Y-%m-%d"),
                        "發現人": e_disc, "責任人": e_resp, "機台類型": ew_mac, "工單ID": wo_id,
                        "工單號碼": ew_wo_no, "圖號": ew_drawing, "工件數量": ew_qty, "生產類型": ew_type,
                        "NG類型": ntype, "NG說明": nnote, "處理方式": meth, "狀態": "待處理",
                        "備註": "", "更新時間": now_str
                    })
                    
                    lmsg = (f"⚠️ NG / 重製工單通知\n類型：{ew_type}\n機台：{ew_mac}\n人員：{ew_name}\n"
                            f"發現人：{e_disc}\n責任人：{e_resp}\n工單號碼：{ew_wo_no}\n圖號：{ew_drawing}\n數量：{ew_qty}\n")
                    if ew_type == "重製": lmsg += f"NG類型：非 NG\nNG說明：{e_remake_note}\n處理方式：{meth}\n"
                    else: lmsg += f"NG類型：{ntype}\nNG說明：{nnote}\n處理方式：{meth}\n"
                    lmsg += f"時間：台灣時間 {now_str}"
                    send_line_message(lmsg)

                # 寫入 work_orders
                append_work_order({
                    '工單ID': wo_id, '日期': now.strftime("%Y-%m-%d"), '填寫人': ew_name,
                    '生產類型': ew_type, '機台類型': ew_mac, '工單號碼': ew_wo_no, '圖號': ew_drawing, '工件數量': ew_qty, '預估工時': ew_est,
                    '實際工時': 0.0, '開始時間': now_str, '結束時間': "", '工作區間工時': 0.0,
                    '累積工作區間工時': 0.0, '最後恢復時間': now_str, '暫停時間': "", '暫停原因': "",
                    '時間差異': 0.0, '狀態': '進行中', '備註': ""
                })
                st.success("✅ 工單已啟動！"); st.rerun()

    st.subheader("⏳ 進行中的工單查詢")
    ongoing_ew = db_df[(db_df['狀態'] == '進行中') & (db_df['機台類型'].isin(["放電機", "快走絲"]))]
    for _, row in ongoing_ew.iterrows():
        wo_no_disp = row.get('工單號碼', '未填寫') if row.get('工單號碼') else '未填寫'
        with st.expander(f"🛠️ {row['填寫人']} | {row['機台類型']} | 工單號碼：{wo_no_disp} | 圖號：{row['圖號']} ({row['生產類型']})"):
            res_dt = parse_taiwan_time(row.get('最後恢復時間'))
            if pd.isna(res_dt): res_dt = parse_taiwan_time(row['開始時間'])
            
            cur_h = round(float(row.get('累積工作區間工時', 0)) + calculate_work_hours_excluding_lunch(res_dt, datetime.now(TAIWAN_TZ)), 2)
            st.write(f"**圖號:** {row['圖號']} | **工件數量:** {row.get('工件數量', 1)}")
            st.info(f"⏱️ 系統目前累積工作區間 (機台運轉) 工時: {cur_h} 小時")
            
            # --- 放電/快走絲修正區塊開始 (V5.2.2) ---
            with st.expander("✏️ 填錯資料修正 / Chỉnh sửa thông tin nhập sai"):
                st.info("僅限修正基本資料。如需更改狀態或時間，請聯繫主管。")
                
                with st.form(key=f"form_edit_ew_{row['工單ID']}"):
                    safe_emp = str(row['填寫人']).strip()
                    emp_idx = emps.index(safe_emp) if safe_emp in emps else 0
                    new_user = st.selectbox("填寫人 / Người điền", emps, index=emp_idx, key=f"edit_ew_user_{row['工單ID']}")
                    
                    safe_mach = str(row['機台類型']).strip()
                    mach_options = ["放電機", "快走絲"]
                    mach_idx = mach_options.index(safe_mach) if safe_mach in mach_options else 0
                    new_machine = st.selectbox("機台類型 / Loại máy", mach_options, index=mach_idx, key=f"edit_ew_mach_{row['工單ID']}")

                    safe_type = str(row['生產類型']).strip()
                    type_idx = PROD_TYPES.index(safe_type) if safe_type in PROD_TYPES else 0
                    new_type = st.selectbox(
                        "生產類型 / Loại sản xuất", 
                        PROD_TYPES, 
                        index=type_idx,
                        format_func=lambda x: PROD_TYPES_BILINGUAL.get(x, x),
                        key=f"edit_ew_type_{row['工單ID']}"
                    )
                    
                    new_order_no = st.text_input("工單號碼 / Mã đơn hàng", value=str(row['工單號碼']).strip(), key=f"edit_ew_order_{row['工單ID']}")
                    new_part_no = st.text_input("圖號 / Mã bản vẽ", value=str(row['圖號']).strip(), key=f"edit_ew_part_{row['工單ID']}")
                    
                    safe_qty = pd.to_numeric(row.get('工件數量', 1), errors='coerce')
                    safe_qty = int(safe_qty) if not pd.isna(safe_qty) and safe_qty >= 1 else 1
                    new_qty = st.number_input("工件數量 / Số lượng", value=safe_qty, min_value=1, step=1, key=f"edit_ew_qty_{row['工單ID']}")

                    safe_est = pd.to_numeric(row.get('預估工時', 0), errors='coerce')
                    safe_est = float(safe_est) if not pd.isna(safe_est) and safe_est > 0 else 0.1
                    new_est = st.number_input("預估機台工時 / TG máy dự kiến (hrs)", value=safe_est, min_value=0.1, step=0.1, key=f"edit_ew_est_{row['工單ID']}")

                    st.caption("⚠️ 若生產類型改為「NG重修」、「NG重製」或「重製」，以下為必填欄位：")
                    ng_disc = st.selectbox("發現人 / Người phát hiện (NG/重製必填)", emps, key=f"edit_ew_ngdisc_{row['工單ID']}")
                    ng_resp = st.selectbox("責任人 / Người phụ trách (NG/重製必填)", emps, key=f"edit_ew_ngresp_{row['工單ID']}")
                    ng_type_input = st.text_input("NG 類型 / Loại NG (重製免填)", key=f"edit_ew_ngtype_{row['工單ID']}")
                    ng_note = st.text_area("NG 說明 / 重製原因 / Giải thích", key=f"edit_ew_ngnote_{row['工單ID']}")

                    submit_btn = st.form_submit_button("確認修改資料 / Xác nhận sửa")

                if submit_btn:
                    has_error = False
                    if not new_part_no.strip():
                        st.error("❌ 圖號不可空白 / Mã bản vẽ không được để trống")
                        has_error = True
                        
                    if new_type in ["NG重修", "NG重製"] and (not ng_type_input.strip() or not ng_note.strip()):
                        st.error("❌ 選擇 NG重修 / NG重製 時，NG類型與NG說明不可空白！")
                        has_error = True
                        
                    if new_type == "重製" and not ng_note.strip():
                        st.error("❌ 選擇 重製 時，重製原因不可空白！")
                        has_error = True

                    if not has_error:
                        curr = load_work_orders_raw()
                        mask = (curr['工單ID'].astype(str) == str(row['工單ID'])) & (curr['狀態'] == '進行中') & (curr['機台類型'].isin(["放電機", "快走絲"]))
                        
                        if not curr[mask].empty:
                            now_dt = datetime.now(TAIWAN_TZ)
                            now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 1. 先準備 work_orders 資料 (先不存)
                            curr.loc[mask, ['填寫人', '機台類型', '生產類型', '工單號碼', '圖號', '工件數量', '預估工時']] = [
                                new_user, new_machine, new_type, new_order_no.strip(), new_part_no.strip(), new_qty, new_est
                            ]

                            # 2. 先準備 ng_records 資料 (先不存)
                            raw_ng = None
                            ng_modified = False
                            send_line = False
                            meth = ""
                            final_ng_type = ""
                            final_note = ""
                            ng_prepare_ok = True # 安全開關
                            
                            try:
                                raw_ng = load_ng_records_raw()
                                ng_mask = raw_ng['工單ID'].astype(str) == str(row['工單ID'])
                                
                                if new_type in ["NG重修", "NG重製", "重製"]:
                                    meth = "重修" if new_type == "NG重修" else "NG重製" if new_type == "NG重製" else "設變重製"
                                    final_ng_type = ng_type_input.strip() if new_type in ["NG重修", "NG重製"] else "非 NG"
                                    final_note = ng_note.strip()
                                    send_line = True
                                    
                                    if raw_ng[ng_mask].empty:
                                        new_ng_id = f"NG-{now_dt.strftime('%Y%m%d%H%M%S%f')}"
                                        new_row = {
                                            'NG_ID': new_ng_id, '建立時間': now_str, 
                                            '發生日期': curr.loc[mask, '日期'].values[0] if '日期' in curr.columns and str(curr.loc[mask, '日期'].values[0]).strip() else now_dt.strftime('%Y-%m-%d'),
                                            '發現人': ng_disc, '責任人': ng_resp, '機台類型': new_machine,
                                            '工單ID': row['工單ID'], '工單號碼': new_order_no.strip(),
                                            '圖號': new_part_no.strip(), '工件數量': new_qty,
                                            '生產類型': new_type, 'NG類型': final_ng_type, 'NG說明': final_note,
                                            '處理方式': meth, '狀態': '待處理', '備註': '', '更新時間': now_str
                                        }
                                        raw_ng = pd.concat([raw_ng, pd.DataFrame([new_row])], ignore_index=True)
                                        ng_modified = True
                                    else:
                                        idx = raw_ng[ng_mask].index[0]
                                        raw_ng.at[idx, '發現人'] = ng_disc
                                        raw_ng.at[idx, '責任人'] = ng_resp
                                        raw_ng.at[idx, '機台類型'] = new_machine
                                        raw_ng.at[idx, '工單號碼'] = new_order_no.strip()
                                        raw_ng.at[idx, '圖號'] = new_part_no.strip()
                                        raw_ng.at[idx, '工件數量'] = new_qty
                                        raw_ng.at[idx, '生產類型'] = new_type
                                        raw_ng.at[idx, 'NG類型'] = final_ng_type
                                        raw_ng.at[idx, 'NG說明'] = final_note
                                        raw_ng.at[idx, '處理方式'] = meth
                                        if pd.isna(raw_ng.at[idx, '狀態']) or str(raw_ng.at[idx, '狀態']).strip() == "":
                                            raw_ng.at[idx, '狀態'] = '待處理'
                                        raw_ng.at[idx, '更新時間'] = now_str
                                        ng_modified = True

                                elif new_type in ["正常生產", "插件"] and not raw_ng[ng_mask].empty:
                                    idx = raw_ng[ng_mask].index[0]
                                    raw_ng.at[idx, '狀態'] = '已取消'
                                    old_note = str(raw_ng.at[idx, '備註']) if pd.notna(raw_ng.at[idx, '備註']) else ""
                                    append_note = f"工單基本資料修正：生產類型已改為 {new_type}，此 NG/重製紀錄取消。"
                                    raw_ng.at[idx, '備註'] = f"{old_note} | {append_note}" if old_note.strip() else append_note
                                    raw_ng.at[idx, '更新時間'] = now_str
                                    ng_modified = True

                            except Exception as e:
                                st.error(f"❌ 準備 NG 紀錄資料時發生錯誤，已終止更新以保護資料安全: {e}")
                                ng_prepare_ok = False 

                            # 3. 如果檢查與準備都通過，才執行儲存
                            if ng_prepare_ok:
                                save_work_orders(curr)
                                if ng_modified and raw_ng is not None:
                                    save_ng_records(raw_ng)

                                # 4. 發送 LINE 通知
                                if send_line:
                                    line_msg = f"\n⚠️ NG / 重製工單資料修正通知\n類型：{new_type}\n機台：{new_machine}\n人員：{new_user}\n發現人：{ng_disc}\n責任人：{ng_resp}\n工單號碼：{new_order_no.strip()}\n圖號：{new_part_no.strip()}\n數量：{new_qty}\nNG類型：{final_ng_type}\nNG說明：{final_note}\n處理方式：{meth}\n時間：台灣時間 {now_str}"
                                    try:
                                        if 'send_line_message' in globals():
                                            send_line_message(line_msg)
                                    except Exception:
                                        pass

                                st.success("✅ 資料已安全更新！請重新整理頁面。")
                                st.rerun()
                        else:
                            st.error("❌ 此工單狀態已變更，無法修改。")
            # --- 放電/快走絲修正區塊結束 ---
            
            p_reason_ew = st.selectbox("暫停原因", PAUSE_REASONS, key=f"pr_ew_{row['工單ID']}")
            if st.button("⏸️ 暫停加工", key=f"pb_ew_{row['工單ID']}"):
                curr = load_work_orders_raw()
                mask = (curr['工單ID'] == row['工單ID']) & (curr['狀態'] == '進行中')
                if not curr[mask].empty:
                    curr.loc[mask, ['累積工作區間工時', '狀態', '暫停時間', '暫停原因']] = \
                        [cur_h, '暫停中', datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S"), p_reason_ew]
                    save_work_orders(curr)
                    st.success(f"⏸️ 工單已暫停！"); st.rerun()

            st.divider()
            
            # 結案區塊：無備註
            c_e1, c_e2 = st.columns(2)
            with c_e1: e_d = st.date_input("機台實際停止日期", value=datetime.now(TAIWAN_TZ).date(), key=f"end_d_{row['工單ID']}")
            with c_e2: e_t = st.time_input("機台實際停止時間", value=datetime.now(TAIWAN_TZ).time(), key=f"end_t_{row['工單ID']}")
            l_run = st.checkbox("午休是否持續加工", value=True, key=f"l_run_{row['工單ID']}")

            finish_col, hour_col = st.columns([1, 2])

            with finish_col:
                finish_clicked = st.button(
                    "✅ 加工完成並結案",
                    key=f"btn_ew_{row['工單ID']}",
                    type="primary",
                    use_container_width=True
                )

            with hour_col:
                st.info(f"⏱️ 目前機台工時 / Thời gian máy hiện tại：{cur_h} 小時")

            if finish_clicked:
                end_dt = datetime.combine(e_d, e_t).replace(tzinfo=TAIWAN_TZ)
                start_dt = parse_taiwan_time(row['開始時間'])
                if end_dt < start_dt:
                    st.error("❌ 機台停止時間不可早於開始時間。")
                else:
                    curr = load_work_orders_raw()
                    mask = (curr['工單ID'] == row['工單ID']) & (curr['狀態'] == '進行中')
                    if not curr[mask].empty:
                        seg = calculate_elapsed_hours(res_dt, end_dt) if l_run else calculate_work_hours_excluding_lunch(res_dt, end_dt)
                        fin_h = round(float(row.get('累積工作區間工時', 0)) + seg, 2)

                        curr.loc[mask, ['結束時間', '實際工時', '工作區間工時', '累積工作區間工時', '時間差異', '狀態', '備註']] = \
                            [end_dt.strftime("%Y-%m-%d %H:%M:%S"), fin_h, fin_h, fin_h, 0.0, '已完成', str(row.get('備註', ''))]
                        save_work_orders(curr)

                        if row['生產類型'] != "正常生產":
                            send_line_message(
                                f"\n⚠️異常結案通知\n機台：{row['機台類型']}\n類型：{row['生產類型']}\n人員：{row['填寫人']}\n"
                                f"工單號碼：{wo_no_disp}\n圖號：{row['圖號']}\n數量：{row.get('工件數量', 1)}\n"
                                f"機台實際停止時間：{end_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                f"機台工時：{fin_h}h\n備註：\n{str(row.get('備註', ''))}"
                            )
                        st.success(f"✅ 已結案！工時：{fin_h}h"); st.rerun()

    st.subheader("⏸️ 暫停中的工單查詢")
    pause_ew = db_df[(db_df['狀態'] == '暫停中') & (db_df['機台類型'].isin(["放電機", "快走絲"]))]
    for _, row in pause_ew.iterrows():
        with st.container(border=True):
            col_p1, col_p2 = st.columns([3, 1])
            with col_p1:
                st.write(f"**圖號:** {row['圖號']} ({row['機台類型']} - {row['生產類型']}) | **人員:** {row['填寫人']}")
                st.error(f"⏸️ 暫停原因: {row.get('暫停原因', '未填寫')} (於 {row.get('暫停時間', '')})")
            with col_p2:
                if st.button("▶️ 繼續加工", key=f"r_btn_ew_{row['工單ID']}", type="primary", use_container_width=True):
                    curr = load_work_orders_raw()
                    mask = (curr['工單ID'] == row['工單ID']) & (curr['狀態'] == '暫停中')
                    if not curr[mask].empty:
                        curr.loc[mask, ['狀態', '最後恢復時間']] = ['進行中', datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S")]
                        save_work_orders(curr)
                        st.success("▶️ 已恢復加工！"); st.rerun()
                        
# --- 頁籤：預估工時 ---
with tab_est:
    render_estimate_tool("estimate_page")
    
# --- 頁籤 NG 管理 ---
with tab_ng:
    st.header("🚨 NG 管理")
    if not st.session_state["is_admin"]:
        st.warning("🔒 NG 管理需輸入主管密碼才能查看。")
    else:
        ng_df = load_ng_records_cached()
        if ng_df.empty:
            st.info("目前無任何 NG 紀錄。")
        else:
            ng_df['發生日期_dt'] = pd.to_datetime(ng_df['發生日期'], errors='coerce').dt.date
            min_d = ng_df['發生日期_dt'].min() if not ng_df['發生日期_dt'].isna().all() else datetime.now().date()
            max_d = ng_df['發生日期_dt'].max() if not ng_df['發生日期_dt'].isna().all() else datetime.now().date()

            with st.expander("🔍 篩選條件", expanded=True):
                nc1, nc2, nc3, nc4 = st.columns(4)
                with nc1: f_ng_dates = st.date_input("日期區間", [min_d, max_d], key="ng_date_range")
                with nc2: f_ng_mac = st.selectbox("機台類型", ["全部"] + MACHINE_TYPES, key="ng_machine_filter")
                with nc3: f_ng_ptype = st.selectbox("生產類型", ["全部"] + ["NG重修", "NG重製", "重製"], key="ng_ptype_filter")
                with nc4: f_ng_res = st.selectbox("責任人", ["全部"] + list(ng_df['責任人'].dropna().unique()), key="ng_res_filter")
                
                nc5, nc6, nc7, nc8 = st.columns(4)
                with nc5: f_ng_ntype = st.selectbox("NG 類型", ["全部"] + list(ng_df['NG類型'].dropna().unique()), key="ng_ntype_filter")
                with nc6: f_ng_meth = st.selectbox("處理方式", ["全部"] + list(ng_df['處理方式'].dropna().unique()), key="ng_meth_filter")
                # --- V5.2.2 新增已取消篩選 ---
                with nc7: f_ng_stat = st.selectbox("狀態", ["全部", "待處理", "處理中", "已完成", "已取消"], key="ng_stat_filter")
                with nc8: f_ng_kw = st.text_input("工單號碼/圖號 關鍵字", key="ng_kw_filter")

            filtered_ng = ng_df.copy()
            if isinstance(f_ng_dates, (list, tuple)) and len(f_ng_dates) == 2:
                filtered_ng = filtered_ng[(filtered_ng['發生日期_dt'] >= f_ng_dates[0]) & (filtered_ng['發生日期_dt'] <= f_ng_dates[1])]
            if f_ng_mac != "全部": filtered_ng = filtered_ng[filtered_ng['機台類型'] == f_ng_mac]
            if f_ng_ptype != "全部": filtered_ng = filtered_ng[filtered_ng['生產類型'] == f_ng_ptype]
            if f_ng_res != "全部": filtered_ng = filtered_ng[filtered_ng['責任人'] == f_ng_res]
            if f_ng_ntype != "全部": filtered_ng = filtered_ng[filtered_ng['NG類型'] == f_ng_ntype]
            if f_ng_meth != "全部": filtered_ng = filtered_ng[filtered_ng['處理方式'] == f_ng_meth]
            if f_ng_stat != "全部": filtered_ng = filtered_ng[filtered_ng['狀態'] == f_ng_stat]
            if f_ng_kw:
                filtered_ng = filtered_ng[
                    filtered_ng['工單號碼'].astype(str).str.contains(f_ng_kw, na=False) |
                    filtered_ng['圖號'].astype(str).str.contains(f_ng_kw, na=False)
                ]

            st.markdown("### 📌 NG 統計")
            sc1, sc2, sc3, sc4, sc5, sc6 = st.columns(6)
            sc1.metric("總筆數", len(filtered_ng))
            sc2.metric("NG重修", len(filtered_ng[filtered_ng['生產類型'] == 'NG重修']))
            sc3.metric("NG重製", len(filtered_ng[filtered_ng['生產類型'] == 'NG重製']))
            sc4.metric("重製", len(filtered_ng[filtered_ng['生產類型'] == '重製']))
            sc5.metric("待處理", len(filtered_ng[filtered_ng['狀態'] == '待處理']))
            sc6.metric("已完成", len(filtered_ng[filtered_ng['狀態'] == '已完成']))

            st.dataframe(filtered_ng[[c for c in NG_COLS if c in filtered_ng.columns]], use_container_width=True)

            st.markdown("### ✏️ 修改 NG 紀錄")
            edit_ng_id = st.selectbox("選擇要修改的 NG_ID", [""] + list(filtered_ng['NG_ID']), key="ng_edit_id_sel")
            if edit_ng_id:
                ng_row = ng_df[ng_df['NG_ID'] == edit_ng_id].iloc[0]
                n_c1, n_c2, n_c3 = st.columns(3)
                with n_c1:
                    all_e = sorted(list(set(emps + ng_df['責任人'].dropna().tolist())))
                    e_idx = all_e.index(ng_row['責任人']) if ng_row['責任人'] in all_e else 0
                    u_resp = st.selectbox("責任人", all_e, index=e_idx, key="ng_edit_resp")
                    u_ntype = st.text_input("NG類型", value=str(ng_row.get('NG類型', '')), key="ng_edit_ntype")
                with n_c2:
                    u_nnote = st.text_area("NG說明/重製原因", value=str(ng_row.get('NG說明', '')), key="ng_edit_nnote")
                with n_c3:
                    u_meth = st.text_input("處理方式", value=str(ng_row.get('處理方式', '')), key="ng_edit_meth")
                    m_stat = ["待處理", "處理中", "已完成", "已取消"]
                    s_idx = m_stat.index(ng_row['狀態']) if ng_row['狀態'] in m_stat else 0
                    u_stat = st.selectbox("狀態", m_stat, index=s_idx, key="ng_edit_stat")
                    u_note = st.text_input("備註", value=str(ng_row.get('備註', '')), key="ng_edit_note")

                if st.button("💾 儲存 NG 紀錄", type="primary"):
                    raw_ng = load_ng_records_raw()
                    m = raw_ng['NG_ID'] == edit_ng_id
                    if not raw_ng[m].empty:
                        raw_ng.loc[m, ['責任人', 'NG類型', 'NG說明', '處理方式', '狀態', '備註', '更新時間']] = \
                            [u_resp, u_ntype, u_nnote, u_meth, u_stat, u_note, datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S")]
                        save_ng_records(raw_ng)
                        st.success("✅ NG 紀錄已更新！"); st.rerun()

# --- 頁籤 3：主管數據看板 ---
with tab3:
    if st.session_state["is_admin"]:
        if is_print_mode:
            st.markdown(f"<h1 style='text-align: center;'>工廠生產管理月報表 (V5.2.2) - {datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')}</h1>", unsafe_allow_html=True)
            full_df = load_work_orders_cached()
        else:
            st.title("📊 生產數據看板 (V5.2.2)")
            full_df = db_df
        
        if full_df.empty: st.info("無工單資料。")
        else:
            full_df['開始時間_dt'] = full_df['開始時間'].apply(parse_taiwan_time)
            full_df = full_df.dropna(subset=['開始時間_dt'])
            if not full_df.empty:
                full_df['日期_date'] = full_df['開始時間_dt'].dt.date
                full_df['年月'] = full_df['開始時間_dt'].dt.strftime('%Y-%m')
                full_df['月日'] = full_df['開始時間_dt'].dt.strftime('%m-%d')
                
                with st.container(border=not is_print_mode):
                    c1, c2, c3, c4, c5, c6 = st.columns([1.5, 2, 2, 2, 2, 2])
                    with c1: v_mode = st.radio("檢視模式", ["整體", "個人"], horizontal=True, key="dash_v_mode")
                    with c2: s_emp = st.selectbox("員工篩選", emps, disabled=(v_mode=="整體"), key="dash_s_emp")
                    with c3: d_range = st.date_input("日期區間", [full_df['日期_date'].min(), full_df['日期_date'].max()], key="dashboard_date_range")
                    with c4: s_status = st.selectbox("工單狀態", ["已完成", "進行中", "暫停中", "全部"], key="dash_status_filter")
                    with c5: s_type = st.selectbox("生產類型篩選", ["全部"] + PROD_TYPES, key="dash_ptype_filter")
                    with c6: s_machine = st.selectbox("機台類型篩選", ["全部"] + MACHINE_TYPES, key="dash_mac_filter")
                
                f_df = full_df.copy()
                if v_mode == "個人": f_df = f_df[f_df['填寫人'] == s_emp]
                if isinstance(d_range, (list, tuple)) and len(d_range) == 2:
                    f_df = f_df[(f_df['日期_date'] >= d_range[0]) & (f_df['日期_date'] <= d_range[1])]
                if s_status != "全部": f_df = f_df[f_df['狀態'] == s_status]
                if s_type != "全部": f_df = f_df[f_df['生產類型'] == s_type]
                if s_machine != "全部": f_df = f_df[f_df['機台類型'] == s_machine]

                done_df = f_df[f_df['狀態'] == '已完成']
                st.markdown("### 📌 關鍵指標彙總")
                k1, k2, k3, k4, k5, k6 = st.columns(6)
                k1.metric("總工作區間", f"{round(done_df['工作區間工時'].sum(), 1)} h")
                k2.metric("總實際加工", f"{round(done_df['實際工時'].sum(), 1)} h")
                k3.metric("區間未加工時間", f"{round(done_df['時間差異'].sum(), 1)} h", delta_color="inverse")
                k4.metric("進行中工單", f"{len(f_df[f_df['狀態'] == '進行中'])} 筆")
                k5.metric("暫停中工單", f"{len(f_df[f_df['狀態'] == '暫停中'])} 筆")
                k6.metric("已完成工單", f"{len(done_df)} 筆")

                with st.container(border=True):
                    st.markdown("### 📈 數據分析戰情室")
                    if done_df.empty: st.info("無已完成工單。")
                    else:
                        t_level = st.radio("分析層級", ["月統計", "日統計", "工單明細"], horizontal=True, key="dash_t_level")
                        x_field = "年月" if t_level == "月統計" else ("月日" if t_level == "日統計" else "工單ID")
                        chart_df = done_df.groupby([x_field, '生產類型']).agg({'實際工時':'sum', '預估工時':'sum'}).reset_index()
                        time_agg = done_df.groupby(x_field).agg({'實際工時':'sum', '預估工時':'sum'}).reset_index()
                        time_agg['偏差'] = time_agg['實際工時'] - time_agg['預估工時']
                        time_agg['標籤'] = time_agg['偏差'].apply(lambda x: f"{'+' if x>0 else ''}{round(x,1)}h")
                        time_agg['偏差顏色'] = time_agg['偏差'].apply(get_diff_color)

                        c_range = ['#1f77b4', '#ff7f0e', '#d62728', '#9467bd', '#8c564b']
                        bars = alt.Chart(chart_df).mark_bar().encode(
                            x=alt.X(f'{x_field}:N', title='時間維度', axis=alt.Axis(labelAngle=-20)),
                            y=alt.Y('實際工時:Q', title='工時 (h)'),
                            xOffset=alt.XOffset('生產類型:N'),
                            color=alt.Color('生產類型:N', scale=alt.Scale(domain=PROD_TYPES, range=c_range))
                        )
                        line = alt.Chart(time_agg).mark_line(point=True, color='black').encode(
                            x=alt.X(f'{x_field}:N'), y=alt.Y('預估工時:Q')
                        )
                        text = alt.Chart(time_agg).mark_text(dy=-15, fontWeight='bold').encode(
                            x=alt.X(f'{x_field}:N'), y='實際工時:Q', text='標籤:N', color=alt.Color('偏差顏色:N', scale=None)
                        )
                        st.altair_chart((bars + line + text).properties(height=350), use_container_width=True)

                st.write("### 🔍 詳細生產紀錄")
                st.dataframe(f_df[[c for c in STANDARD_COLS if c in f_df.columns]], use_container_width=True)
    else: st.warning("🔒 需輸入主管密碼。")

# --- 頁籤 4：主管後台管理 ---
with tab4:
    if st.session_state["is_admin"]:
        st.title("🛠️ 主管後台管理")
        st.subheader("📝 工單資料修正")
        if db_df.empty: st.info("無工單可修改。")
        else:
            with st.container(border=True):
                st.write("**步驟一：篩選**")
                c_f1, c_f2, c_f3 = st.columns(3)
                with c_f1: a_emp = st.selectbox("填寫人", ["全部"] + emps, key="a_emp")
                with c_f2: a_stat = st.selectbox("狀態", ["全部", "進行中", "暫停中", "已完成"], key="admin_stat_filter")
                with c_f3: a_kw = st.text_input("工單號碼/圖號 關鍵字", key="admin_kw_filter")
                
                e_df = db_df.copy()
                if a_emp != "全部": e_df = e_df[e_df['填寫人'] == a_emp]
                if a_stat != "全部": e_df = e_df[e_df['狀態'] == a_stat]
                if a_kw: e_df = e_df[e_df['工單號碼'].astype(str).str.contains(a_kw) | e_df['圖號'].astype(str).str.contains(a_kw)]
                
                st.dataframe(e_df, height=200)
                
                edit_id = st.selectbox("選擇要修改的工單", [""] + list(e_df['工單ID']), key="admin_edit_id")
                if edit_id:
                    row_data = db_df[db_df['工單ID'] == edit_id].iloc[0]
                    col_1, col_2, col_3 = st.columns(3)
                    with col_1:
                        n_emp = st.text_input("填寫人", row_data.get('填寫人'), key="adm_e_emp")
                        n_type = st.selectbox("生產類型", PROD_TYPES, index=PROD_TYPES.index(row_data.get('生產類型', '正常生產')) if row_data.get('生產類型') in PROD_TYPES else 0, key="adm_e_type")
                        n_qty = st.number_input("數量", value=int(row_data.get('工件數量', 1)), key="adm_e_qty")
                    with col_2:
                        n_mac = st.selectbox("機台", MACHINE_TYPES, index=MACHINE_TYPES.index(row_data.get('機台類型', '磨床')) if row_data.get('機台類型') in MACHINE_TYPES else 0, key="adm_e_mac")
                        n_est = st.number_input("預估工時", value=float(row_data.get('預估工時', 0)), key="adm_e_est")
                        n_act = st.number_input("實際工時", value=float(row_data.get('實際工時', 0)), key="adm_e_act")
                    with col_3:
                        n_stat = st.selectbox("狀態", ["進行中", "暫停中", "已完成"], index=["進行中", "暫停中", "已完成"].index(row_data.get('狀態', '已完成')) if row_data.get('狀態') in ["進行中", "暫停中", "已完成"] else 2, key="adm_e_stat")
                        n_note = st.text_area("備註", row_data.get('備註', ''), key="adm_e_note")
                        
                    if st.button("💾 儲存修改", type="primary"):
                        backup_factory_db()
                        rdb = load_work_orders_raw()
                        m = rdb['工單ID'] == edit_id
                        rdb.loc[m, ['填寫人','生產類型','工件數量','機台類型','預估工時','實際工時','狀態','備註']] = \
                            [n_emp, n_type, n_qty, n_mac, n_est, n_act, n_stat, n_note]
                        save_work_orders(rdb)
                        st.success("✅ 修改成功！"); st.rerun()

        st.subheader("🗑️ 工單刪除")
        del_ids = st.multiselect("選擇要刪除的工單ID", list(db_df['工單ID']), key="adm_del_ids")
        if del_ids and st.checkbox("確認永久刪除") and st.button("☠️ 永久刪除"):
            backup_factory_db()
            rdb = load_work_orders_raw()
            rdb = rdb[~rdb['工單ID'].isin(del_ids)]
            save_work_orders(rdb)
            st.success(f"✅ 刪除 {len(del_ids)} 筆工單！"); st.rerun()
    else: st.warning("🔒 需輸入主管密碼。")
