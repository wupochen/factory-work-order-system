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

# --- 0. Streamlit 頁面設定 (必須在最前面) ---
st.set_page_config(page_title="工廠生產管理系統 V5 (防限流快取版)", layout="wide")

# --- 1. 系統常數、密碼與時區設定 ---
TAIWAN_TZ = ZoneInfo("Asia/Taipei")

# Google Sheets 是主資料庫，factory_db.csv 只作為本機備份，不可作為主要讀取來源
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

# 生產類型與中越對照表
PROD_TYPES = ["正常生產", "插件", "NG重修", "重製"]
PROD_TYPES_BILINGUAL = {
    "正常生產": "正常生產 / Sản xuất bình thường",
    "插件": "插件 / Công việc chen ngang",
    "NG重修": "NG重修 / Sửa lại NG",
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

# 主管密碼設定
ADMIN_PASSWORD = "0000"

# STANDARD_COLS 標準欄位定義 (包含工單號碼，順序固定)
STANDARD_COLS = [
    '工單ID', '日期', '填寫人', '生產類型', '機台類型', '工單號碼', '圖號', '工件數量', '預估工時',
    '實際工時', '開始時間', '結束時間', '工作區間工時',
    '累積工作區間工時', '最後恢復時間', '暫停時間', '暫停原因',
    '時間差異', '狀態', '備註'
]

# 初始化 Session State (紀錄登入狀態)
if "is_admin" not in st.session_state:
    st.session_state["is_admin"] = False

# 讀取 LINE Messaging API Secrets
try:
    LINE_CHANNEL_ACCESS_TOKEN = st.secrets.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    LINE_TO_ID = st.secrets.get("LINE_TO_ID", "")
except Exception:
    LINE_CHANNEL_ACCESS_TOKEN = ""
    LINE_TO_ID = ""

# --- 2. Google Sheets 連線與核心資料存取 ---

@st.cache_resource
def get_gsheet_client():
    if "gcp_service_account" not in st.secrets or "GSHEET_ID" not in st.secrets:
        st.error("""❌ **Google Sheets 連線失敗，請檢查：**
        1. Streamlit Secrets 是否有 `gcp_service_account`
        2. Streamlit Secrets 是否有 `GSHEET_ID`
        3. Google Sheet 是否已分享給 service account 的 `client_email`
        4. 分享權限是否為「編輯者」
        5. Google Cloud 是否已啟用 Google Sheets API 與 Google Drive API
        6. `requirements.txt` 是否有 `gspread` 和 `google-auth`
        """)
        return None
        
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        skey = st.secrets["gcp_service_account"]
        credentials = Credentials.from_service_account_info(skey, scopes=scopes)
        gc = gspread.authorize(credentials)
        return gc
    except Exception as e:
        st.error(f"❌ **Google Sheets 連線失敗，詳細錯誤原因：** {str(e)}")
        return None

def normalize_db_df(df):
    num_cols = ['工件數量', '預估工時', '實際工時', '工作區間工時', '累積工作區間工時', '時間差異']
    for c in df.columns:
        if c in num_cols:
            if c == '工件數量':
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(1).astype(int)
            else:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
        else:
            df[c] = df[c].fillna("").astype(str)
    return df

def parse_taiwan_time(value):
    try:
        if pd.isna(value) or str(value).strip() == "":
            return pd.NaT
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return pd.NaT
        if dt.tzinfo is None:
            return dt.tz_localize(TAIWAN_TZ)
        else:
            return dt.tz_convert(TAIWAN_TZ)
    except Exception:
        return pd.NaT

def calculate_work_hours_excluding_lunch(start_dt, end_dt):
    """
    計算 start_dt 到 end_dt 的小時數，並自動扣除每天 12:00~13:00 午休時間。
    start_dt 與 end_dt 必須是 Asia/Taipei timezone-aware datetime。
    """
    try:
        if pd.isna(start_dt) or pd.isna(end_dt):
            return 0.0

        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=TAIWAN_TZ)
        else:
            start_dt = start_dt.astimezone(TAIWAN_TZ)

        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=TAIWAN_TZ)
        else:
            end_dt = end_dt.astimezone(TAIWAN_TZ)

        if end_dt <= start_dt:
            return 0.0

        total_seconds = (end_dt - start_dt).total_seconds()
        lunch_seconds = 0

        current_day = start_dt.date()
        end_day = end_dt.date()

        while current_day <= end_day:
            lunch_start = datetime.combine(current_day, datetime.min.time()).replace(
                hour=12, minute=0, second=0, microsecond=0, tzinfo=TAIWAN_TZ
            )
            lunch_end = datetime.combine(current_day, datetime.min.time()).replace(
                hour=13, minute=0, second=0, microsecond=0, tzinfo=TAIWAN_TZ
            )

            overlap_start = max(start_dt, lunch_start)
            overlap_end = min(end_dt, lunch_end)

            if overlap_end > overlap_start:
                lunch_seconds += (overlap_end - overlap_start).total_seconds()

            current_day = current_day + timedelta(days=1)

        work_hours = (total_seconds - lunch_seconds) / 3600
        return round(max(0.0, work_hours), 2)

    except Exception:
        return 0.0

def calculate_elapsed_hours(start_dt, end_dt):
    """
    單純計算 start_dt 到 end_dt 的總經過小時，不扣午休。
    start_dt 與 end_dt 需要轉成 Asia/Taipei timezone-aware datetime。
    """
    try:
        if pd.isna(start_dt) or pd.isna(end_dt):
            return 0.0

        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=TAIWAN_TZ)
        else:
            start_dt = start_dt.astimezone(TAIWAN_TZ)

        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=TAIWAN_TZ)
        else:
            end_dt = end_dt.astimezone(TAIWAN_TZ)

        if end_dt <= start_dt:
            return 0.0

        return round(max(0.0, (end_dt - start_dt).total_seconds() / 3600), 2)

    except Exception:
        return 0.0

@st.cache_resource
def init_gsheets_once():
    """初始化並確保 Google Sheets 的表頭與工作表齊全 (全域僅啟動時執行一次)"""
    gc = get_gsheet_client()
    if not gc: return
    try:
        sh = gc.open_by_key(st.secrets["GSHEET_ID"])
        # 確保有 work_orders 工作表
        try:
            wo_sheet = sh.worksheet("work_orders")
            headers = wo_sheet.row_values(1)
            if not headers:
                wo_sheet.append_row(STANDARD_COLS)
            else:
                missing = [c for c in STANDARD_COLS if c not in headers]
                if missing:
                    headers.extend(missing)
                    wo_sheet.update(values=[headers], range_name='A1')
        except gspread.exceptions.WorksheetNotFound:
            wo_sheet = sh.add_worksheet(title="work_orders", rows="100", cols="20")
            wo_sheet.append_row(STANDARD_COLS)
            
        # 確保有 employees 工作表
        try:
            emp_sheet = sh.worksheet("employees")
        except gspread.exceptions.WorksheetNotFound:
            emp_sheet = sh.add_worksheet(title="employees", rows="100", cols="5")
            emp_sheet.append_row(["員工名字"])
    except Exception as e:
        st.error(f"初始化 Google Sheets 失敗: {e}")

# --- 資料讀取區 (Raw) ---
def load_work_orders_raw():
    """從 Google Sheets 讀取最新工單資料 (僅在修改資料前呼叫，避免 429)"""
    gc = get_gsheet_client()
    if not gc: return pd.DataFrame(columns=STANDARD_COLS)
    try:
        sh = gc.open_by_key(st.secrets["GSHEET_ID"])
        worksheet = sh.worksheet("work_orders")
        data = worksheet.get_all_records()
        
        if not data:
            df = pd.DataFrame(columns=STANDARD_COLS)
        else:
            df = pd.DataFrame(data)
            
        for c in STANDARD_COLS:
            if c not in df.columns:
                if c in ['預估工時', '實際工時', '工作區間工時', '累積工作區間工時', '時間差異']: 
                    df[c] = 0.0
                elif c == '工件數量':
                    df[c] = 1
                elif c == '機台類型':
                    df[c] = '磨床'
                elif c == '狀態': 
                    df[c] = '已完成'
                else: 
                    df[c] = ""
                    
        df = df[STANDARD_COLS]
        return normalize_db_df(df)
    except Exception as e:
        st.error(f"讀取 work_orders 失敗: {e}")
        return pd.DataFrame(columns=STANDARD_COLS)

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

# --- 資料讀取區 (快取版 Cache) ---
@st.cache_data(ttl=30)
def load_work_orders_cached():
    return load_work_orders_raw()

@st.cache_data(ttl=60)
def load_employees_cached():
    return load_employees_raw()

# --- 資料寫入區 ---
def save_work_orders(df):
    """將工單資料覆蓋寫入 Google Sheets，並清除快取"""
    gc = get_gsheet_client()
    if not gc: return
    try:
        sh = gc.open_by_key(st.secrets["GSHEET_ID"])
        worksheet = sh.worksheet("work_orders")
        
        for c in STANDARD_COLS:
            if c not in df.columns:
                df[c] = ""
        df = df[STANDARD_COLS]
        df = df.fillna("")
        
        worksheet.clear()
        worksheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name='A1')
        
        # 同步備份到本機 CSV
        df.to_csv(DB_FILE, index=False, encoding='utf-8-sig')
        
        # 寫入成功後清除快取，讓所有頁面抓到最新資料
        st.cache_data.clear()
    except Exception as e:
        st.error(f"寫入 work_orders 失敗: {e}")

def append_work_order(row_dict):
    """快速新增一筆工單至 Google Sheets，並清除快取"""
    gc = get_gsheet_client()
    if not gc: return
    try:
        sh = gc.open_by_key(st.secrets["GSHEET_ID"])
        worksheet = sh.worksheet("work_orders")
        row_values = [str(row_dict.get(col, "")) for col in STANDARD_COLS]
        worksheet.append_row(row_values)
        
        st.cache_data.clear() # 寫入成功後清除快取
    except Exception as e:
        st.error(f"新增工單失敗: {e}")

def save_employees_to_sheet(emp_list):
    """更新員工名單至 Google Sheets，並清除快取"""
    gc = get_gsheet_client()
    if not gc: return
    try:
        sh = gc.open_by_key(st.secrets["GSHEET_ID"])
        worksheet = sh.worksheet("employees")
        worksheet.clear()
        worksheet.update(values=[["員工名字"]] + [[e] for e in emp_list], range_name='A1')
        st.cache_data.clear() # 寫入成功後清除快取
    except Exception as e:
        st.error(f"更新員工名單失敗: {e}")

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

def get_diff_color(x):
    if x > 0: return "red"
    elif x < 0: return "green"
    else: return "gray"

def send_line_message(msg):
    if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_TO_ID:
        return False, "LINE 憑證尚未設定"
    try:
        url = "https://api.line.me/v2/bot/message/push"
        headers = {
            "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }
        payload = {
            "to": LINE_TO_ID,
            "messages": [{"type": "text", "text": msg}]
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=5)
        if resp.status_code == 200:
            return True, ""
        else:
            return False, f"HTTP {resp.status_code}，內容：{resp.text}"
    except Exception as e:
        return False, f"例外錯誤：{e}"

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

# 啟動時自動初始化 Google Sheets (只執行一次)
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
            else:
                st.error("❌ 密碼錯誤")
    else:
        st.success("✅ 已登入主管模式")
        if st.button("登出主管模式"):
            st.session_state["is_admin"] = False
            st.info("已登出主管模式")
            st.rerun()

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
                        save_employees_to_sheet(latest_emps + [new_emp])
                        st.success(f"已新增：{new_emp}")
                        st.rerun()
            
            st.divider()
            st.subheader("💬 LINE 通知設定狀態")
            if LINE_CHANNEL_ACCESS_TOKEN: st.write("✅ LINE Token 已設定")
            else: st.write("❌ LINE_CHANNEL_ACCESS_TOKEN 尚未設定")

            if LINE_TO_ID: st.write("✅ LINE_TO_ID 已設定")
            else: st.write("❌ LINE_TO_ID 尚未設定")

            st.write("🕓 未結案提醒：每日 16:55、18:00、21:00 自動推播")

            if st.button("🔔 測試未結案工單提醒"):
                send_unfinished_work_orders_reminder("手動測試")
                st.success("✅ 未結案工單提醒指令已送出！(請檢查 LINE 或終端機)")
    else:
        st.info("🔒 系統設定需主管密碼")

# 集中讀取快取資料給各頁籤共用 (減少 API 讀取次數)
if not is_print_mode:
    emps_cached = load_employees_cached()
    db_df_cached = load_work_orders_cached()
    tab1, tab2, tab3, tab4 = st.tabs(["🏗️ 磨床報工", "⚡ 放電機/快走絲報工", "📊 主管數據看板", "🛠️ 主管後台管理"])
else:
    tab1, tab2, tab3, tab4 = st.empty(), st.empty(), st.container(), st.empty()

# --- 頁籤 1：磨床報工 (免密碼) ---
if not is_print_mode:
    with tab1:
        st.header("磨床即時加工報工 / Báo cáo gia công máy mài")
        emps = emps_cached
        db_df = db_df_cached
        
        st.subheader("🆕 開始新工單 / Bắt đầu lệnh sản xuất mới")
        with st.container(border=True):
            col_s1, col_s2, col_s3 = st.columns(3)
            with col_s1:
                s_name = st.selectbox("填寫人 / Người điền", emps, key="g_name")
                s_type = st.selectbox("生產類型 / Loại sản xuất", PROD_TYPES, format_func=lambda x: PROD_TYPES_BILINGUAL.get(x, x), key="g_type")
            with col_s2:
                g_work_order_no = st.text_input("工單號碼 / Số lệnh sản xuất", key="g_wo_no").strip()
                s_drawing = st.text_input("圖號 / Bản vẽ", key="g_drawing").strip()
            with col_s3:
                s_qty = st.number_input("工件數量 / Số lượng sản phẩm", min_value=1, value=1, step=1, key="g_qty")
                s_est = st.number_input("預估工時 / TG dự kiến (hrs)", min_value=0.0, step=0.1, key="g_est")
            
            if st.button("▶️ 開始加工 / Bắt đầu gia công", type="primary", key="g_start"):
                if not s_drawing: st.error("❌ 請輸入圖號！ / Vui lòng nhập số bản vẽ!")
                elif s_est <= 0: st.error("❌ 預估工時不可為 0！ / TG dự kiến không được bằng 0!")
                else:
                    now = datetime.now(TAIWAN_TZ)
                    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
                    wo_id = f"WO-{now.strftime('%Y%m%d%H%M%S%f')}"
                    new_entry = {
                        '工單ID': wo_id, '日期': now.strftime("%Y-%m-%d"), '填寫人': s_name,
                        '生產類型': s_type, '機台類型': '磨床', '工單號碼': g_work_order_no, '圖號': s_drawing, '工件數量': s_qty, '預估工時': s_est,
                        '實際工時': 0.0, '開始時間': now_str,
                        '結束時間': "", '工作區間工時': 0.0,
                        '累積工作區間工時': 0.0, '最後恢復時間': now_str, '暫停時間': "", '暫停原因': "",
                        '時間差異': 0.0, '狀態': '進行中', '備註': ""
                    }
                    append_work_order(new_entry)
                    st.success(f"✅ 工單已啟動！")
                    st.rerun()

        st.divider()

        st.subheader("⏳ 進行中的工單查詢 / Tra cứu lệnh đang thực hiện")
        filter_ongoing = st.selectbox("查看進行中工單", ["全部"] + emps, index=0, key="g_filter_ongoing")
        ongoing_df = db_df[(db_df['狀態'] == '進行中') & (db_df['機台類型'] == '磨床')].copy()
        if filter_ongoing != "全部": ongoing_df = ongoing_df[ongoing_df['填寫人'] == filter_ongoing]
            
        if ongoing_df.empty:
            st.info(f"目前沒有 {filter_ongoing if filter_ongoing != '全部' else ''} 正在進行的工單。")
        else:
            for index, row in ongoing_df.iterrows():
                wo_no_disp = row.get('工單號碼', '')
                expander_title = f"🛠️ {row['填寫人']} | {row.get('機台類型', '磨床')} | 工單號碼：{wo_no_disp if wo_no_disp else '未填寫'} | 圖號：{row['圖號']} ({row['生產類型']}) - 開始於 {row['開始時間']}"
                with st.expander(expander_title):
                    
                    resume_dt = parse_taiwan_time(row.get('最後恢復時間', ''))
                    if pd.isna(resume_dt):
                        resume_dt = parse_taiwan_time(row['開始時間'])
                    if pd.isna(resume_dt):
                        st.error("❌ 此工單時間格式異常，無法計算，請主管檢查。")
                        continue
                    
                    now_dt = datetime.now(TAIWAN_TZ)
                    segment_h = calculate_work_hours_excluding_lunch(resume_dt, now_dt)
                    
                    old_acc = pd.to_numeric(row.get('累積工作區間工時', 0), errors='coerce')
                    if pd.isna(old_acc): old_acc = 0.0
                    current_total_h = round(old_acc + segment_h, 2)
                    
                    st.write(f"**工單號碼 / Số lệnh sản xuất:** {wo_no_disp if wo_no_disp else '未填寫'}")
                    st.write(f"**圖號 / Bản vẽ:** {row['圖號']}")
                    st.caption(f"系統ID: `{row['工單ID']}`")
                    st.write(f"**工件數量 / Số lượng sản phẩm:** {row.get('工件數量', 1)}")
                    st.info(f"⏱️ 系統累積工作區間工時: {current_total_h} 小時")
                    
                    st.divider()
                    
                    st.markdown("### ⏸️ 暫停加工 / Tạm dừng gia công")
                    p_reason = st.selectbox("暫停原因 / Lý do tạm dừng", PAUSE_REASONS, format_func=lambda x: PAUSE_REASONS_BILINGUAL.get(x, x), key=f"pr_{row['工單ID']}")
                    if st.button("⏸️ 暫停加工 / Tạm dừng", key=f"pb_{row['工單ID']}"):
                        current_db = load_work_orders_raw()
                        mask = (current_db['工單ID'] == row['工單ID']) & (current_db['狀態'] == '進行中')
                        if not current_db[mask].empty:
                            pause_now = datetime.now(TAIWAN_TZ)
                            current_db.loc[mask, '累積工作區間工時'] = current_total_h
                            current_db.loc[mask, '狀態'] = '暫停中'
                            current_db.loc[mask, '暫停時間'] = pause_now.strftime("%Y-%m-%d %H:%M:%S")
                            current_db.loc[mask, '暫停原因'] = p_reason
                            save_work_orders(current_db)
                            st.success(f"⏸️ 工單已暫停，累積區間：{current_total_h}h"); st.rerun()
                        else: st.error("❌ 此工單可能已被其他人修改，請重新整理後再試。")
                    
                    st.divider()

                    st.markdown("### ✅ 加工完成 / Kết thúc lệnh")
                    e_note = st.text_area(f"備註 / 異常原因 / Ghi chú", key=f"note_{row['工單ID']}")
                    
                    if st.button(f"✅ 加工完成並結案 / Hoàn thành", key=f"btn_{row['工單ID']}", type="primary"):
                        if row['生產類型'] != "正常生產" and not e_note.strip():
                            st.error("❌ 異常件請務必填寫備註原因！ / Vui lòng điền lý do bất thường!")
                        else:
                            current_db = load_work_orders_raw()
                            mask = (current_db['工單ID'] == row['工單ID']) & (current_db['狀態'] == '進行中')
                            if not current_db[mask].empty:
                                end_now = datetime.now(TAIWAN_TZ)
                                
                                # 磨床全由系統判定，不再手動輸入實際工時
                                e_act = current_total_h
                                diff_time = 0.0
                                
                                current_db.loc[mask, '結束時間'] = end_now.strftime("%Y-%m-%d %H:%M:%S")
                                current_db.loc[mask, '實際工時'] = e_act
                                current_db.loc[mask, '工作區間工時'] = current_total_h
                                current_db.loc[mask, '累積工作區間工時'] = current_total_h
                                current_db.loc[mask, '時間差異'] = diff_time
                                current_db.loc[mask, '狀態'] = '已完成'
                                current_db.loc[mask, '備註'] = e_note.strip()
                                
                                save_work_orders(current_db)
                                
                                line_ok = True
                                line_error = ""
                                if row['生產類型'] != "正常生產":
                                    line_ok, line_error = send_line_message(
                                        f"\n⚠️異常結案通知\n"
                                        f"機台：{row.get('機台類型', '磨床')}\n"
                                        f"類型：{row['生產類型']}\n"
                                        f"人員：{row['填寫人']}\n"
                                        f"工單號碼：{row.get('工單號碼', '')}\n"
                                        f"圖號：{row['圖號']}\n"
                                        f"數量：{row.get('工件數量', 1)}\n"
                                        f"實際加工：{e_act}h\n"
                                        f"工作區間：{current_total_h}h\n"
                                        f"區間未加工：{diff_time}h\n"
                                        f"備註：{e_note.strip()}"
                                    )

                                if row['生產類型'] != "正常生產" and not line_ok:
                                    st.warning("⚠️ 工單已結案，但 LINE 通知可能未成功送出。")
                                    st.caption(line_error)

                                st.success(f"✅ 已結案！系統計算工作區間：{current_total_h}h")
                                st.rerun()
                            else: st.error("❌ 此工單可能已被其他人修改，請重新整理後再試。")

        st.divider()
        st.subheader("⏸️ 暫停中的工單查詢 / Tra cứu lệnh đang tạm dừng")
        filter_paused = st.selectbox("查看暫停中工單", ["全部"] + emps, index=0, key="g_f_pause")
        pause_df = db_df[(db_df['狀態'] == '暫停中') & (db_df['機台類型'] == '磨床')].copy()
        if filter_paused != "全部": pause_df = pause_df[pause_df['填寫人'] == filter_paused]
            
        if pause_df.empty:
            st.info(f"目前沒有 {filter_paused if filter_paused != '全部' else ''} 暫停中的工單。")
        else:
            for index, row in pause_df.iterrows():
                with st.container(border=True):
                    col_p1, col_p2 = st.columns([3, 1])
                    with col_p1:
                        wo_no_disp = row.get('工單號碼', '')
                        st.write(f"**工單號碼 / Số lệnh sản xuất:** {wo_no_disp if wo_no_disp else '未填寫'}")
                        st.write(f"**圖號 / Bản vẽ:** {row['圖號']} ({row['生產類型']}) | **人員:** {row['填寫人']}")
                        st.caption(f"系統ID: `{row['工單ID']}` | **機台:** {row.get('機台類型', '磨床')}")
                        st.write(f"**開始於:** {row['開始時間']} | **工件數量:** {row.get('工件數量', 1)}")
                        st.error(f"⏸️ 暫停原因: {PAUSE_REASONS_BILINGUAL.get(row.get('暫停原因', ''), row.get('暫停原因', '未填寫'))} (於 {row.get('暫停時間', '')})")
                        st.info(f"⏱️ 累積工作區間工時: {row.get('累積工作區間工時', 0.0)} 小時")
                    with col_p2:
                        st.write("") 
                        if st.button("▶️ 繼續加工 / Tiếp tục", key=f"r_btn_{row['工單ID']}", type="primary", use_container_width=True):
                            current_db = load_work_orders_raw()
                            mask = (current_db['工單ID'] == row['工單ID']) & (current_db['狀態'] == '暫停中')
                            if not current_db[mask].empty:
                                resume_now = datetime.now(TAIWAN_TZ)
                                current_db.loc[mask, '狀態'] = '進行中'
                                current_db.loc[mask, '最後恢復時間'] = resume_now.strftime("%Y-%m-%d %H:%M:%S")
                                save_work_orders(current_db)
                                st.success("▶️ 已恢復加工！"); st.rerun()
                            else: st.error("❌ 此工單可能已被其他人修改，請重新整理後再試。")

# --- 頁籤 2：放電機/快走絲報工 (免密碼) ---
if not is_print_mode:
    with tab2:
        st.header("放電機/快走絲報工 / Báo cáo máy EDM / máy cắt dây nhanh")
        emps = emps_cached
        db_df = db_df_cached
        
        st.subheader("🆕 開始新工單 / Bắt đầu lệnh sản xuất mới")
        with st.container(border=True):
            col_ew1, col_ew2, col_ew3 = st.columns(3)
            with col_ew1:
                ew_name = st.selectbox("填寫人 / Người điền", emps, key="ew_name")
                ew_machine = st.selectbox("機台類型 / Loại máy", ["放電機", "快走絲"], format_func=lambda x: MACHINE_TYPES_BILINGUAL.get(x, x), key="ew_machine")
            with col_ew2:
                ew_type = st.selectbox("生產類型 / Loại sản xuất", PROD_TYPES, format_func=lambda x: PROD_TYPES_BILINGUAL.get(x, x), key="ew_type")
                ew_work_order_no = st.text_input("工單號碼 / Số lệnh sản xuất", key="ew_wo_no").strip()
            with col_ew3:
                ew_drawing = st.text_input("圖號 / Bản vẽ", key="ew_drawing").strip()
                ew_qty = st.number_input("工件數量 / Số lượng sản phẩm", min_value=1, value=1, step=1, key="ew_qty")
                ew_est = st.number_input("預估機台工時 / Thời gian máy dự kiến (hrs)", min_value=0.0, step=0.1, key="ew_est")
            
            if st.button("▶️ 開始加工 / Bắt đầu gia công", type="primary", key="ew_start"):
                if not ew_drawing: st.error("❌ 請輸入圖號！ / Vui lòng nhập số bản vẽ!")
                elif ew_est <= 0: st.error("❌ 預估機台工時不可為 0！ / TG dự kiến không được bằng 0!")
                else:
                    now = datetime.now(TAIWAN_TZ)
                    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
                    wo_id = f"WO-{now.strftime('%Y%m%d%H%M%S%f')}"
                    new_entry = {
                        '工單ID': wo_id, '日期': now.strftime("%Y-%m-%d"), '填寫人': ew_name,
                        '生產類型': ew_type, '機台類型': ew_machine, '工單號碼': ew_work_order_no, '圖號': ew_drawing, '工件數量': ew_qty, '預估工時': ew_est,
                        '實際工時': 0.0, '開始時間': now_str,
                        '結束時間': "", '工作區間工時': 0.0,
                        '累積工作區間工時': 0.0, '最後恢復時間': now_str, '暫停時間': "", '暫停原因': "",
                        '時間差異': 0.0, '狀態': '進行中', '備註': ""
                    }
                    append_work_order(new_entry)
                    st.success(f"✅ 工單已啟動！")
                    st.rerun()

        st.divider()

        st.subheader("⏳ 進行中的工單查詢 / Tra cứu lệnh đang thực hiện")
        filter_ongoing_ew = st.selectbox("查看進行中工單", ["全部"] + emps, index=0, key="ew_filter_ongoing")
        ongoing_df_ew = db_df[(db_df['狀態'] == '進行中') & (db_df['機台類型'].isin(["放電機", "快走絲"]))].copy()
        if filter_ongoing_ew != "全部": ongoing_df_ew = ongoing_df_ew[ongoing_df_ew['填寫人'] == filter_ongoing_ew]
            
        if ongoing_df_ew.empty:
            st.info(f"目前沒有 {filter_ongoing_ew if filter_ongoing_ew != '全部' else ''} 正在進行的工單。")
        else:
            now_tz = datetime.now(TAIWAN_TZ)
            for index, row in ongoing_df_ew.iterrows():
                wo_no_disp = row.get('工單號碼', '')
                expander_title = f"🛠️ {row['填寫人']} | {row.get('機台類型', '')} | 工單號碼：{wo_no_disp if wo_no_disp else '未填寫'} | 圖號：{row['圖號']} ({row['生產類型']}) - 開始於 {row['開始時間']}"
                with st.expander(expander_title):
                    
                    start_dt = parse_taiwan_time(row['開始時間'])
                    resume_dt = parse_taiwan_time(row.get('最後恢復時間', ''))
                    if pd.isna(resume_dt):
                        resume_dt = start_dt
                    if pd.isna(resume_dt) or pd.isna(start_dt):
                        st.error("❌ 此工單時間格式異常，無法計算，請主管檢查。")
                        continue
                    
                    segment_h_now = calculate_work_hours_excluding_lunch(resume_dt, now_tz)
                    old_acc = pd.to_numeric(row.get('累積工作區間工時', 0), errors='coerce')
                    if pd.isna(old_acc): old_acc = 0.0
                    current_total_now = round(old_acc + segment_h_now, 2)
                    
                    st.write(f"**工單號碼 / Số lệnh sản xuất:** {wo_no_disp if wo_no_disp else '未填寫'}")
                    st.write(f"**圖號 / Bản vẽ:** {row['圖號']}")
                    st.caption(f"系統ID: `{row['工單ID']}`")
                    st.write(f"**工件數量 / Số lượng sản phẩm:** {row.get('工件數量', 1)}")
                    st.info(f"⏱️ 系統目前累積工作區間 (機台運轉) 工時: {current_total_now} 小時")
                    
                    st.divider()
                    
                    st.markdown("### ⏸️ 暫停加工 / Tạm dừng gia công")
                    p_reason_ew = st.selectbox("暫停原因 / Lý do tạm dừng", PAUSE_REASONS, format_func=lambda x: PAUSE_REASONS_BILINGUAL.get(x, x), key=f"pr_ew_{row['工單ID']}")
                    if st.button("⏸️ 暫停加工 / Tạm dừng", key=f"pb_ew_{row['工單ID']}"):
                        current_db = load_work_orders_raw()
                        mask = (current_db['工單ID'] == row['工單ID']) & (current_db['狀態'] == '進行中')
                        if not current_db[mask].empty:
                            pause_now = datetime.now(TAIWAN_TZ)
                            current_db.loc[mask, '累積工作區間工時'] = current_total_now
                            current_db.loc[mask, '狀態'] = '暫停中'
                            current_db.loc[mask, '暫停時間'] = pause_now.strftime("%Y-%m-%d %H:%M:%S")
                            current_db.loc[mask, '暫停原因'] = p_reason_ew
                            save_work_orders(current_db)
                            st.success(f"⏸️ 工單已暫停，累積機台區間：{current_total_now}h"); st.rerun()
                        else: st.error("❌ 此工單可能已被其他人修改，請重新整理後再試。")
                    
                    st.divider()

                    st.markdown("### ✅ 加工完成 / Kết thúc lệnh")
                    col_end1, col_end2 = st.columns(2)
                    with col_end1:
                        end_d = st.date_input("機台實際停止日期 / Ngày máy dừng thực tế", value=now_tz.date(), key=f"end_d_{row['工單ID']}")
                    with col_end2:
                        end_t = st.time_input("機台實際停止時間 / Giờ máy dừng thực tế", value=now_tz.time(), key=f"end_t_{row['工單ID']}")
                    
                    lunch_running = st.checkbox("午休是否持續加工 / Máy có chạy trong giờ nghỉ trưa", value=True, key=f"lunch_run_ew_{row['工單ID']}")
                    
                    st.info("""📌 填寫說明：
請填寫「機台實際停止的日期與時間」，系統會自動計算機台工時，員工不用自己算幾小時。

若機台下班後有繼續掛機加工，請以機台晚上真正停止的時間為準。
例如：機台 20:30 停止，就請填 20:30。

若中午 12:00～13:00 機台有繼續加工，請勾選「午休是否持續加工」。
有勾選：中午 1 小時會算進機台工時。
沒勾選：系統會自動扣掉中午 1 小時。

📌 Hướng dẫn:
Vui lòng nhập ngày và giờ máy thật sự dừng. Hệ thống sẽ tự động tính thời gian máy chạy, nhân viên không cần tự tính số giờ.

Nếu máy vẫn chạy sau giờ làm, hãy nhập thời gian máy thật sự dừng vào buổi tối.
Ví dụ: Nếu máy dừng lúc 20:30, vui lòng nhập 20:30.

Nếu máy vẫn chạy trong giờ nghỉ trưa 12:00～13:00, hãy đánh dấu vào ô “Máy có chạy trong giờ nghỉ trưa”.
Có đánh dấu: 1 giờ nghỉ trưa sẽ được tính vào thời gian máy chạy.
Không đánh dấu: hệ thống sẽ tự động trừ 1 giờ nghỉ trưa.""")

                    e_note_ew = st.text_area(f"備註 / 異常原因 / Ghi chú", key=f"note_ew_{row['工單ID']}")
                    
                    if st.button(f"✅ 加工完成並結案 / Hoàn thành", key=f"btn_ew_{row['工單ID']}", type="primary"):
                        end_dt = datetime.combine(end_d, end_t).replace(tzinfo=TAIWAN_TZ)
                        if end_dt < start_dt:
                            st.error("❌ 機台停止時間不可早於開始時間。 / Thời gian máy dừng không được sớm hơn thời gian bắt đầu.")
                        elif row['生產類型'] != "正常生產" and not e_note_ew.strip():
                            st.error("❌ 異常件請務必填寫備註原因！ / Vui lòng điền lý do bất thường!")
                        else:
                            current_db = load_work_orders_raw()
                            mask = (current_db['工單ID'] == row['工單ID']) & (current_db['狀態'] == '進行中')
                            if not current_db[mask].empty:
                                if lunch_running:
                                    segment_h_end = calculate_elapsed_hours(resume_dt, end_dt)
                                else:
                                    segment_h_end = calculate_work_hours_excluding_lunch(resume_dt, end_dt)
                                
                                final_machine_h = round(old_acc + segment_h_end, 2)
                                
                                current_db.loc[mask, '結束時間'] = end_dt.strftime("%Y-%m-%d %H:%M:%S")
                                current_db.loc[mask, '實際工時'] = final_machine_h
                                current_db.loc[mask, '工作區間工時'] = final_machine_h
                                current_db.loc[mask, '累積工作區間工時'] = final_machine_h
                                current_db.loc[mask, '時間差異'] = 0.0
                                current_db.loc[mask, '狀態'] = '已完成'
                                current_db.loc[mask, '備註'] = e_note_ew.strip()
                                
                                save_work_orders(current_db)
                                
                                line_ok = True
                                line_error = ""
                                if row['生產類型'] != "正常生產":
                                    line_ok, line_error = send_line_message(
                                        f"\n⚠️異常結案通知\n"
                                        f"機台：{row.get('機台類型', '')}\n"
                                        f"類型：{row['生產類型']}\n"
                                        f"人員：{row['填寫人']}\n"
                                        f"工單號碼：{row.get('工單號碼', '')}\n"
                                        f"圖號：{row['圖號']}\n"
                                        f"數量：{row.get('工件數量', 1)}\n"
                                        f"機台實際停止時間：{end_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                        f"機台工時：{final_machine_h}h\n"
                                        f"備註：{e_note_ew.strip()}"
                                    )

                                if row['生產類型'] != "正常生產" and not line_ok:
                                    st.warning("⚠️ 工單已結案，但 LINE 通知可能未成功送出。")
                                    st.caption(line_error)

                                st.success(f"✅ 已結案！機台工時：{final_machine_h}h")
                                st.rerun()
                            else: st.error("❌ 此工單可能已被其他人修改，請重新整理後再試。")

        st.divider()
        st.subheader("⏸️ 暫停中的工單查詢 / Tra cứu lệnh đang tạm dừng")
        filter_paused_ew = st.selectbox("查看暫停中工單", ["全部"] + emps, index=0, key="ew_f_pause")
        pause_df_ew = db_df[(db_df['狀態'] == '暫停中') & (db_df['機台類型'].isin(["放電機", "快走絲"]))].copy()
        if filter_paused_ew != "全部": pause_df_ew = pause_df_ew[pause_df_ew['填寫人'] == filter_paused_ew]
            
        if pause_df_ew.empty:
            st.info(f"目前沒有 {filter_paused_ew if filter_paused_ew != '全部' else ''} 暫停中的工單。")
        else:
            for index, row in pause_df_ew.iterrows():
                with st.container(border=True):
                    col_p1, col_p2 = st.columns([3, 1])
                    with col_p1:
                        wo_no_disp = row.get('工單號碼', '')
                        st.write(f"**工單號碼 / Số lệnh sản xuất:** {wo_no_disp if wo_no_disp else '未填寫'}")
                        st.write(f"**圖號:** {row['圖號']} ({row['機台類型']} - {row['生產類型']}) | **人員:** {row['填寫人']}")
                        st.caption(f"系統ID: `{row['工單ID']}` | **機台:** {row.get('機台類型', '')}")
                        st.write(f"**開始於:** {row['開始時間']} | **工件數量:** {row.get('工件數量', 1)}")
                        st.error(f"⏸️ 暫停原因: {PAUSE_REASONS_BILINGUAL.get(row.get('暫停原因', ''), row.get('暫停原因', '未填寫'))} (於 {row.get('暫停時間', '')})")
                        st.info(f"⏱️ 累積機台工作區間工時: {row.get('累積工作區間工時', 0.0)} 小時")
                    with col_p2:
                        st.write("") 
                        if st.button("▶️ 繼續加工 / Tiếp tục", key=f"r_btn_ew_{row['工單ID']}", type="primary", use_container_width=True):
                            current_db = load_work_orders_raw()
                            mask = (current_db['工單ID'] == row['工單ID']) & (current_db['狀態'] == '暫停中')
                            if not current_db[mask].empty:
                                resume_now = datetime.now(TAIWAN_TZ)
                                current_db.loc[mask, '狀態'] = '進行中'
                                current_db.loc[mask, '最後恢復時間'] = resume_now.strftime("%Y-%m-%d %H:%M:%S")
                                save_work_orders(current_db)
                                st.success("▶️ 已恢復加工！"); st.rerun()
                            else: st.error("❌ 狀態錯誤或已被其他人修改，無法繼續。")

# --- 頁籤 3：主管數據看板 (受密碼保護) ---
with tab3:
    if st.session_state["is_admin"]:
        if is_print_mode:
            st.markdown(f"<h1 style='text-align: center;'>工廠生產管理月報表 (V5) - {datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')}</h1>", unsafe_allow_html=True)
            full_df = load_work_orders_cached()
        else:
            st.title("📊 生產數據看板 (V5)")
            full_df = db_df_cached
        
        if full_df.empty:
            st.info("尚未有有效工單資料。請先到「現場報工填寫」新增工單。")
        else:
            if '生產類型' in full_df.columns:
                full_df['生產類型'] = full_df['生產類型'].replace({'NG修復': 'NG重修'})
            
            full_df['開始時間'] = full_df['開始時間'].fillna("").astype(str)
            full_df['開始時間_dt'] = full_df['開始時間'].apply(parse_taiwan_time)
            
            full_df['結束時間'] = full_df['結束時間'].fillna("").astype(str)
            full_df['結束時間_dt'] = full_df['結束時間'].apply(parse_taiwan_time)

            full_df = full_df.dropna(subset=['開始時間_dt'])
            
            if full_df.empty:
                st.info("尚未有有效工單資料。請先到「現場報工填寫」新增工單。")
            else:
                full_df['日期_date'] = full_df['開始時間_dt'].dt.date
                full_df['年月'] = full_df['開始時間_dt'].dt.strftime('%Y-%m')
                full_df['月日'] = full_df['開始時間_dt'].dt.strftime('%m-%d')

                with st.container(border=not is_print_mode):
                    c1, c2, c3, c4, c5, c6 = st.columns([1.5, 2, 2, 2, 2, 2])
                    with c1: v_mode = st.radio("檢視模式", ["整體", "個人"], horizontal=True)
                    with c2: s_emp = st.selectbox("員工篩選", load_employees_cached(), disabled=(v_mode=="整體"))
                    with c3: d_range = st.date_input("日期區間", [full_df['日期_date'].min(), full_df['日期_date'].max()])
                    with c4: s_status = st.selectbox("工單狀態", ["已完成", "進行中", "暫停中", "全部"])
                    with c5: s_type = st.selectbox("生產類型篩選", ["全部"] + PROD_TYPES)
                    with c6: s_machine = st.selectbox("機台類型篩選", ["全部"] + MACHINE_TYPES)
                
                f_df = full_df.copy()
                if v_mode == "個人": f_df = f_df[f_df['填寫人'] == s_emp]
                if isinstance(d_range, (list, tuple)) and len(d_range) == 2:
                    f_df = f_df[(f_df['日期_date'] >= d_range[0]) & (f_df['日期_date'] <= d_range[1])]
                if s_status != "全部": f_df = f_df[f_df['狀態'] == s_status]
                if s_type != "全部": f_df = f_df[f_df['生產類型'] == s_type]
                if s_machine != "全部": f_df = f_df[f_df['機台類型'] == s_machine]

                if f_df.empty:
                    st.warning("目前篩選條件下沒有資料。")
                else:
                    done_df = f_df[f_df['狀態'] == '已完成']
                    ing_df = f_df[f_df['狀態'] == '進行中']
                    pause_df = f_df[f_df['狀態'] == '暫停中']
                    
                    st.markdown("### 📌 關鍵指標彙總")
                    k1, k2, k3, k4, k5, k6 = st.columns(6)
                    k1.metric("總工作區間", f"{round(done_df['工作區間工時'].sum(), 1)} h")
                    k2.metric("總實際加工", f"{round(done_df['實際工時'].sum(), 1)} h")
                    k3.metric("區間未加工時間", f"{round(done_df['時間差異'].sum(), 1)} h", delta_color="inverse")
                    k4.metric("進行中工單", f"{len(ing_df)} 筆")
                    k5.metric("暫停中工單", f"{len(pause_df)} 筆")
                    k6.metric("已完成工單", f"{len(done_df)} 筆")

                    if not pause_df.empty and not is_print_mode:
                        st.warning("⏸️ 目前有暫停中工單，請確認是否為下班未完成、臨時插件、等料或其他原因。")
                        show_cols = ['工單ID', '機台類型', '填寫人', '生產類型', '工單號碼', '圖號', '工件數量', '開始時間', '暫停時間', '暫停原因', '累積工作區間工時']
                        st.dataframe(pause_df[[c for c in show_cols if c in pause_df.columns]], use_container_width=True)

                    with st.container(border=True):
                        st.markdown("### 📈 數據分析戰情室")
                        if s_status in ["進行中", "暫停中"]:
                            st.warning(f"ℹ️ 狀態為「{s_status}」的工單尚未結案，暫不納入統計。")
                        elif done_df.empty:
                            st.info("目前沒有已完成工單，暫無正式圖表。")
                        else:
                            t_level = st.radio("分析層級", ["月統計", "日統計", "工單明細"], horizontal=True)
                            if t_level == "月統計": x_field = "年月"
                            elif t_level == "日統計": x_field = "月日"
                            else: x_field = "工單ID"

                            chart_df = done_df.groupby([x_field, '生產類型']).agg({'實際工時':'sum', '預估工時':'sum'}).reset_index()
                            time_agg = done_df.groupby(x_field).agg({'實際工時':'sum', '預估工時':'sum'}).reset_index()
                            time_agg['偏差'] = time_agg['實際工時'] - time_agg['預估工時']
                            time_agg['標籤'] = time_agg['偏差'].apply(lambda x: f"{'+' if x>0 else ''}{round(x,1)}h")
                            time_agg['偏差顏色'] = time_agg['偏差'].apply(get_diff_color)

                            c_range = ['#1f77b4', '#ff7f0e', '#d62728', '#9467bd'] if not is_print_mode else ['#333', '#666', '#999', '#CCC']

                            bars = alt.Chart(chart_df).mark_bar().encode(
                                x=alt.X(f'{x_field}:N', title='時間維度', axis=alt.Axis(labelAngle=-20)),
                                y=alt.Y('實際工時:Q', title='工時 (h)'),
                                xOffset=alt.XOffset('生產類型:N'),
                                color=alt.Color('生產類型:N', scale=alt.Scale(domain=PROD_TYPES, range=c_range))
                            )
                            
                            line = alt.Chart(time_agg).mark_line(point=True, color='black').encode(
                                x=alt.X(f'{x_field}:N'),
                                y=alt.Y('預估工時:Q')
                            )
                            
                            text = alt.Chart(time_agg).mark_text(dy=-15, fontWeight='bold').encode(
                                x=alt.X(f'{x_field}:N'), y='實際工時:Q', text='標籤:N',
                                color=alt.Color('偏差顏色:N', scale=None)
                            )
                            st.altair_chart((bars + line + text).properties(height=350), use_container_width=True)

                    st.write("### 📅 每月彙總數據表")
                    if not done_df.empty:
                        pivot_df = done_df.pivot_table(index='年月', columns='生產類型', values='實際工時', aggfunc='sum').fillna(0)
                        for col in PROD_TYPES:
                            if col not in pivot_df.columns: pivot_df[col] = 0
                        sum_df = done_df.groupby('年月').agg({'預估工時':'sum', '實際工時':'sum', '工作區間工時':'sum', '時間差異':'sum'})
                        
                        format_dict = {
                            '預估工時': '{:.1f}', '實際工時': '{:.1f}', '工作區間工時': '{:.1f}',
                            '時間差異': '{:.1f}', '正常生產': '{:.1f}', '插件': '{:.1f}',
                            'NG重修': '{:.1f}', '重製': '{:.1f}'
                        }
                        st.dataframe(pd.concat([sum_df, pivot_df], axis=1).style.format(format_dict), use_container_width=True)

                    st.write("### 🔍 詳細生產紀錄清單")
                    show_cols_db = ['工單ID', '日期', '填寫人', '生產類型', '機台類型', '工單號碼', '圖號', '工件數量', '預估工時', '實際工時', '開始時間', '結束時間', '工作區間工時', '累積工作區間工時', '暫停原因', '時間差異', '狀態', '備註']
                    show_df = f_df[[c for c in show_cols_db if c in f_df.columns]]
                    st.dataframe(show_df, use_container_width=True)
                    
                    if not is_print_mode:
                        csv_data = show_df.to_csv(index=False).encode('utf-8-sig')
                        st.download_button(
                            "📥 下載篩選後 CSV 報表", 
                            csv_data, 
                            f"Report_{datetime.now(TAIWAN_TZ).strftime('%Y%m%d')}.csv", 
                            "text/csv"
                        )
    else:
        st.warning("🔒 主管數據看板需輸入主管密碼才能查看。\n請在左側「主管登入」輸入密碼。")

# --- 頁籤 4：主管後台管理 (受密碼保護) ---
if not is_print_mode:
    with tab4:
        if st.session_state["is_admin"]:
            st.title("🛠️ 主管後台管理")
            st.info("此區塊專供主管進行人員與工單資料的強制修正與清理。")
            
            # --- 人員名單管理 ---
            st.subheader("👤 人員名單管理")
            with st.container(border=True):
                admin_emps = load_employees_cached()
                st.write("**目前系統人員名單：**")
                st.dataframe(pd.DataFrame({"員工名字": admin_emps}), use_container_width=True)
                
                st.write("**移除人員操作：**")
                del_emp = st.selectbox("選擇要移除的人員", [""] + admin_emps, key="admin_del_emp")
                del_emp_confirm = st.checkbox("我確認要從人員名單移除此人", key="admin_del_emp_chk")
                
                if st.button("🗑️ 移除人員", type="primary"):
                    if not del_emp:
                        st.error("❌ 請先選擇要移除的人員。")
                    elif not del_emp_confirm:
                        st.error("❌ 請勾選確認核取方塊。")
                    else:
                        current_emps = load_employees_raw()
                        new_emps = [e for e in current_emps if e != del_emp]
                        save_employees_to_sheet(new_emps)
                        st.success(f"✅ 已移除：{del_emp}。 (注意：歷史工單紀錄仍保留)")
                        st.rerun()

            st.divider()

            admin_db = db_df_cached
            
            # --- 工單資料修正 ---
            st.subheader("📝 工單資料修正")
            if admin_db.empty:
                st.info("目前沒有工單可修改。")
            else:
                with st.container(border=True):
                    st.write("**步驟一：篩選要修改的工單**")
                    col_f1, col_f2, col_f3, col_f4, col_f5, col_f6 = st.columns(6)
                    
                    with col_f1:
                        try:
                            admin_db['過濾日期'] = admin_db['開始時間'].apply(parse_taiwan_time).dt.date
                            valid_dates = admin_db['過濾日期'].dropna()
                            if valid_dates.empty:
                                admin_d_range = st.date_input("日期區間 (選填)", [])
                            else:
                                min_d = valid_dates.min()
                                max_d = valid_dates.max()
                                admin_d_range = st.date_input("日期區間 (選填)", value=[min_d, max_d])
                        except:
                            admin_d_range = []
                            
                    with col_f2:
                        all_emp_names = sorted(set(e for e in (load_employees_cached() + admin_db["填寫人"].dropna().astype(str).tolist()) if e.strip()))
                        admin_s_emp = st.selectbox("填寫人", ["全部"] + all_emp_names, key="admin_s_emp")
                        
                    with col_f3: admin_s_status = st.selectbox("工單狀態", ["全部", "進行中", "暫停中", "已完成"], key="admin_s_status")
                    with col_f4: admin_s_type = st.selectbox("生產類型", ["全部"] + PROD_TYPES, key="admin_s_type")
                    with col_f5: admin_s_machine = st.selectbox("機台類型", ["全部"] + MACHINE_TYPES, key="admin_s_machine")
                    with col_f6: admin_s_kw = st.text_input("工單號碼 / 圖號 關鍵字搜尋", key="admin_s_kw").strip()

                    edit_df = admin_db.copy()
                    if isinstance(admin_d_range, (list, tuple)) and len(admin_d_range) == 2:
                        edit_df = edit_df[(edit_df['過濾日期'] >= admin_d_range[0]) & (edit_df['過濾日期'] <= admin_d_range[1])]
                    if admin_s_emp != "全部": edit_df = edit_df[edit_df['填寫人'] == admin_s_emp]
                    if admin_s_status != "全部": edit_df = edit_df[edit_df['狀態'] == admin_s_status]
                    if admin_s_type != "全部": edit_df = edit_df[edit_df['生產類型'] == admin_s_type]
                    if admin_s_machine != "全部": edit_df = edit_df[edit_df['機台類型'] == admin_s_machine]
                    if admin_s_kw: 
                        edit_df = edit_df[
                            edit_df['圖號'].astype(str).str.contains(admin_s_kw, na=False) |
                            edit_df['工單號碼'].astype(str).str.contains(admin_s_kw, na=False)
                        ]
                    
                    st.write(f"篩選結果：共 {len(edit_df)} 筆")
                    st.dataframe(edit_df[[c for c in STANDARD_COLS if c in edit_df.columns]], height=200)

                    st.write("**步驟二：選擇並修改工單**")
                    edit_wo_id = st.selectbox("選擇要修改的 工單ID", [""] + list(edit_df['工單ID']), key="admin_edit_wo")
                    
                    if edit_wo_id:
                        wo_data = admin_db[admin_db['工單ID'] == edit_wo_id].iloc[0]
                        st.info("請直接在下方修改欄位內容：")
                        
                        col_ed1, col_ed2, col_ed3 = st.columns(3)
                        with col_ed1:
                            new_emp = st.text_input("填寫人", value=str(wo_data.get('填寫人', '')))
                            new_type = st.selectbox("生產類型", PROD_TYPES, index=PROD_TYPES.index(wo_data.get('生產類型', '正常生產')) if wo_data.get('生產類型', '正常生產') in PROD_TYPES else 0, key="ed_type")
                            new_machine = st.selectbox("機台類型", MACHINE_TYPES, index=MACHINE_TYPES.index(wo_data.get('機台類型', '磨床')) if wo_data.get('機台類型', '磨床') in MACHINE_TYPES else 0, key="ed_machine")
                            new_work_order_no = st.text_input("工單號碼", value=str(wo_data.get('工單號碼', '')))
                            new_drawing = st.text_input("圖號", value=str(wo_data.get('圖號', '')))
                            new_qty = st.number_input("工件數量", value=int(wo_data.get('工件數量', 1)), step=1, min_value=1)
                            new_status = st.selectbox("狀態", ["進行中", "暫停中", "已完成"], index=["進行中", "暫停中", "已完成"].index(wo_data.get('狀態', '已完成')) if wo_data.get('狀態', '已完成') in ["進行中", "暫停中", "已完成"] else 2, key="ed_status")
                        with col_ed2:
                            new_est = st.number_input("預估工時", value=float(wo_data.get('預估工時', 0.0)), step=0.1)
                            new_act = st.number_input("實際工時", value=float(wo_data.get('實際工時', 0.0)), step=0.1)
                            new_work = st.number_input("工作區間工時", value=float(wo_data.get('工作區間工時', 0.0)), step=0.1)
                            new_acc = st.number_input("累積工作區間工時", value=float(wo_data.get('累積工作區間工時', 0.0)), step=0.1)
                        with col_ed3:
                            new_start = st.text_input("開始時間 (YYYY-MM-DD HH:MM:SS)", value=str(wo_data.get('開始時間', '')))
                            new_end = st.text_input("結束時間 (YYYY-MM-DD HH:MM:SS)", value=str(wo_data.get('結束時間', '')))
                            new_pause_r = st.text_input("暫停原因", value=str(wo_data.get('暫停原因', '')))
                            new_note = st.text_area("備註", value=str(wo_data.get('備註', '')))
                            
                        edit_confirm = st.checkbox("我確認要修改這筆工單資料", key="admin_edit_chk")
                        if st.button("💾 儲存工單修改", type="primary"):
                            if edit_confirm:
                                backup_path = backup_factory_db()
                                
                                latest_db = load_work_orders_raw()
                                mask = latest_db['工單ID'] == edit_wo_id
                                if not latest_db[mask].empty:
                                    latest_db.loc[mask, '填寫人'] = new_emp
                                    latest_db.loc[mask, '生產類型'] = new_type
                                    latest_db.loc[mask, '機台類型'] = new_machine
                                    latest_db.loc[mask, '工單號碼'] = new_work_order_no
                                    latest_db.loc[mask, '圖號'] = new_drawing
                                    latest_db.loc[mask, '工件數量'] = new_qty
                                    latest_db.loc[mask, '預估工時'] = new_est
                                    latest_db.loc[mask, '實際工時'] = new_act
                                    latest_db.loc[mask, '工作區間工時'] = new_work
                                    latest_db.loc[mask, '累積工作區間工時'] = new_acc
                                    latest_db.loc[mask, '開始時間'] = new_start
                                    latest_db.loc[mask, '結束時間'] = new_end
                                    latest_db.loc[mask, '暫停原因'] = new_pause_r
                                    latest_db.loc[mask, '狀態'] = new_status
                                    latest_db.loc[mask, '備註'] = new_note
                                    
                                    latest_db.loc[mask, '時間差異'] = round(new_work - new_act, 2)
                                    
                                    save_work_orders(latest_db)
                                    
                                    st.success(f"✅ 修改成功！已同步至 Google Sheets，並自動備份本機：{backup_path}")
                                    st.rerun()
                                else:
                                    st.error("❌ 此工單可能已被其他人刪除或修改，請重新整理後再試。")
                            else:
                                st.error("❌ 請勾選確認核取方塊。")

            st.divider()

            # --- 工單刪除 ---
            st.subheader("🗑️ 工單刪除")
            if admin_db.empty:
                st.info("目前沒有工單可刪除。")
            else:
                with st.container(border=True):
                    st.warning("⚠️ 警告：刪除工單會永久移除 Google Sheets 資料，請確認已備份。\n請確認選取清單無誤後再刪除。")
                    
                    def format_del_wo(wid):
                        r = admin_db[admin_db['工單ID'] == wid].iloc[0]
                        return f"{wid} | {r['填寫人']} | {r.get('機台類型','')} | {r.get('工單號碼','')} | {r.get('圖號','')} | {r['狀態']} | {r['開始時間']}"
                    
                    del_wo_ids = st.multiselect("選擇要刪除的工單ID，可多選", list(admin_db['工單ID']), format_func=format_del_wo, key="admin_del_wo_multi")
                    
                    if del_wo_ids:
                        st.write("**即將刪除的工單資料：**")
                        delete_preview_df = admin_db[admin_db['工單ID'].isin(del_wo_ids)]
                        st.dataframe(delete_preview_df[[c for c in STANDARD_COLS if c in delete_preview_df.columns]], use_container_width=True)
                        
                        del_wo_confirm = st.checkbox("我確認要永久刪除以上選取工單", key="admin_del_wo_chk")
                        
                        if st.button("☠️ 永久刪除選取工單", type="primary"):
                            if not del_wo_ids:
                                st.error("❌ 請先選擇要刪除的工單。")
                            else:
                                if not del_wo_confirm:
                                    st.error("❌ 請先勾選確認核取方塊。")
                                else:
                                    backup_path = backup_factory_db()
                                    
                                    latest_db = load_work_orders_raw()
                                    latest_db = latest_db[~latest_db['工單ID'].isin(del_wo_ids)]
                                    
                                    save_work_orders(latest_db)
                                    
                                    st.success(f"✅ 已成功刪除 {len(del_wo_ids)} 筆工單！已同步至 Google Sheets，並自動備份本機：{backup_path}")
                                    st.rerun()
        else:
            st.warning("🔒 主管後台管理需輸入主管密碼才能使用。\n請在左側「主管登入」輸入密碼。")
