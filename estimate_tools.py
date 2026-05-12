import streamlit as st
import pandas as pd

def render_estimate_tool(key_prefix="main"):
    st.markdown("### 🧮 預估工時工具")
    st.info("此工具主要供主管估算加工時間，估算結果不會寫入系統，完全不影響大家的正常報工流程。")
    
    # 在頁面內部使用 Radio 切換加工類型，避免側邊欄過於雜亂
    tool_type = st.radio(
        "請選擇要估算的加工類型：",
        ["📐 快走絲", "⚡ 放電", "🟦 磨床"],
        horizontal=True,
        key=f"{key_prefix}_estimate_tool_type"
    )
    
    st.divider()
    
    # ---------------------------------------------------------
    # ⚡ 放電預估工時
    # ---------------------------------------------------------
    if tool_type == "⚡ 放電":
        st.warning("⚡ 放電預估工時功能之後新增。")
        
    # ---------------------------------------------------------
    # 🟦 磨床預估工時 (大台 1070 版 V1.0)
    # ---------------------------------------------------------
    elif tool_type == "🟦 磨床":
        st.subheader("🟦 磨床預估工時 (大台 1070 版 V1.0)")
        st.info("目前以 S型研磨 500×500mm 基準進行時間預估。本版本僅提供時間估算，暫不計算報價。")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("#### 📥 輸入參數")
            g_type = st.selectbox("加工類型", ["平面研磨"], key=f"{key_prefix}_g_type")
            g_subtype = st.selectbox("加工細項", ["S型研磨", "弓形研磨"], key=f"{key_prefix}_g_subtype")
            
            c_len = st.number_input("工件長度 (mm)", min_value=1.0, value=500.0, step=10.0, key=f"{key_prefix}_g_len")
            c_wid = st.number_input("工件寬度 (mm)", min_value=1.0, value=500.0, step=10.0, key=f"{key_prefix}_g_wid")
            w_wid = st.number_input("砂輪寬度 (mm)", min_value=1.0, value=30.0, step=1.0, key=f"{key_prefix}_g_wwid")
            
            st.divider()
            total_amount = st.number_input("總磨削量 (條)", min_value=0.1, value=10.0, step=1.0, key=f"{key_prefix}_g_total")
            
            # 粗磨與精修參數
            c1_r, c1_f = st.columns(2)
            with c1_r:
                rough_feed = st.number_input("粗磨每次進刀量 (條)", min_value=0.1, value=2.0, step=0.5, key=f"{key_prefix}_g_rfeed")
                rough_speed = st.number_input("粗磨速率", min_value=1.0, value=300.0, step=10.0, key=f"{key_prefix}_g_rspeed")
            with c1_f:
                fin_allowance = st.number_input("精修預留量 (條)", min_value=0.0, value=2.0, step=0.5, key=f"{key_prefix}_g_fallo")
                fin_feed = st.number_input("精修每次進刀量 (條)", min_value=0.1, value=1.0, step=0.5, key=f"{key_prefix}_g_ffeed")
                fin_speed = st.number_input("精修速率", min_value=1.0, value=200.0, step=10.0, key=f"{key_prefix}_g_fspeed")
            
            spark_out_passes = st.number_input("空跑次數", min_value=0, value=0, step=1, key=f"{key_prefix}_g_spark")

        with col2:
            st.markdown("#### ⚙️ 系統基準設定")
            base_time = 3.18    # 基準單次時間 (分)
            base_speed = 200.0  # 基準速率
            st.caption(f"目前基準：單次 {base_time} 分鐘 ｜ 基準速率：{int(base_speed)}")
            
            # 防呆機制：確保精修預留量不超過總磨削量
            if fin_allowance > total_amount:
                st.error("⚠️ 精修預留量不可大於總磨削量，請重新確認。")
            else:
                # 核心計算邏輯
                rough_amount = max(0.0, total_amount - fin_allowance)
                rough_passes = rough_amount / rough_feed if rough_feed > 0 else 0
                fin_passes = fin_allowance / fin_feed if fin_feed > 0 else 0
                
                # 單次時間反比運算
                rough_pass_time = (base_time * base_speed) / rough_speed if rough_speed > 0 else 0
                fin_pass_time = (base_time * base_speed) / fin_speed if fin_speed > 0 else 0
                spark_pass_time = fin_pass_time  # 空跑速率預設等於精修速率
                
                # 各階段總時間
                rough_time = rough_passes * rough_pass_time
                fin_time = fin_passes * fin_pass_time
                spark_time = spark_out_passes * spark_pass_time
                
                total_time_min = rough_time + fin_time + spark_time
                total_time_hr = total_time_min / 60
                
                # 換算分秒
                total_m = int(total_time_min)
                total_s = int(round((total_time_min - total_m) * 60))
                if total_s == 60:
                    total_m += 1
                    total_s = 0

                st.markdown("#### 📌 預估結果")
                st.success(f"""
                ⏱️ 預估加工時間：約 **{total_m}分 {total_s}秒** ({total_time_min:.2f} 分鐘)  
                ⏱️ 預估工時：約 **{total_time_hr:.2f}** 小時
                """)
                
                st.markdown("#### 📊 計算明細")
                res_data = [
                    {"階段": "粗磨", "磨削量(條)": f"{rough_amount:.1f}", "次數": f"{rough_passes:.1f}", "單次時間(分)": f"{rough_pass_time:.2f}", "總時間(分)": f"{rough_time:.2f}"},
                    {"階段": "精修", "磨削量(條)": f"{fin_allowance:.1f}", "次數": f"{fin_passes:.1f}", "單次時間(分)": f"{fin_pass_time:.2f}", "總時間(分)": f"{fin_time:.2f}"},
                    {"階段": "空跑", "磨削量(條)": "0.0", "次數": f"{spark_out_passes:.1f}", "單次時間(分)": f"{spark_pass_time:.2f}", "總時間(分)": f"{spark_time:.2f}"}
                ]
                st.table(pd.DataFrame(res_data))
                
                st.markdown("#### 📋 複製預估結果")
                copy_text = f"""磨床預估工時 (大台 1070)
加工類型：{g_type} ({g_subtype})
工件尺寸：{c_len} x {c_wid} mm
總磨削量：{total_amount} 條
粗磨：{rough_amount} 條 (進刀 {rough_feed} 條, 速率 {rough_speed})
精修：{fin_allowance} 條 (進刀 {fin_feed} 條, 速率 {fin_speed})
空跑次數：{spark_out_passes} 次
預估加工時間：約 {total_m}分 {total_s}秒
預估工時：約 {total_time_hr:.2f} 小時"""
                st.code(copy_text, language="text")

    # ---------------------------------------------------------
    # 📐 快走絲預估工時 (V1.1 完整版)
    # ---------------------------------------------------------
    elif tool_type == "📐 快走絲":
        # 定義加上 prefix 的 session_state key
        calc_key = f"{key_prefix}_calc_cut_length"
        cut_key = f"{key_prefix}_wc_cut_length"
        
        # 初始化切割長度的 session_state
        if calc_key not in st.session_state:
            st.session_state[calc_key] = 350.0
        if cut_key not in st.session_state:
            st.session_state[cut_key] = 350.0
            
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("#### 📥 輸入參數")
            thickness = st.number_input("厚度(mm)", min_value=0.1, value=23.0, step=1.0, key=f"{key_prefix}_wc_thickness")
            
            # 使用帶有 prefix 的 session_state key 連動切割長度
            cut_length = st.number_input(
                "切割長度(mm)", 
                min_value=0.0, 
                step=1.0, 
                key=cut_key
            )
            
            setup_diff = st.selectbox("架機難度", ["簡單", "正常", "複雜"], index=1, key=f"{key_prefix}_wc_setup")
            change_wire = st.selectbox("是否換線", ["是", "否"], index=0, key=f"{key_prefix}_wc_wire")
            slice_count = st.number_input("切片刀數", min_value=1, value=16, step=1, key=f"{key_prefix}_wc_slice")
            
        # 參數與時間計算邏輯
        setup_time = 30 if setup_diff == "簡單" else (45 if setup_diff == "正常" else 60)
        wire_time = 30 if change_wire == "是" else 0
        
        tiers = [
            {"name": "超高厚度 100mm以上", "param": 60},
            {"name": "高厚度 51mm~99mm", "param": 70},
            {"name": "一般厚度 15mm~50mm", "param": 85},
            {"name": "薄件 15mm以下", "param": 100}
        ]
        
        results = []
        selected_tier = None
        
        for t in tiers:
            speed = t["param"] / thickness
            cut_t = ((cut_length + 10) / speed) * slice_count
            accum_t = cut_t + setup_time + wire_time
            cut_hr = accum_t / 60
            
            # 判斷實際採用級距
            is_selected = False
            if thickness >= 100 and t["param"] == 60:
                is_selected = True
            elif 51 <= thickness < 100 and t["param"] == 70:
                is_selected = True
            elif 15 <= thickness < 51 and t["param"] == 85:
                is_selected = True
            elif thickness < 15 and t["param"] == 100:
                is_selected = True
                
            if is_selected:
                selected_tier = {
                    "name": t["name"],
                    "speed": speed,
                    "cut_t": cut_t,
                    "accum_t": accum_t,
                    "cut_hr": cut_hr
                }
                
            results.append({
                "厚度級距": t["name"],
                "參數": t["param"],
                "加工速度(mm/min)": f"{speed:.2f}",
                "切割時間(min)": f"{cut_t:.1f}",
                "架機時間(min)": setup_time,
                "換線時間(min)": wire_time,
                "累積時間(min)": f"{accum_t:.1f}",
                "切割時間(hr)": f"{cut_hr:.1f}",
                "是否採用": "✅ 實際採用" if is_selected else ""
            })
            
        with col2:
            st.markdown("#### 📌 實際採用結果")
            if selected_tier:
                st.success(f"""
                **實際採用級距**：{selected_tier['name']}  
                ⏱️ **預估工時**：約 **{selected_tier['cut_hr']:.1f}** 小時  
                🧮 加工速度：{selected_tier['speed']:.2f} mm/min  
                📏 切割時間：{selected_tier['cut_t']:.1f} 分鐘
                """)
            
            st.markdown("#### ⭕ 圓線長換算")
            with st.container(border=True):
                circle_count = st.number_input("圓數量", min_value=1, value=1, step=1, key=f"{key_prefix}_wc_circ_cnt")
                diameter = st.number_input("直徑(mm)", min_value=0.0, value=43.3, step=0.1, key=f"{key_prefix}_wc_diam")
                circ_len = diameter * 3.14 * circle_count
                st.write(f"線長：約 **{circ_len:.3f}** mm")
                
                if st.button("📋 使用此線長作為切割長度", key=f"{key_prefix}_btn_use_circ_len"):
                    st.session_state[calc_key] = circ_len
                    st.session_state[cut_key] = circ_len
                    st.rerun()
                    
            st.markdown("#### 📋 複製預估結果")
            if selected_tier:
                copy_text = f"""快走絲預估工時
厚度：{thickness} mm
切割長度：{cut_length} mm
切片刀數：{slice_count} 刀
架機難度：{setup_diff}
是否換線：{change_wire}
採用級距：{selected_tier['name']}
加工速度：{selected_tier['speed']:.2f} mm/min
預估工時：約 {selected_tier['cut_hr']:.1f} 小時"""
                st.code(copy_text, language="text")

        st.markdown("#### 📊 各級距試算對照表")
        st.dataframe(pd.DataFrame(results), use_container_width=True)
