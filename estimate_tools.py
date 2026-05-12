import streamlit as st
import pandas as pd

def render_estimate_tool(key_prefix="main"):
    st.markdown("### 🧮 預估工時工具")
    st.info("此工具主要供主管估算快走絲加工時間，估算結果不會寫入系統，完全不影響大家的正常報工流程。")
    
    # 選擇加工類型
    tool_type = st.radio(
        "請選擇要估算的加工類型：",
        ["📐 快走絲", "⚡ 放電", "🟦 磨床"],
        horizontal=True,
        key=f"{key_prefix}_estimate_tool_type"
    )
    
    st.divider()
    
    if tool_type == "⚡ 放電":
        st.warning("⚡ 放電預估工時功能之後新增。")
    
    elif tool_type == "🟦 磨床":
        st.warning("🟦 磨床預估工時功能之後新增。")
        
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
