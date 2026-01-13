import streamlit as st
import asyncio
import aiosqlite
import pandas as pd
from datetime import date, timedelta, datetime, time as dtime
from pathlib import Path

from db import init_db, DB_PATH, vacuum_db
from queries import (
    get_guild_rows,
    get_character_list,
    replace_guild_list,
    get_top20_by_level,
    get_top20_by_best_power,
    get_best_power_per_character_in_range,
    get_level_and_class_gender_in_range,
    get_liberation_count,
    count_characters_by_name_keywords,
    get_character_full_profile_by_role_id,
    count_paid_sword_holders,
    count_double_legendary_by_part,
    count_potential_and_rules,
    count_item_holders,
    count_combo_items,
    POTENTIAL_WHITELIST,
)
from services import (
    expand_guilds_to_character_list,
    update_characters,
    check_character_completeness,
    optimize_data,
    validate_and_purge_character_list,   # ★Step 2.5 健檢用
    sync_blacklist_incremental,          # ★Step 2.6 增量同步用
    get_blacklist_last_sync_date,        # ★顯示上次同步日期
    purge_or_skip_blacklisted,           # ★Step 2.6 套用 DB
)

# ============================================================
# UI-only：裝備 Icon Tooltip 樣式
# ============================================================
st.markdown("""
<style>
.tooltip {
  position: relative;
  display: inline-block;
  margin: 6px;
}
.tooltip img {
  border-radius: 6px;
  border: 1px solid #444;
  background-color: #000;
}
.tooltip .tooltiptext {
  visibility: hidden;
  width: 260px;
  background-color: #111;
  color: #fff;
  text-align: left;
  border-radius: 6px;
  padding: 8px;
  position: absolute;
  z-index: 100;
  top: 110%;
  left: 50%;
  transform: translateX(-50%);
  font-size: 12px;
  box-shadow: 0 0 10px #000;
  line-height: 1.4;
}
.tooltip:hover .tooltiptext {
  visibility: visible;
}
</style>
""", unsafe_allow_html=True)


# ============================
# UI-only：潛能詞條類型顯示用
# （queries.py 已改為 DB-only，不再輸出此常數）
# ============================
POT_TYPE_LABEL = {
    "BOSS_DMG": "Boss傷害",
    "ATT_PCT": "物理攻擊力(%)",
    "MATK_PCT": "魔法攻擊力(%)",
}



# -----------------------------
# 初始化：只在 process 啟動後跑一次
# -----------------------------
@st.cache_resource
def init_once():
    asyncio.run(init_db())
    return True


# -----------------------------
# UI helpers
# -----------------------------
def parse_bulk_lines(text: str):
    return [ln.strip() for ln in (text or "").splitlines() if ln.strip()]

def normalize_class(character_class: str, character_date_create: str) -> str:
    c = (character_class or "").strip()
    if c and c != "?":
        return c

    # 暫時規則：? 且建立日 >= 2025-12-03 -> 視為「蓮」
    try:
        if character_date_create:
            d = datetime.fromisoformat(character_date_create.replace("Z", "+00:00")).date()
            if d >= date(2025, 12, 3):
                return "蓮"
    except Exception:
        pass
    return "?"

def render_pie_or_bar(title: str, labels: list[str], counts: list[int]):
    total = sum(counts) if counts else 0
    if total <= 0:
        st.info(f"{title}：目前沒有可統計的資料。")
        return

    df = pd.DataFrame({"分類": labels, "人數": counts})
    df["比例(%)"] = (df["人數"] / total * 100).round(2)

    colA, colB = st.columns([1, 1])
    with colA:
        st.subheader(title)
        st.dataframe(df, use_container_width=True)

    with colB:
        chart_df = df.set_index("分類")[["人數"]]
        st.bar_chart(chart_df, use_container_width=True)

def show_top10_table(title: str, rows: list[dict], mode: str):
    st.subheader(title)
    if not rows:
        st.info("目前沒有資料可顯示（可能尚未抓取角色資料或指定期間內無 stat）。")
        return

    show = []
    for r in rows:
        show.append({
            "角色": r.get("character_name"),
            "等級": r.get("character_level"),
            "職業": normalize_class(r.get("character_class"), r.get("character_date_create")),
            "戰鬥力(期間最高)" if mode == "power" else "建立日": r.get("best_power") if mode == "power" else r.get("character_date_create"),
            "頭像": r.get("character_image"),
        })

    df = pd.DataFrame(show)
    st.dataframe(df.drop(columns=["頭像"]), use_container_width=True)

    with st.expander("顯示頭像"):
        for r in rows:
            cols = st.columns([1, 5])
            with cols[0]:
                if r.get("character_image"):
                    st.image(r["character_image"], width=64)
            with cols[1]:
                if mode == "power":
                    st.write(f"{r.get('character_name')}｜等級 {r.get('character_level')}｜職業 {normalize_class(r.get('character_class'), r.get('character_date_create'))}｜戰鬥力 {r.get('best_power')}")
                else:
                    st.write(f"{r.get('character_name')}｜等級 {r.get('character_level')}｜職業 {normalize_class(r.get('character_class'), r.get('character_date_create'))}")

def render_character_full_profile(profile: dict):
    """
    Streamlit 顯示用：basic / power / latest_stats / latest_equip
    """
    basic = profile.get("basic") or {}
    power = profile.get("power_summary")
    stats = profile.get("latest_stats") or {}
    equip = profile.get("latest_equip") or {}

    st.subheader("角色基本資料（DB）")
    c1, c2 = st.columns([1, 2])
    with c1:
        if basic.get("character_image"):
            st.image(basic["character_image"], width=120)
    with c2:
        st.write(f"角色：**{basic.get('character_name')}**｜世界：**{basic.get('world_name')}**")
        st.write(f"ocid：`{basic.get('ocid')}`")
        st.write(f"等級：**{basic.get('character_level')}**｜職業：**{normalize_class(basic.get('character_class'), basic.get('character_date_create'))}**")
        st.write(f"公會：{basic.get('character_guild_name') or '-'}")
        st.write(f"更新時間(updated_at)：{basic.get('updated_at') or '-'}")

    st.subheader("戰鬥力摘要（DB）")
    if power:
        st.json(power)
    else:
        st.info("character_power_summary 目前沒有資料（可能尚未抓取或尚未寫入摘要）。")

    st.subheader("最新 Stat（DB）")
    st.write(f"stat_date：**{stats.get('stat_date') or '-'}**")
    stat_rows = stats.get("stats") or []
    if stat_rows:
        st.dataframe(pd.DataFrame(stat_rows), use_container_width=True, height=320)
    else:
        st.info("此角色目前沒有 stat 資料。")

    st.subheader("最新 裝備 raw_json（DB）")
    st.write(f"equip_date：**{equip.get('equip_date') or '-'}**｜fetched_at：{equip.get('fetched_at') or '-'}")

    raw = equip.get("raw")
    if not raw:
        st.info("此角色目前沒有裝備 raw_json（或解析失敗）。")
        return

    items = raw.get("item_equipment") or []
    weapon = None
    for it in items:
        if it.get("item_equipment_slot") == "武器":
            weapon = it
            break

    with st.expander("武器（slot=武器）摘要"):
        if weapon:
            st.json({
                "item_name": weapon.get("item_name"),
                "item_equipment_part": weapon.get("item_equipment_part"),
                "item_equipment_slot": weapon.get("item_equipment_slot"),
                "item_icon": weapon.get("item_icon"),
                "potential_option_grade": weapon.get("potential_option_grade"),
                "potential_option_1": weapon.get("potential_option_1"),
                "potential_option_2": weapon.get("potential_option_2"),
                "potential_option_3": weapon.get("potential_option_3"),
            })
        else:
            st.info("raw_json 中未找到 slot=武器 的物品。")

    with st.expander("完整 raw_json（注意：內容可能很大）"):
        st.json(raw)


# -----------------------------
# blacklist：06:00 規則（前一天資料隔天 06:00 才完整）
# -----------------------------
def latest_available_blacklist_date() -> date:
    now = datetime.now()
    cutoff = datetime.combine(now.date(), dtime(6, 0, 0))
    if now < cutoff:
        return now.date() - timedelta(days=2)
    return now.date() - timedelta(days=1)

def parse_ymd(s: str) -> date | None:
    # 允許 "YYYY/MM/DD" 或 "YYYY-MM-DD"
    try:
        if not s:
            return None
        s = s.strip()
        if "/" in s:
            return datetime.strptime(s, "%Y/%m/%d").date()
        return date.fromisoformat(s)
    except Exception:
        return None

def is_permanent_unblock_date(unblocked: str) -> bool:
    # 官網永久鎖定顯示 2079/..（你提供的圖）
    d = parse_ymd(unblocked)
    if not d:
        return False
    return d.year >= 2079

def is_7day_lock(blocked: str, unblocked: str) -> bool:
    bd = parse_ymd(blocked)
    ud = parse_ymd(unblocked)
    if not bd or not ud:
        return False
    # 常見暫時停權 = 7 天（含 7）
    return (ud - bd).days <= 7 and (ud - bd).days >= 1 and ud.year < 2079


# -----------------------------
# App start
# -----------------------------
st.set_page_config(page_title="Maple Guild Tool", layout="wide")
st.title("MapleStory 公會 / 角色資料管理")

def load_api_key_from_file() -> str:
    p = Path(__file__).resolve().parent / "apikey.txt"
    if not p.exists():
        return ""
    try:
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


init_once()

with st.sidebar:
    st.header("全域設定")

    default_api_key = load_api_key_from_file()
    api_key = st.text_input(
        "Nexon Open API Key",
        value=default_api_key,
        type="password",
        help="可從同資料夾的 apikey.txt 自動讀取；也可在此手動覆蓋。"
    )

    world = "挑戰者"
    st.info(f"世界名稱：{world}（固定）")

    base_date = st.date_input("stat 抓取基準日期", value=date.today())
    days_back = st.number_input("stat 回溯天數", min_value=1, max_value=5, value=4)

    st.divider()
    st.subheader("裝備抓取設定（可選）")

    equip_enabled = st.checkbox("抓取裝備", value=True)

    equip_date_mode_ui = st.radio(
        "裝備日期模式",
        ["抓取當天（不填 date）", "指定日期（填 date）", "指定日期並回溯 N 天"],
        index=0,
        horizontal=False,
        help="『抓取當天』會呼叫 API 時不帶 date 參數；其餘模式則以你選的日期組合 date 清單。"
    )

    equip_base_date = base_date
    equip_days_back = 0

    if equip_date_mode_ui != "抓取當天（不填 date）":
        equip_base_date = st.date_input("裝備基準日期", value=base_date, key="equip_base_date")

    if equip_date_mode_ui == "指定日期並回溯 N 天":
        equip_days_back = st.number_input("裝備回溯天數", min_value=0, max_value=30, value=0, key="equip_days_back")

    fetch_mode_ui = st.selectbox(
        "抓取模式",
        ["有資料就跳過（推薦）", "完整抓取（可恢復，不重抓已完成的日資料）"],
        index=0
    )
    fetch_mode = "skip_existing" if fetch_mode_ui.startswith("有資料") else "full_fetch"

    refresh_days = st.number_input("資料過期週期（天）", min_value=1, max_value=60, value=7)
    only_expired = st.checkbox("只更新過期角色（未過期一律略過）", value=False)
    force_refresh_all = st.checkbox("強制全量刷新（忽略跳過與日資料存在檢查）", value=False)

    st.divider()
    st.subheader("統計設定")

    ignore_stat_range = st.checkbox(
        "統計時忽略時間區間（推薦：已做資料優化/只保留最佳值時）",
        value=True,
        help="勾選後，統計查詢會以全期間(0001-01-01~9999-12-31)計算，避免優化後資料因日期不在區間而被篩掉。"
    )


if not api_key:
    st.warning("請先在左側輸入 Nexon API Key")
    st.stop()


tabs = st.tabs([
    "公會清單",
    "角色清單/抓取",
    "資料完整性檢查",
    "數據統計專區",
    "資料優化",
])


# ============================================================
# 裝備 Icon + Tooltip（DB best_equipment_json）
# ============================================================
def render_item_icon(it: dict) -> str:
    name = it.get("item_name", "未知裝備")
    icon = it.get("item_icon", "")
    star = it.get("starforce", "0")

    # ✅ 星力圖示化（在 return 之前先算好）
    try:
        stars = int(star)
    except Exception:
        stars = 0
    stars = max(0, min(25, stars))
    star_html = "★" * stars + "☆" * (25 - stars)

    def opt(label, key):
        v = it.get("item_total_option", {}).get(key)
        return f"{label}: +{v}" if v and str(v) != "0" else ""

    stats = "<br>".join(filter(None, [
        opt("STR", "str"),
        opt("DEX", "dex"),
        opt("INT", "int"),
        opt("LUK", "luk"),
        opt("物理攻擊力", "attack_power"),
        opt("魔法攻擊力", "magic_power"),
        opt("全屬性", "all_stat"),
    ]))

    pot_main = "<br>".join(filter(None, [
        it.get("potential_option_1"),
        it.get("potential_option_2"),
        it.get("potential_option_3"),
    ]))

    pot_add = "<br>".join(filter(None, [
        it.get("additional_potential_option_1"),
        it.get("additional_potential_option_2"),
        it.get("additional_potential_option_3"),
    ]))

    return f"""
    <div class="tooltip">
        <img src="{icon}" width="42">
        <div class="tooltiptext">
            <b>{name}</b><br>
            <span style="color:gold;font-size:13px">{star_html}</span><br><br>
            {stats or "（無主要數值）"}<br><br>
            <u>主潛能</u><br>{pot_main or "無"}<br><br>
            <u>附加潛能</u><br>{pot_add or "無"}
        </div>
    </div>
    """

# ============================================================
# 裝備 Slot 排列定義
# ============================================================

EQUIP_LAYOUT = {
    "row1": ["戒指", "戒指", "戒指", "戒指", "腰帶", "口袋道具"],
    "row2": ["眼飾", "臉飾", "耳環", "墜飾", "墜飾"],
    "center": ["武器", "輔助武器", "徽章"],
    "row3": ["帽子", "上衣", "下衣", "肩膀", "機器人"],
    "row4": ["披風", "手套", "鞋子", "勳章", "機器人心臟", "胸章"],
    "totem": ["圖騰", "圖騰", "圖騰"],
}

def build_slot_map(items: list[dict]) -> dict:
    """
    回傳：
    {
      "戒指": [it1, it2, it3, it4],
      "圖騰": [itA, itB, itC],
      "武器": it,
      ...
    }
    """
    slot_map: dict[str, list | dict] = {}

    for it in items:
        raw_slot = it.get("item_equipment_slot")
        slot = SLOT_ALIAS.get(raw_slot, raw_slot)

        if not slot:
            continue

        # 多格 slot
        if slot in ("戒指", "圖騰"):
            slot_map.setdefault(slot, []).append(it)
        else:
            slot_map.setdefault(slot, it)

    return slot_map

def render_placeholder(label: str, red: bool = False) -> str:
    bg = "#7a1e1e" if red else "#222"
    return f"""
    <div style="
        width:42px;height:42px;
        border-radius:6px;
        background:{bg};
        border:1px solid #444;
        display:flex;
        align-items:center;
        justify-content:center;
        color:#bbb;
        font-size:11px;
        margin:6px;
    ">{label}</div>
    """

# ============================================================
# 裝備 slot 正規化（API → UI 顯示用）
# ============================================================

SLOT_ALIAS = {
    # 戒指
    "戒指1": "戒指",
    "戒指2": "戒指",
    "戒指3": "戒指",
    "戒指4": "戒指",

    # 墜飾
    "墜飾": "墜飾",
    "墜飾2": "墜飾",

    # 下衣（套服判斷）
    "褲": "下衣",
    "裙": "下衣",

    # 肩膀
    "肩膀裝飾": "肩膀",

    # 心臟
    "機器心臟": "機器人心臟",

    # 圖騰（三格）
    "馴服的怪物": "圖騰",
    "怪物裝備": "圖騰",
    "馬鞍": "圖騰",
}



# =====================================================
# Tab 1：公會清單
# =====================================================
with tabs[0]:
    st.header("公會清單管理")

    guild_rows = asyncio.run(get_guild_rows())
    st.write(f"目前已加入公會數量：**{len(guild_rows)}**")
    if guild_rows:
        st.dataframe(pd.DataFrame(guild_rows), use_container_width=True)

    st.subheader("批次加入公會（每行一個）")
    bulk_text = st.text_area("公會名稱清單", height=160, placeholder="紅蓮兔兔之家\n另一個公會\n...")

    if st.button("加入公會（批次）"):
        names = parse_bulk_lines(bulk_text)
        if not names:
            st.warning("沒有可加入的公會名稱（可能都是空白行）")
        else:
            async def add_guilds_batch():
                inserted = 0
                duplicates = 0
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("PRAGMA busy_timeout=10000;")
                    for gname in names:
                        cur = await db.execute("""
                            INSERT OR IGNORE INTO guild_list (guild_name, world_name)
                            VALUES (?, ?)
                        """, (gname, world))
                        if cur.rowcount == 1:
                            inserted += 1
                        else:
                            duplicates += 1
                    await db.commit()
                return inserted, duplicates

            inserted, duplicates = asyncio.run(add_guilds_batch())
            st.success(f"批次加入完成：新增 {inserted} 筆、重複忽略 {duplicates} 筆（世界：{world}）")
            st.rerun()

    st.divider()
    st.subheader("直接編輯公會清單（修正 OCR 錯字）")

    if "guild_editor_df" not in st.session_state:
        st.session_state.guild_editor_df = pd.DataFrame(
            guild_rows if guild_rows else [{"guild_name": "", "world_name": world}]
        )

    edited_df = st.data_editor(
        st.session_state.guild_editor_df,
        num_rows="dynamic",
        use_container_width=True
    )

    c1, c2 = st.columns([1, 2])
    with c1:
        if st.button("儲存公會清單變更"):
            rows = edited_df.to_dict(orient="records")
            result = asyncio.run(replace_guild_list(rows))
            st.success(f"已儲存。保留 {result['kept']} 筆；丟棄 {result['dropped']} 筆（空白/重複）")
            st.session_state.guild_editor_df = pd.DataFrame(
                asyncio.run(get_guild_rows()) or [{"guild_name": "", "world_name": world}]
            )
            st.rerun()

    with c2:
        if st.button("放棄未儲存變更（重新載入 DB）"):
            st.session_state.guild_editor_df = pd.DataFrame(
                asyncio.run(get_guild_rows()) or [{"guild_name": "", "world_name": world}]
            )
            st.rerun()


# =====================================================
# Tab 2：角色清單/抓取
# =====================================================
with tabs[1]:
    st.header("角色清單 / 抓取流程")

    st.subheader("Step 1：從公會清單展開角色名單")
    guild_rows = asyncio.run(get_guild_rows())
    st.write(f"目前公會數：**{len(guild_rows)}**")

    if st.button("從公會展開角色清單（寫入 character_list，並自動去重）"):
        prog = st.progress(0)
        status = st.empty()

        guild_names = [g["guild_name"] for g in guild_rows if g["world_name"] == world]

        def cb(i, gname):
            if guild_names:
                prog.progress(min(1.0, (i + 1) / len(guild_names)))
            status.write(f"正在處理公會：{gname}")

        result = asyncio.run(expand_guilds_to_character_list(api_key, guild_names, world, cb))
        st.success(f"完成：新增角色 {result['added']} 筆")

        if result["errors"]:
            st.error("以下公會無法取得成員清單（可能 OCR 錯字）：")
            st.dataframe(pd.DataFrame(result["errors"]), use_container_width=True)

    st.divider()
    st.subheader("Step 2：查看目前已加入的角色清單")
    chars = asyncio.run(get_character_list(world))
    st.write(f"目前角色數：**{len(chars)}**")
    if chars:
        st.dataframe(pd.DataFrame({"character_name": chars}), use_container_width=True, height=260)

    # -------------------------
    # Step 2.5：角色清單健檢（改名/停權/不存在）
    # -------------------------
    st.divider()
    st.subheader("Step 2.5：角色清單健檢（抓不到 ocid/basic / access_flag=false / 改名自動修正）")

    colA, colB, colC = st.columns([1, 1, 2])
    with colA:
        dry_run = st.checkbox("只預演（dry_run）", value=True)
    with colB:
        treat_access_flag_false_as_invalid = st.checkbox("access_flag=false 視為失效", value=True)
    with colC:
        st.caption("建議先用『只預演』確認名單，再取消勾選實際剔除。")

    if st.button("開始健檢（Step 2.5）"):
        prog = st.progress(0)
        status = st.empty()

        def cb(i, total, name):
            if total > 0:
                prog.progress(min(1.0, (i + 1) / total))
            status.write(f"健檢中：{name}（{i+1}/{total}）")

        out = asyncio.run(validate_and_purge_character_list(
            api_key=api_key,
            world=world,
            dry_run=dry_run,
            treat_access_flag_false_as_invalid=treat_access_flag_false_as_invalid,
            auto_fix_rename=True,
            purge_invalid_from_db=True,
            delete_equip_history=True,
            progress_cb=cb
        ))

        st.success(f"健檢完成｜總數 {out['total']}｜有效 {out['ok']}｜失效 {out['invalid_count']}｜改名 {out['renamed_count']}")
        if out["invalid_count"] > 0:
            st.error("失效名單（前 5000 筆）：")
            st.dataframe(pd.DataFrame(out["invalid"]), use_container_width=True, height=260)
        if out["renamed_count"] > 0:
            st.info("改名修正（前 5000 筆）：")
            st.dataframe(pd.DataFrame(out["renamed"]), use_container_width=True, height=260)

    # -------------------------
    # Step 2.6：停權名單（表格預覽 + 7日/永久勾選 + 套用）
    # -------------------------
    st.divider()
    st.subheader("Step 2.6：官網停權名單（可預覽表格、勾選 7 日/永久）")

    # 上次同步資訊
    last_sync = asyncio.run(get_blacklist_last_sync_date(world))
    latest_date = latest_available_blacklist_date()
    default_start = date(2025, 12, 3) if not last_sync else (date.fromisoformat(last_sync) + timedelta(days=1))

    st.caption(f"上次同步到：{last_sync or '（尚未同步）'}｜本次可抓到的最新日期（依 06:00 規則）：{latest_date.isoformat()}")

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        bl_start = st.date_input("停權名單起日", value=default_start, key="bl_start")
    with col2:
        bl_end = st.date_input("停權名單迄日", value=latest_date, key="bl_end")
    with col3:
        lock_types = st.multiselect("篩選類型", options=["7日", "永久(2079)"], default=["永久(2079)"])

    mode = st.radio("套用模式", options=["跳過（從 character_list 移除，但不刪歷史）", "刪除（從 character_list 移除，並刪歷史資料）"], horizontal=True)
    mode_val = "skip" if mode.startswith("跳過") else "purge"

    # 先準備 session_state 暫存表格
    if "bl_preview_df" not in st.session_state:
        st.session_state.bl_preview_df = None

    cA, cB = st.columns([1, 1])

    with cA:
        if st.button("預覽名單（顯示表格，不動 DB）"):
            # 直接用 blacklist_fetcher 抓資料以顯示表格（避免你看不到名單）
            try:
                from blacklist_fetcher import BlacklistClient
            except Exception as e:
                st.error(f"無法載入 blacklist_fetcher.py：{repr(e)}")
                st.stop()

            if bl_start > bl_end:
                st.error("起日不可晚於迄日")
                st.stop()

            if bl_end > latest_date:
                st.warning(f"你選的迄日 {bl_end} 超過可取得的最新日期 {latest_date}，將自動改成 {latest_date}")
                bl_end = latest_date

            total_days = (bl_end - bl_start).days + 1
            prog = st.progress(0)
            status = st.empty()

            rows_all = []
            cli = BlacklistClient(server_name="挑戰者")
            csrf, server_id = cli.init()

            cur = bl_start
            done = 0
            while cur <= bl_end:
                done += 1
                dstr = cur.strftime("%Y/%m/%d")
                status.write(f"抓取 {dstr}（{done}/{total_days}）")
                prog.progress(min(1.0, done / total_days))

                day_rows = cli.fetch_all_for_date(dstr, sleep_sec=0.25)
                # day_rows 預期為 list[dict]，dict 至少有 characterName/reason/blockDate/unBlockedDate
                for r in (day_rows or []):
                    rows_all.append({
                        "character_name": r.get("characterName") or r.get("character_name") or "",
                        "reason": r.get("reason") or "",
                        "blockDate": r.get("blockDate") or r.get("block_date") or dstr,
                        "unBlockedDate": r.get("unBlockedDate") or r.get("unblocked_date") or "",
                    })
                cur += timedelta(days=1)

            df = pd.DataFrame(rows_all)
            df = df[df["character_name"].astype(str).str.strip() != ""].copy()

            # 類型分類
            def _classify(row):
                bd = str(row.get("blockDate") or "")
                ud = str(row.get("unBlockedDate") or "")
                if is_permanent_unblock_date(ud):
                    return "永久(2079)"
                if is_7day_lock(bd, ud):
                    return "7日"
                return "其他/未知"

            if not df.empty:
                df["lock_type"] = df.apply(_classify, axis=1)
            else:
                df["lock_type"] = []

            # 依勾選篩選
            want = set(lock_types or [])
            if want:
                df = df[df["lock_type"].isin(want)].copy()

            df = df.sort_values(["lock_type", "blockDate", "character_name"], ascending=[False, True, True])
            st.session_state.bl_preview_df = df

            st.success(f"預覽完成｜筆數 {len(df)}（已依勾選篩選：{', '.join(lock_types) if lock_types else '未篩選'}）")

    with cB:
        if st.button("套用到 DB（依上方模式與篩選）"):
            df = st.session_state.get("bl_preview_df")
            if df is None:
                st.warning("請先按『預覽名單』確認表格後，再套用到 DB。")
                st.stop()
            if df.empty:
                st.info("目前預覽表格為空，沒有可套用的名單。")
                st.stop()

            names = set(df["character_name"].astype(str).tolist())
            out = asyncio.run(purge_or_skip_blacklisted(
                world=world,
                blacklisted_names=names,
                mode=mode_val,
                delete_equip_history=True
            ))

            st.success(f"套用完成｜命中 {out.get('matched')}｜模式 {out.get('mode')}｜deleted={out.get('deleted')}｜skipped={out.get('skipped')}")
            st.info("提醒：官網名單僅代表停權；你若選『刪除』會清掉歷史資料（DB 才會變小需再搭配 VACUUM）。")

    # 表格顯示（不管預覽或套用後，都可以看）
    df_show = st.session_state.get("bl_preview_df")
    if df_show is not None:
        st.subheader("Step 2.6 名單表格（預覽結果）")
        st.dataframe(df_show, use_container_width=True, height=320)
        st.caption("欄位：character_name / reason / blockDate / unBlockedDate / lock_type")

    st.divider()
    st.subheader("Step 3：抓取角色資料（basic + stat + 裝備）")
    st.write(f"stat 抓取日期範圍：**{(base_date - timedelta(days=days_back)).isoformat()} ~ {base_date.isoformat()}**")

    # 裝備抓取摘要（便於確認模式）
    if equip_enabled:
        if equip_date_mode_ui == "抓取當天（不填 date）":
            st.write("裝備抓取：**抓取當天（不填 date）**")
        elif equip_date_mode_ui == "指定日期（填 date）":
            st.write(f"裝備抓取：**指定日期 {equip_base_date.isoformat()}**")
        else:
            s2 = (equip_base_date - timedelta(days=int(equip_days_back))).isoformat()
            st.write(f"裝備抓取：**{s2} ~ {equip_base_date.isoformat()}**（回溯 {int(equip_days_back)} 天）")
    else:
        st.write("裝備抓取：**本次不抓取**")


    if st.button("開始抓取（Step 3）"):
        prog = st.progress(0)
        status = st.empty()

        chars = asyncio.run(get_character_list(world))
        total = len(chars)

        def cb(i, name):
            if total > 0:
                prog.progress(min(1.0, (i + 1) / total))
            status.write(f"正在處理角色：{name}（{i+1}/{total}）")

        asyncio.run(update_characters(
            api_key=api_key,
            world=world,
            base_date=base_date,
            days_back=int(days_back),
            fetch_mode=fetch_mode,
            refresh_days=int(refresh_days),
            only_expired=only_expired,
            force_refresh_all=force_refresh_all,

            # 裝備抓取：可選日期模式（不影響 stat）
            equip_enabled=bool(equip_enabled),
            equip_date_mode=("today" if equip_date_mode_ui == "抓取當天（不填 date）" else ("date" if equip_date_mode_ui == "指定日期（填 date）" else "range")),
            equip_base_date=equip_base_date,
            equip_days_back=int(equip_days_back),

            progress_cb=cb
        ))
        st.success("抓取完成。")


# =====================================================
# Tab 3：資料完整性檢查
# =====================================================
with tabs[2]:
    st.header("資料完整性檢查（顯示進度與目前檢查項目）")

    if ignore_stat_range:
        start = "0001-01-01"
        end = "9999-12-31"
        st.write("檢查期間：**全期間（忽略時間區間）**（以 stat 的『戰鬥力』為最低檢查基準）")
    else:
        start = (base_date - timedelta(days=int(days_back))).isoformat()
        end = base_date.isoformat()
        st.write(f"檢查期間：**{start} ~ {end}**（以 stat 的『戰鬥力』為最低檢查基準）")

    if st.button("開始檢查"):
        prog = st.progress(0)
        status = st.empty()

        # ★修正：直接用 characters 的實際總數當 total（避免顯示不準）
        async def count_characters_in_world():
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("PRAGMA busy_timeout=10000;")
                async with db.execute("SELECT COUNT(1) FROM characters WHERE world_name=?", (world,)) as cur:
                    row = await cur.fetchone()
                    return int(row[0]) if row else 0

        total = asyncio.run(count_characters_in_world())

        def cb(i, name):
            if total > 0:
                prog.progress(min(1.0, (i + 1) / total))
            status.write(f"正在檢查：{name}（{i+1}/{total}）")

        result = asyncio.run(check_character_completeness(world, start, end, cb))
        st.write(f"檢查角色總數：**{result['total']}**")
        st.write(f"缺資料角色數：**{len(result['missing'])}**")

        if result["missing"]:
            st.error("缺資料清單：")
            st.dataframe(pd.DataFrame(result["missing"]), use_container_width=True)
        else:
            st.success("期間內戰鬥力資料皆存在。")


# =====================================================
# Tab 4：數據統計專區
# =====================================================
with tabs[3]:
    st.header("數據統計專區（期間內取最優資料比較）")

    if ignore_stat_range:
        start = "0001-01-01"
        end = "9999-12-31"
        st.write("統計期間：**全期間（忽略時間區間）**")
    else:
        start = (base_date - timedelta(days=int(days_back))).isoformat()
        end = base_date.isoformat()
        st.write(f"統計期間：**{start} ~ {end}**")


    st.divider()
    st.subheader("統計篩選條件")

    min_level = st.number_input(
        "排除低於此等級的角色（含以下不計）",
        min_value=1,
        max_value=300,
        value=1,
        step=1,
        help="例如輸入 260，代表只統計等級 ≥260 的角色"
    )

    colL, colR = st.columns([1, 1])
    with colL:
        if st.button("刷新等級 Top20"):
            top = asyncio.run(get_top20_by_level(world))
            show_top10_table("等級排行 Top20", top, mode="level")

    with colR:
        if st.button("刷新戰鬥力 Top20（期間內最高）"):
            top = asyncio.run(get_top20_by_best_power(world, start, end))
            show_top10_table("戰鬥力排行 Top20（期間內最高）", top, mode="power")

    st.divider()
    st.subheader("期間內最優戰鬥力分佈 / 等級分佈 / 性別 / 職業 / 解放數")

    data = asyncio.run(get_best_power_per_character_in_range(world, start, end))
    base = asyncio.run(get_level_and_class_gender_in_range(world))
    liberation = asyncio.run(get_liberation_count(world))

    # -------------------------------------------------
    # 套用最低等級門檻（統一統計資料來源）
    # -------------------------------------------------
    base_filtered = [
        r for r in base
        if r.get("character_level") is not None
        and int(r["character_level"]) >= int(min_level)
    ]

    st.caption(f"※ 本頁所有統計均僅包含等級 ≥ {min_level} 的角色（共 {len(base_filtered)} 人）")


    levels = [int(r["character_level"]) for r in base_filtered]
    c_186 = sum(1 for lv in levels if lv == 186)
    bins_level = [
        ("200(含)以下", sum(1 for lv in levels if lv <= 200)),
        ("其中186等", c_186),
        ("201-210", sum(1 for lv in levels if 201 <= lv <= 210)),
        ("211-259", sum(1 for lv in levels if 211 <= lv <= 259)),
        ("260-264", sum(1 for lv in levels if 260 <= lv <= 264)),
        ("265-269", sum(1 for lv in levels if 265 <= lv <= 269)),
        ("270-274", sum(1 for lv in levels if 270 <= lv <= 274)),
        ("275-279", sum(1 for lv in levels if 275 <= lv <= 279)),
        ("280-284", sum(1 for lv in levels if 280 <= lv <= 284)),
        ("285以上", sum(1 for lv in levels if lv >= 285)),
    ]
    render_pie_or_bar("等級人數分佈", [b[0] for b in bins_level], [b[1] for b in bins_level])

    powers = [int(x["best_power"]) for x in data if x.get("best_power") is not None]
    bins_power = [
        ("5億以上", sum(1 for p in powers if p >= 500_000_000)),
        ("4億-4億9999萬9999", sum(1 for p in powers if 400_000_000 <= p <= 499_999_999)),
        ("3億-3億9999萬9999", sum(1 for p in powers if 300_000_000 <= p <= 399_999_999)),
        ("2億-2億9999萬9999", sum(1 for p in powers if 200_000_000 <= p <= 299_999_999)),
        ("1億-1億9999萬9999", sum(1 for p in powers if 100_000_000 <= p <= 199_999_999)),
        ("9000萬-99999999", sum(1 for p in powers if 90_000_000 <= p <= 99_999_999)),
        ("8000萬-89999999", sum(1 for p in powers if 80_000_000 <= p <= 89_999_999)),
        ("7000萬-79999999", sum(1 for p in powers if 70_000_000 <= p <= 79_999_999)),
        ("6000萬-69999999", sum(1 for p in powers if 60_000_000 <= p <= 69_999_999)),
        ("5000萬-59999999", sum(1 for p in powers if 50_000_000 <= p <= 59_999_999)),
        ("4000萬-49999999", sum(1 for p in powers if 40_000_000 <= p <= 49_999_999)),
        ("3000萬-39999999", sum(1 for p in powers if 30_000_000 <= p <= 39_999_999)),
        ("2000萬-29999999", sum(1 for p in powers if 20_000_000 <= p <= 29_999_999)),
        ("1000萬-19999999", sum(1 for p in powers if 10_000_000 <= p <= 19_999_999)),
        ("1000萬以下", sum(1 for p in powers if p < 10_000_000)),
    ]
    render_pie_or_bar("戰鬥力人數分佈（期間內最佳）", [b[0] for b in bins_power], [b[1] for b in bins_power])

    male = 0
    female = 0
    male_lian = 0
    female_lian = 0

    for r in base_filtered:
        g = (r.get("character_gender") or "").strip()
        c = normalize_class(r.get("character_class"), r.get("character_date_create"))
        if g == "男":
            male += 1
            if c == "蓮":
                male_lian += 1
        elif g == "女":
            female += 1
            if c == "蓮":
                female_lian += 1

    render_pie_or_bar("角色性別比例", ["男", "女"], [male, female])
    render_pie_or_bar("「蓮」職業性別比例", ["男(蓮)", "女(蓮)"], [male_lian, female_lian])

    class_count = {}
    for r in base_filtered:
        c = normalize_class(r.get("character_class"), r.get("character_date_create"))
        class_count[c] = class_count.get(c, 0) + 1

    cls_labels = sorted(class_count.keys(), key=lambda k: class_count[k], reverse=True)
    cls_counts = [class_count[k] for k in cls_labels]
    render_pie_or_bar("角色職業比例", cls_labels, cls_counts)

    st.subheader("已解放創世武器（liberation_quest_clear=1）")
    st.write(f"數量：**{liberation}**")

    st.divider()
    st.subheader("查表：角色ID包含指定字元（可多條件合併去重）")
    kw_text = st.text_input("輸入條件（用逗號或空白分隔）", value="兔,蓮")
    kws = [k for k in kw_text.replace("，", ",").replace(" ", ",").split(",") if k.strip()]

    if st.button("開始查表"):
        res = asyncio.run(count_characters_by_name_keywords(world, kws))
        st.write(f"去重後符合任一條件的人數：**{res['total_unique']}**")
        st.write("各條件命中人數（不去重）：")
        st.json(res["per_keyword"])
        with st.expander("展開查看去重後角色清單"):
            st.dataframe(pd.DataFrame({"character_name": res["matched_names"]}), use_container_width=True)

    st.divider()
    st.header("新增：角色 / 付費版道具統計（DB-only）")

    with st.expander("功能 1：輸入角色ID（純角色名）查該角色目前 DB 全資料", expanded=True):
        role_id = st.text_input("角色ID（純角色名）", value="")
        if st.button("查詢"):
            profile = asyncio.run(
                get_character_full_profile_by_role_id(world, role_id)
            )

            if not profile:
                st.warning("查無角色資料")
                st.stop()

            st.markdown(f"### {profile['basic']['character_name']}（{profile['basic']['character_class']}）")
            st.caption(f"等級 {profile['basic']['character_level']}")

            latest = profile.get("latest_equip") or {}
            equip_raw = latest.get("raw") or {}
            items = equip_raw.get("item_equipment") or []

            if not items:
                st.info("查無裝備資料")
            else:
                st.subheader("目前裝備（Best Preset）")

                slot_map = build_slot_map(items)

                def render_row(slots: list[str]):
                    html = ""
                    temp_count = {}
                    for s in slots:
                        cnt = temp_count.get(s, 0)
                        temp_count[s] = cnt + 1

                        val = slot_map.get(s)
                        if isinstance(val, list):
                            if cnt < len(val):
                                html += render_item_icon(val[cnt])
                            else:
                                html += render_placeholder(s)
                        elif isinstance(val, dict):
                            html += render_item_icon(val)
                        else:
                            html += render_placeholder(s)
                    st.markdown(html, unsafe_allow_html=True)


                # === 第一行 ===
                render_row(EQUIP_LAYOUT["row1"])

                # === 第二行 ===
                render_row(EQUIP_LAYOUT["row2"])

                # === 圖騰 ===
                st.markdown("<div style='margin-top:6px'></div>", unsafe_allow_html=True)
                render_row(EQUIP_LAYOUT["totem"])

                # === 中央（角色圖可之後插）===
                render_row(EQUIP_LAYOUT["center"])

                # === 第三行（套服判斷）===
                has_top = "上衣" in slot_map
                has_bottom = "下衣" in slot_map

                html = ""
                for s in EQUIP_LAYOUT["row3"]:
                    if s == "下衣" and has_top and not has_bottom:
                        html += render_placeholder("套服", red=True)
                    else:
                        val = slot_map.get(s)
                        if isinstance(val, dict):
                            html += render_item_icon(val)
                        else:
                            html += render_placeholder(s)

                st.markdown(html, unsafe_allow_html=True)

                # === 第四行 ===
                render_row(EQUIP_LAYOUT["row4"])




    with st.expander("功能 2：統計『付費版』神祕冥界幽靈之劍持有人數（潛能完全一致）", expanded=True):
        st.caption("判定：item_name + slot/part + icon + potential_grade + potential_option_1/2/3 完全一致。")
        if st.button("開始統計（DB）"):
            result = asyncio.run(count_paid_sword_holders(world, include_list=True))
            st.success(f"符合『付費版』條件的人數：**{result['count']}**")

            holders = result.get("holders") or []
            if holders:
                df = pd.DataFrame(holders)
                st.dataframe(df, use_container_width=True, height=320)

                st.subheader("從名單選擇角色，展開查看 DB 全資料")
                names = [h["character_id"] for h in holders]
                sel = st.selectbox("選擇角色", options=[""] + names, index=0)
                if sel:
                    profile = asyncio.run(get_character_full_profile_by_role_id(world, sel))
                    if not profile:
                        st.error("DB 找不到此角色（可能 basic 尚未抓取或資料不完整）。")
                    else:
                        render_character_full_profile(profile)
            else:
                st.info("目前 DB 中沒有符合條件的角色。")




    st.divider()
    st.subheader("新增：裝備 / 潛能 / 指定道具統計（DB-only，使用 best 裝備展開表）")

    with st.expander("功能 3：指定道具持有玩家數量 / 自訂物品搜尋 / 同時裝備 N 件（DB）", expanded=True):
        st.write("預設道具：**輪迴碑石**、**勇敢挑戰者的圖騰**（可加上『挑戰者鑽石階級』提示）")

        colX, colY = st.columns([1, 1])
        with colX:
            if st.button("統計：輪迴碑石持有人數"):
                out = asyncio.run(count_item_holders(world, "輪迴碑石", include_list=False))
                st.success(f"輪迴碑石持有人數：**{out['count']}**")

        with colY:
            if st.button("統計：勇敢挑戰者的圖騰（數值完全一致）"):
                brave_spec = {
                    "item_equipment_part": "馴服的怪物",
                    "item_equipment_slot": "馴服的怪物",
                    "item_name": "勇敢挑戰者的圖騰",
                    "item_total_option": {
                        "str": "40", "dex": "40", "int": "40", "luk": "40",
                        "attack_power": "15", "magic_power": "15",
                    },
                }
                out = asyncio.run(count_item_holders(world, "勇敢挑戰者的圖騰", exact_match=brave_spec, include_list=False))
                st.success(f"持有勇敢挑戰者的圖騰／達成挑戰者鑽石階級的人數：**{out['count']}**")

        st.divider()
        st.subheader("自訂：依物品名稱搜尋（完全比對）")
        custom_item = st.text_input("物品名稱", value="")
        show_list = st.checkbox("列出名單（前 5000 以內）", value=False)

        if st.button("開始統計（自訂物品）"):
            if not custom_item.strip():
                st.warning("請先輸入物品名稱")
            else:
                out = asyncio.run(count_item_holders(world, custom_item.strip(), include_list=show_list))
                st.success(f"『{custom_item.strip()}』持有人數：**{out['count']}**")
                if show_list:
                    df = pd.DataFrame(out.get("holders") or [])
                    st.dataframe(df, use_container_width=True, height=320)

        st.divider()
        st.subheader("同時裝備 N 件指定裝備（以『不同物品名稱』計數）")
        combo_text = st.text_area("每行一個物品名稱", height=120, placeholder="輪迴碑石\n勇敢挑戰者的圖騰\n...")
        min_n = st.number_input("至少同時裝備幾種（distinct item_name）", min_value=1, max_value=10, value=2)
        if st.button("開始統計（同時裝備 N 件）"):
            items = parse_bulk_lines(combo_text)
            if not items:
                st.warning("請先輸入至少 1 個物品名稱")
            else:
                cnt = asyncio.run(count_combo_items(world, items, int(min_n)))
                st.success(f"同時裝備 ≥{int(min_n)} 種指定裝備的人數：**{cnt}**")

    with st.expander("功能 4：指定部位潛能條件統計（AND 條件，詞條白名單，DB）", expanded=True):
        st.caption("支援：a) 主要/附加都傳說（雙傳說）人數；b) 依詞條種類 + 門檻 + 至少幾排，規則之間用 AND 串接。")
        part = st.selectbox("部位（item_equipment_slot）", options=["輔助武器", "徽章", "武器"], index=0)

        colA, colB = st.columns([1, 2])
        with colA:
            if st.button("統計：雙傳說人數（此部位）"):
                cnt = asyncio.run(count_double_legendary_by_part(world, part))
                st.success(f"{part}｜主要/附加皆傳說：**{cnt}** 人")

        st.divider()
        st.subheader("規則設定（最多 3 條，全部 AND）")
        st.caption("數值不需要額外解析，直接用你提供的『可能詞條清單』做白名單比對。")

        # 三條規則（可自行留空不啟用）
        def rule_row(prefix: str):
            c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
            with c1:
                enabled = st.checkbox(f"{prefix}啟用", value=False, key=f"{prefix}_en")
            with c2:
                side = st.selectbox(f"{prefix}潛能面", options=[("main", "主要潛能"), ("add", "附加潛能")], format_func=lambda x: x[1], key=f"{prefix}_side")[0]
            with c3:
                token_type = st.selectbox(f"{prefix}詞條類型", options=list(POT_TYPE_LABEL.keys()), format_func=lambda k: POT_TYPE_LABEL[k], key=f"{prefix}_type")
            with c4:
                min_lines = st.number_input(f"{prefix}至少幾排", min_value=1, max_value=3, value=2, key=f"{prefix}_lines")
            threshold = st.selectbox(
                f"{prefix}門檻（>=）",
                options=sorted(POTENTIAL_WHITELIST[token_type].keys()),
                index=0,
                key=f"{prefix}_th"
            )
            return enabled, {"side": side, "token_type": token_type, "threshold": int(threshold), "min_lines": int(min_lines)}

        r1_en, r1 = rule_row("規則1-")
        r2_en, r2 = rule_row("規則2-")
        r3_en, r3 = rule_row("規則3-")

        require_double = st.checkbox("同時要求：雙傳說（主要/附加皆傳說）", value=False)

        if st.button("開始統計（AND 規則）"):
            rules = []
            if r1_en: rules.append(r1)
            if r2_en: rules.append(r2)
            if r3_en: rules.append(r3)

            if not rules and not require_double:
                st.warning("你目前沒有啟用任何規則，也沒有勾選『雙傳說』。")
            else:
                cnt = asyncio.run(count_potential_and_rules(
                    world=world,
                    part=part,
                    require_double_legendary=bool(require_double),
                    rules=rules
                ))
                st.success(f"符合條件的人數：**{cnt}**")


# =====================================================
# Tab 5：資料優化
# =====================================================
with tabs[4]:
    st.header("資料優化（縮小 DB、統一最優資料）")

    start = (base_date - timedelta(days=int(days_back))).isoformat()
    end = base_date.isoformat()
    st.write(f"優化期間：**{start} ~ {end}**")

    st.warning("注意：SQLite 刪除後檔案不一定縮小，需要額外執行 VACUUM 才會真的縮檔。")

    if st.button("開始優化（刪除多餘資料）"):
        prog = st.progress(0)
        status = st.empty()

        async def count_world_characters():
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("PRAGMA busy_timeout=10000;")
                async with db.execute("SELECT COUNT(1) FROM characters WHERE world_name=?", (world,)) as cur:
                    row = await cur.fetchone()
                    return int(row[0]) if row else 0

        total = asyncio.run(count_world_characters())

        def cb(i, name):
            if total > 0:
                prog.progress(min(1.0, (i + 1) / total))
            status.write(f"正在優化：{name}（{i+1}/{total}）")

        report = asyncio.run(optimize_data(world, start, end, cb))

        st.success("優化完成")
        st.write(f"刪除戰鬥力列數：**{report['power_deleted_rows']}**")
        st.write(f"刪除裝備日資料列數：**{report['equip_deleted_rows']}**")

        with st.expander("查看保留摘要（前 50 筆）"):
            st.write("戰鬥力保留：")
            st.dataframe(pd.DataFrame(report["power_kept"][:50]), use_container_width=True)
            st.write("裝備保留：")
            st.dataframe(pd.DataFrame(report["equip_kept"][:50]), use_container_width=True)

    if st.button("執行 VACUUM（真正縮小 DB）"):
        st.info("VACUUM 執行中，期間請勿中斷。")
        asyncio.run(vacuum_db())
        st.success("VACUUM 完成。")
