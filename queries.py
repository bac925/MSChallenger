import aiosqlite
import json
from typing import List, Set
from db import DB_PATH

# ============================================================
# 基礎工具：偵測 table 是否存在 & 取得欄位
# ============================================================
async def _table_exists(db: aiosqlite.Connection, table_name: str) -> bool:
    async with db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ) as cur:
        return (await cur.fetchone()) is not None


async def _table_columns(db: aiosqlite.Connection, table_name: str) -> set[str]:
    async with db.execute(f"PRAGMA table_info({table_name});") as cur:
        rows = await cur.fetchall()
    return {r[1] for r in rows}  # r[1] = column name


async def _pick_equip_table(db: aiosqlite.Connection) -> tuple[str, set[str]]:
    """
    優先使用 character_item_equipment_best（若存在），否則使用 character_item_equipment。
    回傳：(table_name, columns_set)
    """
    if await _table_exists(db, "character_item_equipment_best"):
        cols = await _table_columns(db, "character_item_equipment_best")
        return "character_item_equipment_best", cols

    cols = await _table_columns(db, "character_item_equipment")
    return "character_item_equipment", cols


# ============================================================
# 你原本 queries.py 既有功能（保留不動/或依你原檔案有調整）
# ============================================================
async def get_guild_rows():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
        SELECT guild_name, world_name
        FROM guild_list
        ORDER BY world_name, guild_name
        """) as cur:
            rows = await cur.fetchall()
            return [{"guild_name": r[0], "world_name": r[1]} for r in rows]


async def get_guild_list():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
        SELECT guild_name
        FROM guild_list
        ORDER BY guild_name
        """) as cur:
            rows = await cur.fetchall()
            return [r[0] for r in rows]


async def get_character_list(world: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT character_name
            FROM character_list
            WHERE world_name = ?
            ORDER BY character_name
        """, (world,)) as cur:
            rows = await cur.fetchall()
            return [r[0] for r in rows]


async def replace_guild_list(new_rows):
    cleaned = []
    seen = set()

    for row in new_rows:
        g = str(row.get("guild_name", "")).strip()
        w = str(row.get("world_name", "")).strip()
        if not g or not w:
            continue
        key = (g, w)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append((g, w))

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN")
        await db.execute("DELETE FROM guild_list")
        if cleaned:
            await db.executemany("""
                INSERT INTO guild_list (guild_name, world_name)
                VALUES (?, ?)
            """, cleaned)
        await db.commit()

    return {"kept": len(cleaned), "dropped": len(new_rows) - len(cleaned)}


async def get_top20_by_level(world: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT
              ocid,
              character_name,
              character_image,
              character_level,
              character_class,
              character_date_create
            FROM characters
            WHERE world_name = ?
            ORDER BY character_level DESC, character_name ASC
            LIMIT 20
        """, (world,)) as cur:
            rows = await cur.fetchall()

    return [{
        "ocid": r[0],
        "character_name": r[1],
        "character_image": r[2],
        "character_level": r[3],
        "character_class": r[4],
        "character_date_create": r[5],
    } for r in rows]


async def get_top20_by_best_power(world: str, start_date: str, end_date: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            WITH power AS (
              SELECT
                ocid,
                MAX(stat_value) AS best_power
              FROM character_stats
              WHERE stat_name = '戰鬥力'
                AND stat_date BETWEEN ? AND ?
              GROUP BY ocid
            )
            SELECT
              c.ocid,
              c.character_name,
              c.character_image,
              c.character_level,
              c.character_class,
              c.character_date_create,
              p.best_power
            FROM power p
            JOIN characters c ON c.ocid = p.ocid
            WHERE c.world_name = ?
            ORDER BY p.best_power DESC, c.character_name ASC
            LIMIT 20
        """, (start_date, end_date, world)) as cur:
            rows = await cur.fetchall()

    return [{
        "ocid": r[0],
        "character_name": r[1],
        "character_image": r[2],
        "character_level": r[3],
        "character_class": r[4],
        "character_date_create": r[5],
        "best_power": int(r[6]) if r[6] is not None else None
    } for r in rows]


async def get_best_power_per_character_in_range(world: str, start_date: str, end_date: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            WITH power AS (
              SELECT
                ocid,
                MAX(stat_value) AS best_power
              FROM character_stats
              WHERE stat_name='戰鬥力'
                AND stat_date BETWEEN ? AND ?
              GROUP BY ocid
            )
            SELECT c.ocid, c.character_name, c.character_class, c.character_gender, c.character_level, p.best_power, c.character_date_create
            FROM power p
            JOIN characters c ON c.ocid = p.ocid
            WHERE c.world_name = ?
        """, (start_date, end_date, world)) as cur:
            rows = await cur.fetchall()

    return [{
        "ocid": r[0],
        "character_name": r[1],
        "character_class": r[2],
        "character_gender": r[3],
        "character_level": r[4],
        "best_power": int(r[5]) if r[5] is not None else None,
        "character_date_create": r[6],
    } for r in rows]


async def get_level_and_class_gender_in_range(world: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT
              character_name,
              character_class,
              character_gender,
              character_level,
              character_date_create,
              liberation_quest_clear
            FROM characters
            WHERE world_name = ?
        """, (world,)) as cur:
            rows = await cur.fetchall()

    return [{
        "character_name": r[0],
        "character_class": r[1],
        "character_gender": r[2],
        "character_level": r[3],
        "character_date_create": r[4],
        "liberation_quest_clear": r[5],
    } for r in rows]


async def get_liberation_count(world: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT COUNT(1)
            FROM characters
            WHERE world_name = ?
              AND liberation_quest_clear = 1
        """, (world,)) as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else 0


async def count_characters_by_name_keywords(world: str, keywords: list[str]):
    kws = [k.strip() for k in (keywords or []) if k and k.strip()]
    if not kws:
        return {"total_unique": 0, "per_keyword": {}, "matched_names": []}

    per_kw = {}
    union_set = set()

    async with aiosqlite.connect(DB_PATH) as db:
        for kw in kws:
            async with db.execute("""
                SELECT character_name
                FROM characters
                WHERE world_name = ?
                  AND character_name LIKE ?
            """, (world, f"%{kw}%")) as cur:
                rows = await cur.fetchall()
                names = [r[0] for r in rows]
                per_kw[kw] = len(names)
                union_set.update(names)

    return {
        "total_unique": len(union_set),
        "per_keyword": per_kw,
        "matched_names": sorted(union_set)
    }


# ============================================================
# 新功能 1：角色ID（純角色名）查 DB 全資料
#   裝備來源：優先 best 表，否則原表
# ============================================================

async def get_character_full_profile_by_role_id(world: str, role_id: str):
    """
    以「角色名」查詢該角色目前 DB 全資料（basic / power_summary / latest_stats / latest_equip）。
    裝備來源：優先 character_item_equipment_best（best_equipment_json），否則 fallback 到 history 表。
    """
    role_id = (role_id or "").strip()
    if not role_id:
        return None

    async with aiosqlite.connect(DB_PATH) as db:
        # resolve ocid by (world, character_name)
        async with db.execute(
            """
            SELECT ocid
            FROM characters
            WHERE world_name=? AND character_name=?
            LIMIT 1
            """,
            (world, role_id),
        ) as cur:
            r = await cur.fetchone()
            if not r:
                return None
            ocid = r[0]

        # basic
        async with db.execute(
            """
            SELECT
              ocid, character_name, world_name, character_gender,
              character_class, character_class_level, character_level,
              character_exp, character_exp_rate,
              character_guild_name, character_image,
              character_date_create, liberation_quest_clear, access_flag, updated_at
            FROM characters
            WHERE ocid = ?
            LIMIT 1
            """,
            (ocid,),
        ) as cur:
            b = await cur.fetchone()

        basic = {
            "ocid": b[0],
            "character_name": b[1],
            "world_name": b[2],
            "character_gender": b[3],
            "character_class": b[4],
            "character_class_level": b[5],
            "character_level": b[6],
            "character_exp": b[7],
            "character_exp_rate": b[8],
            "character_guild_name": b[9],
            "character_image": b[10],
            "character_date_create": b[11],
            "liberation_quest_clear": b[12],
            "access_flag": b[13],
            "updated_at": b[14],
        }

        # power summary
        async with db.execute(
            """
            SELECT power_best, power_best_date, power_today, power_yesterday, updated_at
            FROM character_power_summary
            WHERE ocid = ?
            LIMIT 1
            """,
            (ocid,),
        ) as cur:
            ps = await cur.fetchone()

        power_summary = None
        if ps:
            power_summary = {
                "power_best": ps[0],
                "power_best_date": ps[1],
                "power_today": ps[2],
                "power_yesterday": ps[3],
                "updated_at": ps[4],
            }

        # latest stats date
        async with db.execute(
            """
            SELECT stat_date
            FROM character_stats
            WHERE ocid = ?
            ORDER BY stat_date DESC
            LIMIT 1
            """,
            (ocid,),
        ) as cur:
            sd = await cur.fetchone()

        latest_stats = {"stat_date": None, "stats": []}
        if sd:
            stat_date = sd[0]
            async with db.execute(
                """
                SELECT stat_name, stat_value
                FROM character_stats
                WHERE ocid = ? AND stat_date = ?
                ORDER BY stat_name
                """,
                (ocid, stat_date),
            ) as cur:
                rows = await cur.fetchall()
            latest_stats = {
                "stat_date": stat_date,
                "stats": [{"stat_name": r[0], "stat_value": r[1]} for r in rows],
            }

        # latest equip: prefer BEST
        latest_equip = {"equip_date": None, "fetched_at": None, "raw": None, "source_table": None}

        async with db.execute(
            """
            SELECT best_equip_date, updated_at, best_equipment_json
            FROM character_item_equipment_best
            WHERE ocid = ?
            LIMIT 1
            """,
            (ocid,),
        ) as cur:
            er = await cur.fetchone()

        if er and er[2]:
            equip_date, fetched_at, raw_json = er[0], er[1], er[2]
            try:
                raw = json.loads(raw_json)
            except Exception:
                raw = None
            latest_equip = {
                "equip_date": equip_date,
                "fetched_at": fetched_at,
                "raw": raw,
                "source_table": "character_item_equipment_best",
            }
        else:
            # fallback：history table
            async with db.execute(
                """
                SELECT equip_date, fetched_at, raw_json
                FROM character_item_equipment
                WHERE ocid = ?
                ORDER BY equip_date DESC
                LIMIT 1
                """,
                (ocid,),
            ) as cur:
                er2 = await cur.fetchone()
            if er2 and er2[2]:
                equip_date, fetched_at, raw_json = er2
                try:
                    raw = json.loads(raw_json)
                except Exception:
                    raw = None
                latest_equip = {
                    "equip_date": equip_date,
                    "fetched_at": fetched_at,
                    "raw": raw,
                    "source_table": "character_item_equipment",
                }

        return {
            "basic": basic,
            "power_summary": power_summary,
            "latest_stats": latest_stats,
            "latest_equip": latest_equip,
        }


# ============================================================
# 新功能 2：統計付費版武器持有人數（潛能完全一致、順序固定）
#   裝備來源：固定只用 best 表（character_item_equipment_best）
# ============================================================
PAID_SWORD_TARGET = {
    "item_equipment_slot": "武器",
    "potential_option_grade": "傳說",
    # 注意：順序固定 → 下面三條要依序比對
    "potential_option_1": "攻擊Boss怪物時傷害 +35%",
    "potential_option_2": "傷害 +12%",
    "potential_option_3": "爆擊機率 +12%",
}

def _extract_weapon_item(equip_json: dict) -> dict | None:
    """
    best_equipment_json 已被裁剪成 item_equipment（最佳組合）
    這裡只要找 slot == '武器' 的那一件即可。
    """
    items = equip_json.get("item_equipment") or []
    if not isinstance(items, list):
        return None

    for it in items:
        if isinstance(it, dict) and it.get("item_equipment_slot") == "武器":
            return it
    return None


def _is_paid_variant(item: dict) -> bool:
    """
    順序固定：pot1/pot2/pot3 必須完全一致。
    並且需符合：
      - slot == '武器'
      - 潛能階級 == '傳說'
    """
    if not isinstance(item, dict):
        return False

    if item.get("item_equipment_slot") != PAID_SWORD_TARGET["item_equipment_slot"]:
        return False

    # 階級一致（主潛能）
    if item.get("potential_option_grade") != PAID_SWORD_TARGET["potential_option_grade"]:
        return False

    # ★順序固定：完全依序比對
    if item.get("potential_option_1") != PAID_SWORD_TARGET["potential_option_1"]:
        return False
    if item.get("potential_option_2") != PAID_SWORD_TARGET["potential_option_2"]:
        return False
    if item.get("potential_option_3") != PAID_SWORD_TARGET["potential_option_3"]:
        return False

    return True


async def count_paid_sword_holders(world: str, include_list: bool = True):
    async with aiosqlite.connect(DB_PATH) as db:
        sql = """
            SELECT
              c.character_name,
              c.world_name,
              c.ocid,
              c.character_level,
              c.character_class,
              b.best_equip_date,
              b.updated_at,
              b.best_equipment_json
            FROM character_item_equipment_best b
            JOIN characters c ON c.ocid = b.ocid
            WHERE c.world_name = ?
        """
        async with db.execute(sql, (world,)) as cur:
            rows = await cur.fetchall()

    holders = []
    seen = set()

    for (
        name, w, ocid, lv, job,
        equip_date, updated_at, raw_json
    ) in rows:
        if ocid in seen:
            continue
        if not raw_json:
            continue

        try:
            equip = json.loads(raw_json)
        except Exception:
            continue

        weapon = _extract_weapon_item(equip)
        if not weapon:
            continue

        if _is_paid_variant(weapon):
            seen.add(ocid)
            holders.append({
                "character_id": name,
                "world_name": w,
                "ocid": ocid,
                "character_level": lv,
                "character_class": job,
                "equip_date": equip_date,
                "fetched_at": updated_at,
                "source_table": "character_item_equipment_best",
            })

    return {
        "world": world,
        "target": PAID_SWORD_TARGET,
        "count": len(holders),
        "holders": holders if include_list else None
    }



# ============================================================
# 共用工具：從 best 裝備 JSON 取 item_equipment list
# ============================================================
def _iter_best_items(best_equipment_json: str):
    if not best_equipment_json:
        return []
    try:
        data = json.loads(best_equipment_json)
    except Exception:
        return []
    items = data.get("item_equipment")
    return items if isinstance(items, list) else []

# ============================================================
# 裝備 / 潛能統計（DB-only, best-only）
# ============================================================

POTENTIAL_WHITELIST = {
    "BOSS_DMG": {
        30: ["攻擊Boss怪物時傷害 +30%", "攻擊Boss怪物時傷害 +35%", "攻擊Boss怪物時傷害 +40%"],
        35: ["攻擊Boss怪物時傷害 +35%", "攻擊Boss怪物時傷害 +40%"],
        40: ["攻擊Boss怪物時傷害 +40%"],
    },
    "ATT_PCT": {
        3:  ["物理攻擊力 +3%", "物理攻擊力 +6%", "物理攻擊力 +7%", "物理攻擊力 +9%", "物理攻擊力 +12%", "物理攻擊力 +13%"],
        6:  ["物理攻擊力 +6%", "物理攻擊力 +7%", "物理攻擊力 +9%", "物理攻擊力 +12%", "物理攻擊力 +13%"],
        9:  ["物理攻擊力 +9%", "物理攻擊力 +12%", "物理攻擊力 +13%"],
        12: ["物理攻擊力 +12%", "物理攻擊力 +13%"],
    },
    "MATK_PCT": {
        3:  ["魔法攻擊力 +3%", "魔法攻擊力 +6%", "魔法攻擊力 +7%", "魔法攻擊力 +9%", "魔法攻擊力 +12%", "魔法攻擊力 +13%"],
        6:  ["魔法攻擊力 +6%", "魔法攻擊力 +7%", "魔法攻擊力 +9%", "魔法攻擊力 +12%", "魔法攻擊力 +13%"],
        9:  ["魔法攻擊力 +9%", "魔法攻擊力 +12%", "魔法攻擊力 +13%"],
        12: ["魔法攻擊力 +12%", "魔法攻擊力 +13%"],
    },
}


def _count_tokens(item: dict, tokens: List[str], side: str) -> int:
    keys = (
        ("additional_potential_option_1",
         "additional_potential_option_2",
         "additional_potential_option_3")
        if side == "add"
        else
        ("potential_option_1",
         "potential_option_2",
         "potential_option_3")
    )
    return sum(1 for k in keys if item.get(k) in tokens)


async def count_double_legendary_by_part(world: str, part: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        rows = await db.execute_fetchall("""
            SELECT b.best_equipment_json
            FROM character_item_equipment_best b
            JOIN characters c ON c.ocid=b.ocid
            WHERE c.world_name=?
        """, (world,))

    cnt = 0
    for (raw,) in rows:
        for it in _iter_best_items(raw):
            if it.get("item_equipment_slot") != part:
                continue
            if (
                it.get("potential_option_grade") == "傳說"
                and it.get("additional_potential_option_grade") == "傳說"
            ):
                cnt += 1
                break
    return cnt


async def count_potential_and_rules(
    world: str,
    part: str,
    require_double_legendary: bool,
    rules: list[dict],
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        rows = await db.execute_fetchall("""
            SELECT b.best_equipment_json
            FROM character_item_equipment_best b
            JOIN characters c ON c.ocid=b.ocid
            WHERE c.world_name=?
        """, (world,))

    cnt = 0
    for (raw,) in rows:
        for it in _iter_best_items(raw):
            if it.get("item_equipment_slot") != part:
                continue

            if require_double_legendary:
                if not (
                    it.get("potential_option_grade") == "傳說"
                    and it.get("additional_potential_option_grade") == "傳說"
                ):
                    continue

            ok = True
            for r in rules:
                tokens = POTENTIAL_WHITELIST[r["token_type"]][int(r["threshold"])]
                if _count_tokens(it, tokens, r.get("side", "main")) < int(r["min_lines"]):
                    ok = False
                    break

            if ok:
                cnt += 1
                break
    return cnt


async def count_item_holders(
    world: str,
    item_name: str,
    *,
    exact_match: dict | None = None,
    include_list: bool = False,
):
    async with aiosqlite.connect(DB_PATH) as db:
        rows = await db.execute_fetchall("""
            SELECT c.character_name, c.ocid, b.best_equipment_json
            FROM character_item_equipment_best b
            JOIN characters c ON c.ocid=b.ocid
            WHERE c.world_name=?
        """, (world,))

    holders = []
    seen = set()

    def _match_exact(it: dict, spec: dict) -> bool:
        for k, v in spec.items():
            if isinstance(v, dict):
                sub = it.get(k) or {}
                if not isinstance(sub, dict):
                    return False
                for sk, sv in v.items():
                    if str(sub.get(sk)) != str(sv):
                        return False
            else:
                if str(it.get(k)) != str(v):
                    return False
        return True

    for name, ocid, raw in rows:
        if ocid in seen:
            continue
        for it in _iter_best_items(raw):
            if it.get("item_name") != item_name:
                continue
            if exact_match and not _match_exact(it, exact_match):
                continue
            seen.add(ocid)
            if include_list:
                holders.append({"character_name": name, "ocid": ocid})
            break

    return {"count": len(seen), "holders": holders if include_list else None}


async def count_combo_items(world: str, item_names: List[str], min_distinct: int) -> int:
    names = set(x.strip() for x in item_names if x and x.strip())
    if not names:
        return 0

    async with aiosqlite.connect(DB_PATH) as db:
        rows = await db.execute_fetchall("""
            SELECT b.best_equipment_json
            FROM character_item_equipment_best b
            JOIN characters c ON c.ocid=b.ocid
            WHERE c.world_name=?
        """, (world,))

    cnt = 0
    for (raw,) in rows:
        equipped = {it.get("item_name") for it in _iter_best_items(raw)}
        if len(equipped & names) >= min_distinct:
            cnt += 1
    return cnt
