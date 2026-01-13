import asyncio
import aiohttp
import aiosqlite
import json
from datetime import date, timedelta, datetime, time as dtime

from nexon_api import NexonClient
from db import DB_PATH


# ============================================================
# 兼容 NexonClient 回傳格式：可能回傳 data、(data, err)、(data, err, ...)
# ============================================================
def _unpack_api_result(result):
    if isinstance(result, tuple):
        data = result[0] if len(result) >= 1 else None
        err = result[1] if len(result) >= 2 else None
        return data, err
    return result, None


# ============================================================
# common helpers
# ============================================================
def safe_float(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def extract_power(stat_json):
    """
    tools_light_daemon.py 會 import 這個，所以必須保留。
    """
    if not stat_json:
        return None
    for s in stat_json.get("final_stat", []):
        if (s.get("stat_name") or "").strip() == "戰鬥力":
            try:
                return int(float(str(s.get("stat_value")).strip()))
            except Exception:
                return None
    return None


def count_drop_rate_mentions(raw_json_text: str) -> int:
    if not raw_json_text:
        return 0
    return raw_json_text.count("道具掉落率")


def _as_access_flag(v) -> int:
    s = str(v or "").strip().lower()
    return 1 if s == "true" else 0


def _chunked(lst, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _days_since(iso_dt: str) -> int:
    try:
        d = datetime.fromisoformat(iso_dt.replace("Z", "+00:00")).date()
        return (date.today() - d).days
    except Exception:
        return 999999


def pick_best_preset_by_lowest_drop_rate(equip_json: dict) -> dict:
    """
    Nexon 裝備 API 同時回傳：
      - item_equipment：玩家「目前選擇」的 preset 裝備（但這組常會包含某些只出現在這裡的部位/道具）
      - item_equipment_preset_1 / 2 / 3：三組 preset 的裝備清單

    需求：
      1) 不能「吃掉」只會出現在 item_equipment 的部位/道具
      2) 必須在 (preset1/2/3) 之間比對，找出「道具掉落率」出現次數最少的那一組
      3) 再把該 preset 與 item_equipment 做 merge（以該 preset 為主，item_equipment 補缺漏）
      4) 最後裁剪 JSON：只保留 item_equipment（最佳組合）與 preset_no

    Tie-break：若掉落率計數相同，偏好 preset 2（通常第二組是戰鬥力最強）
    """
    if not equip_json:
        return equip_json

    preset_keys = [
        (1, "item_equipment_preset_1"),
        (2, "item_equipment_preset_2"),
        (3, "item_equipment_preset_3"),
    ]

    base_items = equip_json.get("item_equipment")
    if not isinstance(base_items, list):
        base_items = []

    def _slot_key(it: dict) -> str:
        # 用 slot 為主，沒有就退回 part，避免 key 缺失
        return str(it.get("item_equipment_slot") or it.get("item_equipment_part") or "").strip()

    def _merge_items(base: list, preset: list) -> list:
        """以 preset 為主，base 補缺漏（避免吃掉只存在於 item_equipment 的部位/道具）"""
        merged = {}
        # 先放 base（補洞）
        for it in base:
            if not isinstance(it, dict):
                continue
            k = _slot_key(it)
            if k:
                merged[k] = it
        # 再用 preset 覆蓋（以 preset 為主）
        for it in preset:
            if not isinstance(it, dict):
                continue
            k = _slot_key(it)
            if k:
                merged[k] = it
        # 穩定輸出：依 slot_key 排序（避免每次 json dumps 順序不同造成 diff 太大）
        return [merged[k] for k in sorted(merged.keys())]

    candidates = []
    for no, key in preset_keys:
        preset_items = equip_json.get(key)
        if not (isinstance(preset_items, list) and preset_items):
            continue

        merged_items = _merge_items(base_items, preset_items)
        raw = json.dumps(merged_items, ensure_ascii=False)
        cnt = count_drop_rate_mentions(raw)
        # (drop_cnt, prefer2_flag, preset_no, merged_items)
        candidates.append((cnt, 0 if no == 2 else 1, no, merged_items))

    if not candidates:
        # 若 API 沒給 preset（或全空），避免誤刪
        return equip_json

    candidates.sort(key=lambda x: (x[0], x[1], x[2]))
    best_items = candidates[0][3]
    best_no = candidates[0][2]

    trimmed = dict(equip_json)
    for _no, key in preset_keys:
        trimmed.pop(key, None)

    trimmed["preset_no"] = best_no
    trimmed["item_equipment"] = best_items
    return trimmed



# -----------------------------
# DB low-level helpers
# -----------------------------
async def _get_ocid_from_db(db, character_name, world):
    async with db.execute("""
        SELECT ocid FROM characters
        WHERE character_name = ? AND world_name = ?
        LIMIT 1
    """, (character_name, world)) as cur:
        row = await cur.fetchone()
        return row[0] if row else None


async def _basic_updated_at(db, ocid):
    async with db.execute("""
        SELECT updated_at FROM characters
        WHERE ocid = ?
        LIMIT 1
    """, (ocid,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else None


async def _stat_date_exists(db, ocid, date_str):
    async with db.execute("""
        SELECT 1 FROM character_stats
        WHERE ocid = ? AND stat_date = ?
        LIMIT 1
    """, (ocid, date_str)) as cur:
        return (await cur.fetchone()) is not None


async def _equip_best_exists(db, ocid: str, best_equip_date: str) -> bool:
    async with db.execute("""
        SELECT 1
        FROM character_item_equipment_best
        WHERE ocid = ?
          AND best_equip_date = ?
        LIMIT 1
    """, (ocid, best_equip_date)) as cur:
        return (await cur.fetchone()) is not None


async def should_refresh_character(db, ocid: str, refresh_days: int) -> bool:
    updated_at = await _basic_updated_at(db, ocid)
    if not updated_at:
        return True
    return _days_since(updated_at) >= int(refresh_days)


# ============================================================
# A) 公會展開 -> character_list
# ============================================================
async def expand_guilds_to_character_list(api_key, guild_names, world, progress_cb):
    client = NexonClient(api_key, concurrency=3)
    errors = []
    total_added = 0

    async with aiohttp.ClientSession() as session, aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")

        for i, guild_name in enumerate(guild_names):
            progress_cb(i, guild_name)

            gid_res = await client.get_guild_id(session, guild_name, world)
            gid, _err = _unpack_api_result(gid_res)

            if not gid or not gid.get("oguild_id"):
                errors.append({"guild_name": guild_name, "reason": "無法取得 oguild_id"})
                continue

            oguild_id = gid["oguild_id"]

            gb_res = await client.get_guild_basic(session, oguild_id)
            gb, _err2 = _unpack_api_result(gb_res)

            members = (gb or {}).get("guild_member") or []
            if not members:
                errors.append({"guild_name": guild_name, "reason": "無法取得公會成員清單"})
                continue

            for name in members:
                name = str(name or "").strip()
                if not name:
                    continue
                cur = await db.execute("""
                    INSERT OR IGNORE INTO character_list (character_name, world_name)
                    VALUES (?, ?)
                """, (name, world))
                if cur.rowcount == 1:
                    total_added += 1

            await db.commit()

    return {"added": total_added, "errors": errors}


# ============================================================
# internal: retry wrapper (因 NexonClient 現在多數 method 只回 data，無法辨識 status)
# ============================================================
async def _call_with_retry(coro_factory, *, retries: int = 5, base_delay: float = 0.35):
    """
    coro_factory: lambda -> awaitable
    成功條件：回傳非 None（或 tuple[0] 非 None）
    """
    delay = base_delay
    for _ in range(max(1, int(retries))):
        try:
            res = await coro_factory()
            data, _err = _unpack_api_result(res)
            if data is not None:
                return res
        except Exception:
            pass
        await asyncio.sleep(delay)
        delay = min(delay * 2, 6.0)
    return None


# ============================================================
# B) 抓角色 basic/stat/equipment（併行加速版）
# ============================================================
async def update_characters(
    api_key: str,
    world: str,
    base_date: date,
    days_back: int,
    fetch_mode: str,
    refresh_days: int,
    only_expired: bool,
    force_refresh_all: bool,
    progress_cb,

    # 新增：裝備抓取可選功能
    equip_enabled: bool = True,
    equip_date_mode: str = "today",   # "today" | "date" | "range"
    equip_base_date: date | None = None,
    equip_days_back: int = 0,

    # 新增：併行參數
    max_workers: int = 50,
    max_in_flight: int = 60,
    db_batch: int = 500,
):
    """
    加速策略（適用 38,000+ 角色）：
    - 以「角色」為併行單位（worker pool）
    - DB 寫入集中到單一 writer task（避免 SQLite 寫鎖競爭）
    - 降低 commit 次數（db_batch 筆才 commit）
    """
    client = NexonClient(api_key, concurrency=3)
    end_day = base_date
    today_str = end_day.isoformat()

    # stat 日期區間（保留原本）
    stat_dates = [(end_day - timedelta(days=d)).isoformat() for d in range(int(days_back) + 1)]

    # equipment 日期清單（新的獨立規則）
    equip_dates: list[str | None] = []
    if equip_enabled:
        mode = (equip_date_mode or "today").strip().lower()
        if mode == "today":
            equip_dates = [None]  # 不帶 date
        elif mode == "date":
            d0 = (equip_base_date or end_day).isoformat()
            equip_dates = [d0]
        else:  # "range"
            ed = (equip_base_date or end_day)
            equip_dates = [(ed - timedelta(days=d)).isoformat() for d in range(int(equip_days_back) + 1)]

    # names 清單一次性讀出
    async with aiosqlite.connect(DB_PATH) as db_ro:
        await db_ro.execute("PRAGMA busy_timeout=30000;")
        async with db_ro.execute("""
            SELECT character_name
            FROM character_list
            WHERE world_name = ?
            ORDER BY character_name
        """, (world,)) as cur:
            names = [r[0] for r in await cur.fetchall()]

    # 併行設定
    max_workers = max(1, int(max_workers))
    max_in_flight = max(1, int(max_in_flight))
    db_batch = max(50, int(db_batch))

    # HTTP 連線池
    connector = aiohttp.TCPConnector(limit=0, limit_per_host=max(10, max_in_flight))
    timeout = aiohttp.ClientTimeout(total=25)

    # Queues
    name_q: asyncio.Queue[str | None] = asyncio.Queue()
    write_q: asyncio.Queue[tuple[str, tuple] | None] = asyncio.Queue(maxsize=10000)

    for nm in names:
        name_q.put_nowait(nm)

    # progress counter（併行下仍能順序顯示進度）
    done_counter = {"n": 0}
    done_lock = asyncio.Lock()

    async def writer_task():
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("PRAGMA busy_timeout=30000;")
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")

            pending = 0
            while True:
                item = await write_q.get()
                if item is None:
                    write_q.task_done()
                    break

                sql, params = item
                try:
                    await db.execute(sql, params)
                except Exception:
                    # 不中斷全局：避免單筆 SQL/型別問題卡死 38k 任務
                    pass

                pending += 1
                write_q.task_done()

                if pending >= db_batch:
                    await db.commit()
                    pending = 0

            if pending:
                await db.commit()

    async def worker_task(session: aiohttp.ClientSession):
        # 每個 worker 自己開一條 aiosqlite 連線做讀查（WAL 下可併行讀）
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("PRAGMA busy_timeout=30000;")
            await db.execute("PRAGMA journal_mode=WAL;")

            while True:
                name = await name_q.get()
                if name is None:
                    name_q.task_done()
                    break

                # progress（注意：progress_cb 仍用 (i,name) 形式）
                async with done_lock:
                    i = done_counter["n"]
                    done_counter["n"] += 1
                try:
                    progress_cb(i, name)
                except Exception:
                    pass

                try:
                    # 1) ocid
                    ocid = await _get_ocid_from_db(db, name, world)
                    if not ocid or force_refresh_all:
                        ocid_res = await _call_with_retry(lambda: client.get_ocid(session, name))
                        ocid_json, _err = _unpack_api_result(ocid_res)
                        if not ocid_json or not ocid_json.get("ocid"):
                            name_q.task_done()
                            continue
                        ocid = ocid_json["ocid"]

                    # 2) should refresh?
                    expired = True if force_refresh_all else await should_refresh_character(db, ocid, refresh_days)
                    if only_expired and not expired:
                        name_q.task_done()
                        continue

                    # 3) basic
                    updated_at = await _basic_updated_at(db, ocid)
                    basic_already_today = bool(updated_at and updated_at[:10] == today_str)

                    if force_refresh_all or expired or (not basic_already_today):
                        basic_res = await _call_with_retry(lambda: client.get_basic(session, ocid))
                        basic, _errb = _unpack_api_result(basic_res)
                        if basic:
                            await write_q.put((
                                """
                                INSERT INTO characters (
                                  ocid, character_name, world_name,
                                  character_gender, character_class, character_class_level,
                                  character_level, character_exp, character_exp_rate,
                                  character_guild_name, character_image, character_date_create,
                                  liberation_quest_clear, access_flag, updated_at
                                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                ON CONFLICT(ocid) DO UPDATE SET
                                  character_name=excluded.character_name,
                                  world_name=excluded.world_name,
                                  character_gender=excluded.character_gender,
                                  character_class=excluded.character_class,
                                  character_class_level=excluded.character_class_level,
                                  character_level=excluded.character_level,
                                  character_exp=excluded.character_exp,
                                  character_exp_rate=excluded.character_exp_rate,
                                  character_guild_name=excluded.character_guild_name,
                                  character_image=excluded.character_image,
                                  character_date_create=excluded.character_date_create,
                                  liberation_quest_clear=excluded.liberation_quest_clear,
                                  access_flag=excluded.access_flag,
                                  updated_at=excluded.updated_at
                                """,
                                (
                                    ocid,
                                    basic.get("character_name") or name,
                                    basic.get("world_name") or world,
                                    basic.get("character_gender"),
                                    basic.get("character_class"),
                                    int(basic.get("character_class_level") or 0),
                                    int(basic.get("character_level") or 0),
                                    int(basic.get("character_exp") or 0),
                                    float(basic.get("character_exp_rate") or 0),
                                    basic.get("character_guild_name"),
                                    basic.get("character_image"),
                                    basic.get("character_date_create"),
                                    int(basic.get("liberation_quest_clear") or 0),
                                    _as_access_flag(basic.get("access_flag")),
                                    datetime.now().isoformat(),
                                )
                            ))

                    # 4) stat（若 skip_existing 則保留存在檢查；但這裡只做讀查，不寫）
                    for ds in stat_dates:
                        if (not force_refresh_all) and fetch_mode == "skip_existing":
                            try:
                                if await _stat_date_exists(db, ocid, ds):
                                    continue
                            except Exception:
                                pass

                        stat_res = await _call_with_retry(lambda: client.get_stat(session, ocid, ds))
                        stat_json, _errs = _unpack_api_result(stat_res)
                        if stat_json:
                            for s in stat_json.get("final_stat", []):
                                stat_name = s.get("stat_name")
                                stat_value = safe_float(s.get("stat_value"))
                                if stat_name and stat_value is not None:
                                    await write_q.put((
                                        """
                                        INSERT OR REPLACE INTO character_stats
                                        (ocid, stat_date, stat_name, stat_value)
                                        VALUES (?,?,?,?)
                                        """,
                                        (ocid, ds, stat_name, stat_value)
                                    ))

                    # 5) equipment（只寫 best；skip_existing 以 best_equip_date 判斷）
                    if equip_dates:
                        for ed in equip_dates:
                            store_date = today_str if ed is None else ed

                            if (not force_refresh_all) and fetch_mode == "skip_existing":
                                try:
                                    if await _equip_best_exists(db, ocid, store_date):
                                        continue
                                except Exception:
                                    pass

                            equip_res = await _call_with_retry(lambda: client.get_item_equipment(session, ocid, ed))
                            equip_json, _erre = _unpack_api_result(equip_res)
                            if not equip_json:
                                continue

                            trimmed = pick_best_preset_by_lowest_drop_rate(equip_json)
                            raw = json.dumps(trimmed, ensure_ascii=False)
                            mention = count_drop_rate_mentions(raw)

                            await write_q.put((
                                """
                                INSERT INTO character_item_equipment_best
                                (ocid, best_equip_date, drop_rate_mentions, best_equipment_json, updated_at)
                                VALUES (?, ?, ?, ?, ?)
                                ON CONFLICT(ocid) DO UPDATE SET
                                  best_equip_date=excluded.best_equip_date,
                                  drop_rate_mentions=excluded.drop_rate_mentions,
                                  best_equipment_json=excluded.best_equipment_json,
                                  updated_at=excluded.updated_at
                                """,
                                (ocid, store_date, int(mention), raw, datetime.now().isoformat())
                            ))

                except Exception:
                    # 單一角色失敗不影響整體 38k 任務
                    pass
                finally:
                    name_q.task_done()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # 啟動 writer
        w = asyncio.create_task(writer_task())

        # 啟動 workers
        workers = [asyncio.create_task(worker_task(session)) for _ in range(max_workers)]

        # 送 None 給 workers 讓其結束
        for _ in range(max_workers):
            name_q.put_nowait(None)

        await name_q.join()
        await write_q.join()

        # 關閉 writer
        await write_q.put(None)
        await w

        await asyncio.gather(*workers, return_exceptions=True)


# ============================================================
# C) 完整性檢查
# ============================================================
async def check_character_completeness(world: str, start_date: str, end_date: str, progress_cb):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")

        async with db.execute("""
            SELECT ocid, character_name
            FROM characters
            WHERE world_name = ?
            ORDER BY character_name
        """, (world,)) as cur:
            rows = await cur.fetchall()

        missing = []
        total = len(rows)

        for idx, (ocid, name) in enumerate(rows):
            progress_cb(idx, name)
            async with db.execute("""
                SELECT 1
                FROM character_stats
                WHERE ocid = ?
                  AND stat_date BETWEEN ? AND ?
                  AND stat_name = '戰鬥力'
                LIMIT 1
            """, (ocid, start_date, end_date)) as cur2:
                ok = await cur2.fetchone()
            if not ok:
                missing.append({"ocid": ocid, "character_name": name})

        return {"total": total, "missing": missing}


# ============================================================
# D) optimize_data
# ============================================================
async def optimize_data(world: str, start_date: str, end_date: str, progress_cb):
    """
    注意：若你已採用 update_characters 的新裝備策略（只寫 best，不寫歷史表）
    這裡的裝備歷史最佳化會抓不到資料而自然略過；保留不影響執行。
    """
    report = {"power_deleted_rows": 0, "equip_deleted_rows": 0, "power_kept": [], "equip_kept": []}

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")
        await db.execute("PRAGMA journal_mode=WAL;")

        async with db.execute("""
            SELECT ocid, character_name
            FROM characters
            WHERE world_name = ?
            ORDER BY character_name
        """, (world,)) as cur:
            chars = await cur.fetchall()

        for idx, (ocid, cname) in enumerate(chars):
            progress_cb(idx, cname)

            # 戰鬥力：保留區間最大值
            async with db.execute("""
                SELECT stat_date, stat_value
                FROM character_stats
                WHERE ocid = ?
                  AND stat_name='戰鬥力'
                  AND stat_date BETWEEN ? AND ?
                ORDER BY stat_value DESC, stat_date DESC
                LIMIT 1
            """, (ocid, start_date, end_date)) as cur2:
                best = await cur2.fetchone()

            if best:
                best_date, best_power = best
                report["power_kept"].append({
                    "ocid": ocid,
                    "character_name": cname,
                    "best_power": int(best_power),
                    "best_date": best_date
                })

                cur3 = await db.execute("""
                    DELETE FROM character_stats
                    WHERE ocid = ?
                      AND stat_name='戰鬥力'
                      AND stat_date BETWEEN ? AND ?
                      AND NOT (stat_date=? AND stat_value=?)
                """, (ocid, start_date, end_date, best_date, best_power))
                report["power_deleted_rows"] += cur3.rowcount or 0

                await db.execute("""
                    INSERT INTO character_power_summary (ocid, power_best, power_best_date, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(ocid) DO UPDATE SET
                      power_best=excluded.power_best,
                      power_best_date=excluded.power_best_date,
                      updated_at=excluded.updated_at
                """, (ocid, int(best_power), best_date, datetime.now().isoformat()))

            # 裝備歷史表：挑「道具掉落率最少」那天（ASC）
            async with db.execute("""
                SELECT equip_date, drop_rate_mentions, raw_json
                FROM character_item_equipment
                WHERE ocid = ?
                  AND equip_date BETWEEN ? AND ?
                ORDER BY drop_rate_mentions ASC, equip_date DESC
                LIMIT 1
            """, (ocid, start_date, end_date)) as cur4:
                ebest = await cur4.fetchone()

            if ebest:
                best_equip_date, best_drop, best_raw = ebest
                report["equip_kept"].append({
                    "ocid": ocid,
                    "character_name": cname,
                    "best_equip_date": best_equip_date,
                    "drop_rate_mentions": int(best_drop or 0)
                })

                await db.execute("""
                    INSERT INTO character_item_equipment_best
                    (ocid, best_equip_date, drop_rate_mentions, best_equipment_json, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(ocid) DO UPDATE SET
                      best_equip_date=excluded.best_equip_date,
                      drop_rate_mentions=excluded.drop_rate_mentions,
                      best_equipment_json=excluded.best_equipment_json,
                      updated_at=excluded.updated_at
                """, (ocid, best_equip_date, int(best_drop or 0), best_raw, datetime.now().isoformat()))

                cur5 = await db.execute("""
                    DELETE FROM character_item_equipment
                    WHERE ocid = ?
                      AND equip_date BETWEEN ? AND ?
                      AND NOT (equip_date=?)
                """, (ocid, start_date, end_date, best_equip_date))
                report["equip_deleted_rows"] += cur5.rowcount or 0

            if idx % 50 == 0:
                await db.commit()

        await db.commit()

    return report


# ============================================================
# E) 角色清單健檢：抓不到 ocid/basic、access_flag、改名修正、剔除歷史資料
# ============================================================
async def validate_and_purge_character_list(
    api_key: str,
    world: str,
    *,
    dry_run: bool = True,
    treat_access_flag_false_as_invalid: bool = True,
    auto_fix_rename: bool = True,
    purge_invalid_from_db: bool = True,
    delete_equip_history: bool = True,
    progress_cb=None,  # progress_cb(i, total, name)
) -> dict:
    client = NexonClient(api_key, concurrency=3)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")
        async with db.execute("""
            SELECT character_name
            FROM character_list
            WHERE world_name = ?
            ORDER BY character_name
        """, (world,)) as cur:
            names = [r[0] for r in await cur.fetchall()]

    total = len(names)
    invalid = []
    renamed = []
    ok = 0

    async with aiohttp.ClientSession() as session, aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")
        await db.execute("PRAGMA journal_mode=WAL;")

        for i, name in enumerate(names):
            if progress_cb:
                progress_cb(i, total, name)

            ocid_res = await client.get_ocid(session, name)
            ocid_json, err = _unpack_api_result(ocid_res)
            if err or not ocid_json or not ocid_json.get("ocid"):
                invalid.append({"character_name": name, "reason": f"ocid_not_found:{err or 'unknown'}"})
                continue

            ocid = ocid_json["ocid"]

            basic_res = await client.get_basic(session, ocid)
            basic_json, errb = _unpack_api_result(basic_res)
            if errb or not basic_json:
                invalid.append({"character_name": name, "ocid": ocid, "reason": f"basic_not_found:{errb or 'unknown'}"})
                continue

            access_flag = _as_access_flag(basic_json.get("access_flag"))
            if treat_access_flag_false_as_invalid and access_flag != 1:
                invalid.append({"character_name": name, "ocid": ocid, "reason": "access_flag_false"})
                continue

            new_name = (basic_json.get("character_name") or "").strip()
            if auto_fix_rename and new_name and new_name != name:
                renamed.append({"old_name": name, "new_name": new_name, "ocid": ocid})

                if not dry_run:
                    await db.execute("DELETE FROM character_list WHERE world_name=? AND character_name=?", (world, name))
                    await db.execute("INSERT OR IGNORE INTO character_list (character_name, world_name) VALUES (?, ?)", (new_name, world))
                    await db.execute(
                        "UPDATE characters SET character_name=?, world_name=?, updated_at=? WHERE ocid=?",
                        (new_name, world, datetime.now().isoformat(), ocid),
                    )

            ok += 1

        if not dry_run and invalid:
            await db.executemany(
                "DELETE FROM character_list WHERE world_name=? AND character_name=?",
                [(world, x["character_name"]) for x in invalid]
            )

            if purge_invalid_from_db:
                ocids = [x.get("ocid") for x in invalid if x.get("ocid")]

                missing_name = [x["character_name"] for x in invalid if not x.get("ocid")]
                if missing_name:
                    for chunk in _chunked(missing_name, 500):
                        ph = ",".join(["?"] * len(chunk))
                        async with db.execute(
                            f"SELECT ocid FROM characters WHERE world_name=? AND character_name IN ({ph})",
                            [world] + chunk
                        ) as cur2:
                            rows = await cur2.fetchall()
                            ocids.extend([r[0] for r in rows if r and r[0]])

                ocids = sorted(set([o for o in ocids if o]))
                for chunk in _chunked(ocids, 500):
                    ph = ",".join(["?"] * len(chunk))
                    await db.execute(f"DELETE FROM character_stats WHERE ocid IN ({ph})", chunk)
                    await db.execute(f"DELETE FROM character_power_summary WHERE ocid IN ({ph})", chunk)
                    await db.execute(f"DELETE FROM character_item_equipment_best WHERE ocid IN ({ph})", chunk)
                    if delete_equip_history:
                        await db.execute(f"DELETE FROM character_item_equipment WHERE ocid IN ({ph})", chunk)
                    await db.execute(f"DELETE FROM characters WHERE ocid IN ({ph})", chunk)

            await db.commit()

    return {
        "world": world,
        "dry_run": dry_run,
        "total": total,
        "ok": ok,
        "invalid_count": len(invalid),
        "renamed_count": len(renamed),
        "invalid": invalid[:5000],
        "renamed": renamed[:5000],
        "note": "invalid/renamed 僅回傳前 5000 筆避免 UI 過大"
    }


# ============================================================
# F) 黑名單套用（skip / purge）
# ============================================================
async def purge_or_skip_blacklisted(
    world: str,
    blacklisted_names: set[str],
    *,
    mode: str = "skip",
    delete_equip_history: bool = True,
    return_names_limit: int = 3000,
) -> dict:
    mode = (mode or "").strip().lower()
    if mode not in ("skip", "purge"):
        raise ValueError("mode must be 'skip' or 'purge'")

    if not blacklisted_names:
        return {"world": world, "matched": 0, "mode": mode, "deleted": 0, "skipped": 0, "matched_names_sample": []}

    now = datetime.now().isoformat()
    blacklisted_list = sorted({(x or "").strip() for x in blacklisted_names if (x or "").strip()})
    if not blacklisted_list:
        return {"world": world, "matched": 0, "mode": mode, "deleted": 0, "skipped": 0, "at": now, "matched_names_sample": []}

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")
        await db.execute("PRAGMA journal_mode=WAL;")

        matched_names = []
        for chunk in _chunked(blacklisted_list, 500):
            ph = ",".join(["?"] * len(chunk))
            async with db.execute(
                f"SELECT character_name FROM character_list WHERE world_name=? AND character_name IN ({ph})",
                [world] + chunk
            ) as cur:
                rows = await cur.fetchall()
                matched_names.extend([r[0] for r in rows])

        matched_names = sorted(set(matched_names))
        if not matched_names:
            return {"world": world, "matched": 0, "mode": mode, "deleted": 0, "skipped": 0, "at": now, "matched_names_sample": []}

        await db.executemany(
            "DELETE FROM character_list WHERE world_name=? AND character_name=?",
            [(world, nm) for nm in matched_names]
        )

        sample = matched_names[:max(0, int(return_names_limit or 0))]

        if mode == "skip":
            await db.commit()
            return {"world": world, "matched": len(matched_names), "mode": mode, "deleted": 0, "skipped": len(matched_names), "at": now, "matched_names_sample": sample}

        ocids = []
        for chunk in _chunked(matched_names, 500):
            ph = ",".join(["?"] * len(chunk))
            async with db.execute(
                f"SELECT ocid FROM characters WHERE world_name=? AND character_name IN ({ph})",
                [world] + chunk
            ) as cur:
                rows = await cur.fetchall()
                ocids.extend([r[0] for r in rows if r and r[0]])

        ocids = sorted(set(ocids))
        for chunk in _chunked(ocids, 500):
            ph = ",".join(["?"] * len(chunk))
            await db.execute(f"DELETE FROM character_stats WHERE ocid IN ({ph})", chunk)
            await db.execute(f"DELETE FROM character_power_summary WHERE ocid IN ({ph})", chunk)
            await db.execute(f"DELETE FROM character_item_equipment_best WHERE ocid IN ({ph})", chunk)
            if delete_equip_history:
                await db.execute(f"DELETE FROM character_item_equipment WHERE ocid IN ({ph})", chunk)
            await db.execute(f"DELETE FROM characters WHERE ocid IN ({ph})", chunk)

        await db.commit()
        return {"world": world, "matched": len(matched_names), "mode": mode, "deleted": len(matched_names), "skipped": 0, "at": now, "matched_names_sample": sample}


# ============================================================
# G) blacklist sync log：對外 API（給 app.py import）
# ============================================================
async def _ensure_blacklist_sync_log_table():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS blacklist_sync_log (
          world_name TEXT PRIMARY KEY,
          server_name TEXT,
          server_id TEXT,
          last_success_date TEXT,
          updated_at TEXT
        )
        """)
        await db.commit()


async def get_blacklist_last_sync_date(world: str) -> str | None:
    await _ensure_blacklist_sync_log_table()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")
        async with db.execute("SELECT last_success_date FROM blacklist_sync_log WHERE world_name=?", (world,)) as cur:
            row = await cur.fetchone()
            return row[0] if row and row[0] else None


async def set_blacklist_last_sync_date(world: str, server_name: str, server_id: str, last_date_iso: str):
    await _ensure_blacklist_sync_log_table()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")
        await db.execute("""
            INSERT INTO blacklist_sync_log (world_name, server_name, server_id, last_success_date, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(world_name) DO UPDATE SET
              server_name=excluded.server_name,
              server_id=excluded.server_id,
              last_success_date=excluded.last_success_date,
              updated_at=excluded.updated_at
        """, (world, server_name, server_id, last_date_iso, datetime.now().isoformat()))
        await db.commit()


# ============================================================
# H) 官網可抓到的最新日期（06:00 規則）
# ============================================================
def _latest_available_block_date() -> date:
    now = datetime.now()
    cutoff = datetime.combine(now.date(), dtime(6, 0, 0))
    if now < cutoff:
        return now.date() - timedelta(days=2)
    return now.date() - timedelta(days=1)


# ============================================================
# I) sync_blacklist_incremental（供 app.py 使用）
# ============================================================
async def sync_blacklist_incremental(
    world: str,
    *,
    server_name: str = "挑戰者",
    first_start_date: date = date(2025, 12, 3),
    mode: str = "skip",  # "skip" or "purge"
    delete_equip_history: bool = True,
    progress_cb=None,    # progress_cb(done, total, message)
    permanent_only: bool = True,
    return_names_limit: int = 3000,
) -> dict:
    try:
        from blacklist_fetcher import BlacklistClient, extract_names, extract_permanent_names
    except Exception as e:
        return {"world": world, "server": server_name, "ran": False, "reason": f"blacklist_fetcher import failed: {repr(e)}"}

    latest = _latest_available_block_date()
    if latest < first_start_date:
        return {"world": world, "server": server_name, "ran": False, "reason": "latest < first_start_date"}

    last = await get_blacklist_last_sync_date(world)
    start = (date.fromisoformat(last) + timedelta(days=1)) if last else first_start_date

    if start > latest:
        return {"world": world, "server": server_name, "ran": False, "reason": "up-to-date", "start": start.isoformat(), "latest": latest.isoformat()}

    cli = BlacklistClient(server_name=server_name)
    csrf, server_id = cli.init()

    total_days = (latest - start).days + 1
    done_days = 0

    scanned_total_names = 0
    scanned_effective_names = 0
    applied_matched_total = 0
    matched_names_union = []

    cur_day = start
    while cur_day <= latest:
        done_days += 1
        block_date = cur_day.strftime("%Y/%m/%d")

        if progress_cb:
            progress_cb(done_days, total_days, f"抓取官網：{block_date}")

        rows = cli.fetch_all_for_date(block_date, sleep_sec=0.25)

        all_names = extract_names(rows)
        scanned_total_names += len(all_names)

        if permanent_only:
            names = extract_permanent_names(rows)
        else:
            names = all_names

        scanned_effective_names += len(names)

        if names:
            out = await purge_or_skip_blacklisted(
                world=world,
                blacklisted_names=set(names),
                mode=mode,
                delete_equip_history=delete_equip_history,
                return_names_limit=return_names_limit,
            )
            applied_matched_total += int(out.get("matched") or 0)

            sample = out.get("matched_names_sample") or []
            if sample and len(matched_names_union) < return_names_limit:
                remain = return_names_limit - len(matched_names_union)
                matched_names_union.extend(sample[:remain])

        await set_blacklist_last_sync_date(world, server_name, server_id, cur_day.isoformat())
        cur_day += timedelta(days=1)

    matched_names_union = sorted(set(matched_names_union))

    return {
        "world": world,
        "server": server_name,
        "server_id": server_id,
        "ran": True,
        "start": start.isoformat(),
        "end": latest.isoformat(),
        "total_days": total_days,
        "mode": mode,
        "permanent_only": permanent_only,
        "scanned_total_names": scanned_total_names,
        "scanned_effective_names": scanned_effective_names,
        "applied_matched_total": applied_matched_total,
        "matched_names_sample": matched_names_union,
        "matched_names_sample_limit": return_names_limit,
    }
