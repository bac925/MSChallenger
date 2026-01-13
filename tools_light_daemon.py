import asyncio
import aiohttp
from datetime import date, datetime, timedelta
from pathlib import Path
import traceback

from nexon_api import NexonClient
from services import extract_power
from db import DB_PATH
import aiosqlite


# =========================================================
# 🔧 可手動調整的參數（你只需要改這裡）
# =========================================================
WORLD = "挑戰者"

# 每次更新間隔（分鐘）
REFRESH_INTERVAL_MINUTES = 5

# 只有等級 >= 270 才抓戰鬥力
STAT_MIN_LEVEL = 270

# （可選）basic 也只更新到某個等級以上（想追上位玩家請設定，例如 260/265/270）
# 設為 0 表示全跑（不建議，會很慢）
BASIC_MIN_LEVEL = 0  # 建議改成 260 或 270

# API 並行數（建議先 20~80 測；100 也可以，但仍建議循序加）
API_CONCURRENCY = 40

# 倒數顯示頻率（秒）
COUNTDOWN_PRINT_EVERY_SECONDS = 10
# =========================================================


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def load_api_key() -> str:
    p = Path(__file__).resolve().parent / "apikey.txt"
    if not p.exists():
        raise RuntimeError("找不到 apikey.txt")
    key = p.read_text(encoding="utf-8").strip()
    if not key:
        raise RuntimeError("apikey.txt 為空")
    return key


def fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def to_int01(v) -> int:
    """
    將 "0"/"1"/0/1/True/False/None 等，穩定轉成 0 或 1
    """
    try:
        iv = int(v)
    except (TypeError, ValueError):
        return 0
    return 1 if iv == 1 else 0


async def bootstrap_new_characters(
    api_key: str,
    world: str,
    limit_per_cycle: int = 500,
    concurrency: int = 20,
):
    """
    目的：
      - 把 character_list 裡新加入、但 characters 尚無 ocid 的角色補齊
      - 只寫入 characters 的最低必要欄位，讓後續更新流程能納入
    """
    client = NexonClient(api_key)
    sem = asyncio.Semaphore(concurrency)
    now_iso = datetime.now().isoformat()

    async with aiosqlite.connect(DB_PATH) as db, aiohttp.ClientSession() as session:
        await db.execute("PRAGMA busy_timeout=10000;")

        # 找出名單中但 characters 沒 ocid 的角色（限量避免一次爆量）
        async with db.execute(
            """
            SELECT cl.character_name
            FROM character_list cl
            LEFT JOIN characters c
              ON c.character_name = cl.character_name
             AND c.world_name = cl.world_name
            WHERE cl.world_name = ?
              AND (c.ocid IS NULL OR c.ocid = '')
            ORDER BY cl.character_name
            LIMIT ?
            """,
            (world, int(limit_per_cycle)),
        ) as cur:
            names = [r[0] for r in await cur.fetchall()]

        if not names:
            print(f"[{fmt(datetime.now())}] Bootstrap：無需補齊新角色", flush=True)
            return 0

        print(f"[{fmt(datetime.now())}] Bootstrap：需補齊 {len(names)} 名新角色（本輪上限 {limit_per_cycle}）", flush=True)

        async def one(name: str):
            async with sem:
                # 1) 先拿 ocid
                ocid_json = await client.get_ocid(session, name)
                ocid = (ocid_json or {}).get("ocid")
                if not ocid:
                    return (name, None, None, None)

                # 2) 拿 basic（至少拿 level/class）
                basic = await client.get_basic(session, ocid) or {}
                raw_lvl = basic.get("character_level")
                try:
                    lvl = int(raw_lvl)
                except (TypeError, ValueError):
                    lvl = 0

                job = basic.get("character_class")
                return (name, ocid, lvl, job)

        results = await asyncio.gather(*(one(n) for n in names), return_exceptions=True)

        inserted = 0
        for r in results:
            if not isinstance(r, tuple) or len(r) < 4:
                continue

            name, ocid, lvl, job = r[0], r[1], r[2], r[3]

            # ocid 拿不到就跳過
            if not ocid:
                continue

            try:
                lvl_int = int(lvl)
            except (TypeError, ValueError):
                lvl_int = 0

            await db.execute(
                """
                INSERT INTO characters (
                    ocid, character_name, world_name,
                    character_level, character_class,
                    updated_at
                ) VALUES (?,?,?,?,?,?)
                ON CONFLICT(ocid) DO UPDATE SET
                    character_name=excluded.character_name,
                    world_name=excluded.world_name,
                    character_level=MAX(COALESCE(characters.character_level,0), excluded.character_level),
                    character_class=COALESCE(excluded.character_class, characters.character_class),
                    updated_at=excluded.updated_at
                """,
                (ocid, name, world, lvl_int, job, now_iso),
            )
            inserted += 1

        await db.commit()
        print(f"[{fmt(datetime.now())}] Bootstrap：已補齊 {inserted} 名新角色", flush=True)
        return inserted


# =========================================================
# ① 單一角色：basic（所有人）＋ stat（270+）
#    ✅ 覆蓋式：每次都回傳 liberation_int(0/1)
# =========================================================
async def fetch_one(
    sem: asyncio.Semaphore,
    client: NexonClient,
    session: aiohttp.ClientSession,
    ocid: str,
    name: str,
    old_level: int,
    target_date: date,
):
    async with sem:
        out = {
            "name": name,
            "ocid": ocid,
            "old_level": old_level,
            "new_level": None,
            "new_power": None,
            "liberation_int": None,  # ✅ 0/1（覆蓋式）
        }

        # ── basic（每輪都抓） ──
        basic = await client.get_basic(session, ocid)
        if not basic:
            return out

        try:
            new_level = int(basic.get("character_level") or 0)
        except (TypeError, ValueError):
            new_level = 0

        out["new_level"] = new_level

        # ✅ API 目前回的是 "0"/"1"（字串），這裡統一轉 0/1
        out["liberation_int"] = to_int01(basic.get("liberation_quest_clear"))

        # ── stat（只在達到門檻才抓） ──
        if new_level >= STAT_MIN_LEVEL:
            stat = await client.get_stat(session, ocid)
            if stat:
                out["new_power"] = extract_power(stat)

        return out


# =========================================================
# ② 單一 DB Writer（避免 SQLite 鎖）
#    ✅ 覆蓋式：liberation_quest_clear 新舊不同就寫回 0/1
# =========================================================
async def write_results(results, target_date: date):
    ds = target_date.isoformat()
    now_iso = datetime.now().isoformat()

    updated_level = 0
    updated_power = 0
    updated_liberation = 0

    ocids = [r["ocid"] for r in results if isinstance(r, dict) and r.get("ocid")]

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=10000;")

        # ── 1) 一次性抓「今日戰鬥力最高」 ──
        old_power_today = {}
        for batch in chunked(ocids, 400):
            placeholders = ",".join(["?"] * len(batch))
            async with db.execute(
                f"""
                SELECT ocid, stat_value
                FROM character_stats
                WHERE stat_date = ?
                  AND stat_name = '戰鬥力'
                  AND ocid IN ({placeholders})
                """,
                (ds, *batch),
            ) as cur:
                rows = await cur.fetchall()

            for ocid, val in rows:
                try:
                    old_power_today[ocid] = float(val)
                except Exception:
                    pass

        # ── 2) 一次性抓 liberation 舊值（0/1） ──
        old_liberation = {}
        for batch in chunked(ocids, 400):
            placeholders = ",".join(["?"] * len(batch))
            async with db.execute(
                f"""
                SELECT ocid, liberation_quest_clear
                FROM characters
                WHERE ocid IN ({placeholders})
                """,
                batch,
            ) as cur:
                rows = await cur.fetchall()

            for ocid, val in rows:
                old_liberation[ocid] = to_int01(val)

        # ── 3) 寫入迴圈 ──
        for r in results:
            if not isinstance(r, dict):
                continue

            ocid = r.get("ocid")
            name = r.get("name", "")
            if not ocid:
                continue

            # 等級：只升不降
            if r.get("new_level") and r["new_level"] > (r.get("old_level") or 0):
                await db.execute(
                    """
                    UPDATE characters
                    SET character_level=?, updated_at=?
                    WHERE ocid=?
                    """,
                    (int(r["new_level"]), now_iso, ocid),
                )
                updated_level += 1
                print(f"    ✔ {name} 等級↑ {r.get('old_level', 0)} → {r['new_level']}", flush=True)

            # ✅ 解放：覆蓋式（新舊不同就寫 0/1）
            new_li = r.get("liberation_int")
            if new_li is not None:
                new_li = to_int01(new_li)
                old_li = old_liberation.get(ocid, 0)

                if new_li != old_li:
                    await db.execute(
                        """
                        UPDATE characters
                        SET liberation_quest_clear=?, updated_at=?
                        WHERE ocid=?
                        """,
                        (new_li, now_iso, ocid),
                    )
                    old_liberation[ocid] = new_li
                    updated_liberation += 1
                    print(f"    🔓 {name} 解放狀態變更：{old_li} → {new_li}", flush=True)

            # 戰鬥力：同日只留最高
            if r.get("new_power") is not None:
                try:
                    new_p = float(r["new_power"])
                except Exception:
                    new_p = None

                if new_p is not None:
                    old_p = old_power_today.get(ocid)
                    if (old_p is None) or (new_p > old_p):
                        await db.execute(
                            """
                            INSERT OR REPLACE INTO character_stats
                            (ocid, stat_date, stat_name, stat_value)
                            VALUES (?,?,?,?)
                            """,
                            (ocid, ds, "戰鬥力", new_p),
                        )
                        old_power_today[ocid] = new_p
                        updated_power += 1
                        print(
                            f"    ★ {name} 戰鬥力更新："
                            f"{int(old_p) if old_p is not None else 'None'} → {int(new_p)}",
                            flush=True,
                        )

        await db.commit()

    return updated_level, updated_power, updated_liberation


# =========================================================
# ③ 跑一整輪（含進度顯示）
# =========================================================
async def run_one_cycle(api_key: str):
    target_date = date.today()
    start = datetime.now()

    print("=" * 72, flush=True)
    print(f"[{fmt(start)}] 開始更新｜世界={WORLD}", flush=True)

    # 先補齊新角色（character_list 有，但 characters 沒 ocid）
    await bootstrap_new_characters(
        api_key=api_key,
        world=WORLD,
        limit_per_cycle=500,
        concurrency=20
    )

    if BASIC_MIN_LEVEL > 0:
        print(f"[{fmt(start)}] 本輪 basic 只更新等級 >= {BASIC_MIN_LEVEL}（stat >= {STAT_MIN_LEVEL}）", flush=True)
    else:
        print(f"[{fmt(start)}] 本輪 basic 更新全部角色（stat >= {STAT_MIN_LEVEL}）", flush=True)

    sem = asyncio.Semaphore(API_CONCURRENCY)
    client = NexonClient(api_key)

    # 一次 SQL 拉完（避免逐筆 await 查 DB 卡住）
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=10000;")

        if BASIC_MIN_LEVEL > 0:
            sql = """
                SELECT ocid, character_name, COALESCE(character_level, 0) AS character_level
                FROM characters
                WHERE world_name = ?
                  AND ocid IS NOT NULL
                  AND COALESCE(character_level, 0) >= ?
                ORDER BY character_name
            """
            params = (WORLD, int(BASIC_MIN_LEVEL))
        else:
            sql = """
                SELECT ocid, character_name, COALESCE(character_level, 0) AS character_level
                FROM characters
                WHERE world_name = ?
                  AND ocid IS NOT NULL
                ORDER BY character_name
            """
            params = (WORLD,)

        async with db.execute(sql, params) as cur:
            rows = await cur.fetchall()

    total = len(rows)
    print(f"[{fmt(datetime.now())}] 角色載入完成｜可用角色數={total}", flush=True)
    if total == 0:
        print(f"[{fmt(datetime.now())}] 無可用角色，結束本輪。", flush=True)
        return

    results = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for row in rows:
            ocid = row[0]
            name = row[1]
            old_level = row[2] if len(row) > 2 and row[2] is not None else 0

            tasks.append(
                fetch_one(
                    sem, client, session,
                    ocid, name, int(old_level), target_date
                )
            )

        for idx, coro in enumerate(asyncio.as_completed(tasks), start=1):
            r = await coro
            results.append(r)

            flags = []
            if r.get("new_level") and r["new_level"] > (r.get("old_level") or 0):
                flags.append("等級↑")
            if r.get("new_power") is not None:
                flags.append("戰鬥力")
            # 覆蓋式：每次有 basic 就會有 liberation_int（0/1），但是否寫入取決於新舊是否不同
            if r.get("liberation_int") is not None:
                flags.append(f"解放={to_int01(r.get('liberation_int'))}")

            tail = " / ".join(flags) if flags else "無變動"
            print(f"  - ({idx:>5}/{total:<5}) {r.get('name','')}｜{tail}", flush=True)

    ul, up, ulib = await write_results(results, target_date)

    end = datetime.now()
    print(
        f"[{fmt(end)}] 完成｜耗時={(end-start).total_seconds():.1f}s｜"
        f"等級更新={ul}｜戰鬥力更新={up}｜解放變更={ulib}",
        flush=True
    )


# =========================================================
# ④ 常駐主循環（含倒數顯示）
# =========================================================
async def main():
    api_key = load_api_key()

    while True:
        try:
            await run_one_cycle(api_key)
        except Exception:
            print(f"[{fmt(datetime.now())}] [ERROR] 發生例外，完整 traceback 如下：", flush=True)
            traceback.print_exc()

        interval = max(1, int(REFRESH_INTERVAL_MINUTES))
        next_run = datetime.now() + timedelta(minutes=interval)
        print(f"[{fmt(datetime.now())}] 進入等待｜下次更新：{fmt(next_run)}", flush=True)

        while True:
            now = datetime.now()
            if now >= next_run:
                break
            remain = int((next_run - now).total_seconds())
            mm, ss = divmod(remain, 60)
            hh, mm = divmod(mm, 60)
            print(f"[{fmt(now)}] 倒數 {hh:02d}:{mm:02d}:{ss:02d}", flush=True)
            await asyncio.sleep(min(COUNTDOWN_PRINT_EVERY_SECONDS, remain))


if __name__ == "__main__":
    asyncio.run(main())
