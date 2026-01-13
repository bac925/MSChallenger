import asyncio
import aiosqlite

DB_PATH = "maple.db"


async def _exec_retry(db: aiosqlite.Connection, sql: str, params=None, retries: int = 8):
    """
    用於不需要回傳資料的 SQL（DDL/DML/PRAGMA 等）。
    重點：execute 產生的 cursor 一定要 close，否則 VACUUM 會報 SQL statements in progress。
    """
    if params is None:
        params = ()
    last_err = None

    for i in range(retries):
        cur = None
        try:
            cur = await db.execute(sql, params)
            await cur.close()
            return
        except aiosqlite.OperationalError as e:
            last_err = e
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                await asyncio.sleep(0.25 * (i + 1))
                continue
            raise
        finally:
            try:
                if cur is not None:
                    await cur.close()
            except Exception:
                pass

    raise last_err


async def _get_table_columns(db: aiosqlite.Connection, table_name: str) -> set[str]:
    async with db.execute(f"PRAGMA table_info({table_name});") as cur:
        rows = await cur.fetchall()
    return {row[1] for row in rows}


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await _exec_retry(db, "PRAGMA busy_timeout=10000;")
        await _exec_retry(db, "PRAGMA journal_mode=WAL;")
        await _exec_retry(db, "PRAGMA synchronous=NORMAL;")
        await _exec_retry(db, "PRAGMA temp_store=MEMORY;")

        # === guild_list ===
        await _exec_retry(db, """
        CREATE TABLE IF NOT EXISTS guild_list (
          guild_name TEXT,
          world_name TEXT,
          PRIMARY KEY (guild_name, world_name)
        )
        """)

        # === character_list ===
        await _exec_retry(db, """
        CREATE TABLE IF NOT EXISTS character_list (
          character_name TEXT,
          world_name TEXT,
          PRIMARY KEY (character_name, world_name)
        )
        """)

        # === characters ===
        await _exec_retry(db, """
        CREATE TABLE IF NOT EXISTS characters (
          ocid TEXT PRIMARY KEY,
          character_name TEXT,
          world_name TEXT,
          character_gender TEXT,
          character_class TEXT,
          character_class_level INTEGER,
          character_level INTEGER,
          character_exp INTEGER,
          character_exp_rate REAL,
          character_guild_name TEXT,
          character_image TEXT,
          character_date_create TEXT,
          liberation_quest_clear INTEGER,
          access_flag INTEGER,
          updated_at TEXT
        )
        """)

        # === stats ===
        await _exec_retry(db, """
        CREATE TABLE IF NOT EXISTS character_stats (
          ocid TEXT,
          stat_date TEXT,
          stat_name TEXT,
          stat_value REAL,
          PRIMARY KEY (ocid, stat_date, stat_name)
        )
        """)

        await _exec_retry(db, """
        CREATE INDEX IF NOT EXISTS idx_stats_name_date
        ON character_stats (stat_name, stat_date)
        """)
        await _exec_retry(db, """
        CREATE INDEX IF NOT EXISTS idx_stats_ocid_date
        ON character_stats (ocid, stat_date)
        """)

        # === power summary ===
        await _exec_retry(db, """
        CREATE TABLE IF NOT EXISTS character_power_summary (
          ocid TEXT PRIMARY KEY,
          power_best INTEGER,
          power_best_date TEXT,
          power_today INTEGER,
          power_yesterday INTEGER,
          updated_at TEXT
        )
        """)

        # === equip history（保留相容，但可選擇不再寫入以避免膨脹） ===
        await _exec_retry(db, """
        CREATE TABLE IF NOT EXISTS character_item_equipment (
          ocid TEXT,
          equip_date TEXT,
          drop_rate_mentions INTEGER,
          raw_json TEXT,
          fetched_at TEXT,
          PRIMARY KEY (ocid, equip_date)
        )
        """)

        # migration for history table
        cols = await _get_table_columns(db, "character_item_equipment")
        if "drop_rate_mentions" not in cols:
            await _exec_retry(db, "ALTER TABLE character_item_equipment ADD COLUMN drop_rate_mentions INTEGER DEFAULT 0;")
            await _exec_retry(db, "UPDATE character_item_equipment SET drop_rate_mentions = 0 WHERE drop_rate_mentions IS NULL;")
        if "raw_json" not in cols:
            await _exec_retry(db, "ALTER TABLE character_item_equipment ADD COLUMN raw_json TEXT;")
        if "fetched_at" not in cols:
            await _exec_retry(db, "ALTER TABLE character_item_equipment ADD COLUMN fetched_at TEXT;")

        await _exec_retry(db, """
        CREATE INDEX IF NOT EXISTS idx_equip_ocid_date
        ON character_item_equipment (ocid, equip_date)
        """)
        await _exec_retry(db, """
        CREATE INDEX IF NOT EXISTS idx_equip_drop_rate
        ON character_item_equipment (drop_rate_mentions)
        """)

        # === equip BEST（只保留一筆，避免 DB 膨脹）===
        await _exec_retry(db, """
        CREATE TABLE IF NOT EXISTS character_item_equipment_best (
          ocid TEXT PRIMARY KEY,
          best_equip_date TEXT,
          drop_rate_mentions INTEGER,
          best_equipment_json TEXT,
          updated_at TEXT
        )
        """)
        await _exec_retry(db, """
        CREATE INDEX IF NOT EXISTS idx_equip_best_drop_rate
        ON character_item_equipment_best (drop_rate_mentions)
        """)

        
        # === equip items (from BEST) for fast statistics ===
        await _exec_retry(db, """
        CREATE TABLE IF NOT EXISTS character_equipment_items (
          ocid TEXT NOT NULL,
          equip_date TEXT,
          item_equipment_part TEXT,
          item_equipment_slot TEXT,
          item_name TEXT,
          item_icon TEXT,

          potential_grade TEXT,
          add_potential_grade TEXT,

          pot1 TEXT, pot2 TEXT, pot3 TEXT,
          apot1 TEXT, apot2 TEXT, apot3 TEXT,

          item_json TEXT,
          updated_at TEXT,
          PRIMARY KEY (ocid, item_equipment_part, item_equipment_slot, item_name)
        )
        """)
        await _exec_retry(db, """
        CREATE INDEX IF NOT EXISTS idx_equip_items_name ON character_equipment_items(item_name)
        """)
        await _exec_retry(db, """
        CREATE INDEX IF NOT EXISTS idx_equip_items_part ON character_equipment_items(item_equipment_part)
        """)
        await _exec_retry(db, """
        CREATE INDEX IF NOT EXISTS idx_equip_items_grades ON character_equipment_items(potential_grade, add_potential_grade)
        """)

        # === blacklist sync log（官網停權名單增量同步紀錄）===
        await _exec_retry(db, """
        CREATE TABLE IF NOT EXISTS blacklist_sync_log (
          world_name TEXT PRIMARY KEY,
          server_name TEXT,
          server_id TEXT,
          last_success_date TEXT,  -- YYYY-MM-DD
          updated_at TEXT
        )
        """)

        await db.commit()


async def vacuum_db():
    async with aiosqlite.connect(DB_PATH, isolation_level=None) as db:
        await _exec_retry(db, "PRAGMA busy_timeout=30000;")
        await _exec_retry(db, "PRAGMA journal_mode=WAL;")
        await _exec_retry(db, "PRAGMA synchronous=NORMAL;")
        await _exec_retry(db, "PRAGMA wal_checkpoint(TRUNCATE);")

        try:
            await db.commit()
        except Exception:
            pass

        await _exec_retry(db, "VACUUM;")


