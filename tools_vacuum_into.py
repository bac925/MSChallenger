import asyncio
import os
import aiosqlite

DB_PATH = "maple.db"
OUT_PATH = r"D:\maple.db"  # 改成另一顆磁碟/分割區

async def exec_close(db, sql, params=()):
    cur = await db.execute(sql, params)
    await cur.close()

async def main():
    if os.path.exists(OUT_PATH):
        os.remove(OUT_PATH)

    async with aiosqlite.connect(DB_PATH, isolation_level=None) as db:
        await exec_close(db, "PRAGMA busy_timeout=30000;")
        await exec_close(db, "PRAGMA journal_mode=WAL;")
        await exec_close(db, "PRAGMA synchronous=NORMAL;")
        await exec_close(db, "PRAGMA wal_checkpoint(TRUNCATE);")

        # VACUUM INTO 不能用參數化，只能組字串
        await exec_close(db, f"VACUUM INTO '{OUT_PATH}';")

    print("VACUUM INTO done:", OUT_PATH)

asyncio.run(main())
