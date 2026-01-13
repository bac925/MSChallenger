import asyncio
import aiohttp
import aiosqlite
from datetime import datetime
from pathlib import Path
import argparse
import json
import csv
import traceback
from typing import Dict, Any, List, Optional, Tuple

from nexon_api import NexonClient
from db import DB_PATH

WORLD_DEFAULT = "挑戰者"


# -----------------------------
# helpers
# -----------------------------
def fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def load_api_key() -> str:
    p = Path(__file__).resolve().parent / "apikey.txt"
    if not p.exists():
        raise RuntimeError("找不到 apikey.txt")
    key = p.read_text(encoding="utf-8").strip()
    if not key:
        raise RuntimeError("apikey.txt 為空")
    return key


def safe_int(v, default=0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return int(v)
        s = str(v).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def safe_float(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def to_int01(v) -> int:
    try:
        iv = int(v)
    except (TypeError, ValueError):
        return 0
    return 1 if iv == 1 else 0


def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_incomplete_basic(row: dict) -> bool:
    # 你最在意的：職業、性別
    if is_blank(row.get("character_class")):
        return True
    if is_blank(row.get("character_gender")):
        return True

    # 建議一併補齊，減少統計/顯示缺欄位
    if row.get("character_level") is None:
        return True
    if is_blank(row.get("character_image")):
        return True
    if is_blank(row.get("character_date_create")):
        return True

    if row.get("character_class_level") is None:
        return True
    if row.get("character_exp") is None:
        return True
    if row.get("character_exp_rate") is None:
        return True

    if row.get("access_flag") is None:
        return True
    if row.get("liberation_quest_clear") is None:
        return True

    return False


def now_iso() -> str:
    return datetime.now().isoformat()


# -----------------------------
# pending list IO (ocid not found / api errors)
# -----------------------------
def load_pending(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"world": None, "generated_at": None, "items": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"world": None, "generated_at": None, "items": []}
        if "items" not in data or not isinstance(data["items"], list):
            data["items"] = []
        return data
    except Exception:
        return {"world": None, "generated_at": None, "items": []}


def save_pending(path: Path, world: str, items: List[Dict[str, Any]]):
    data = {
        "world": world,
        "generated_at": now_iso(),
        "items": items,
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def export_pending_csv(path: Path, items: List[Dict[str, Any]]):
    fields = ["character_name", "attempts", "last_reason", "last_error", "last_checked_at"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for it in items:
            w.writerow({
                "character_name": it.get("character_name"),
                "attempts": it.get("attempts", 0),
                "last_reason": it.get("last_reason"),
                "last_error": it.get("last_error"),
                "last_checked_at": it.get("last_checked_at"),
            })


def merge_pending(old_items: List[Dict[str, Any]], updates: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    old_items: [{"character_name":..., "attempts":..., ...}, ...]
    updates:  {name: {"attempts_inc":1, "last_reason":..., "last_error":..., "last_checked_at":...}}
    """
    m: Dict[str, Dict[str, Any]] = {}
    for it in old_items:
        nm = (it.get("character_name") or "").strip()
        if not nm:
            continue
        m[nm] = {
            "character_name": nm,
            "attempts": int(it.get("attempts") or 0),
            "last_reason": it.get("last_reason"),
            "last_error": it.get("last_error"),
            "last_checked_at": it.get("last_checked_at"),
        }

    for nm, u in updates.items():
        nm = (nm or "").strip()
        if not nm:
            continue
        if nm not in m:
            m[nm] = {"character_name": nm, "attempts": 0, "last_reason": None, "last_error": None, "last_checked_at": None}
        m[nm]["attempts"] = int(m[nm].get("attempts") or 0) + int(u.get("attempts_inc") or 0)
        m[nm]["last_reason"] = u.get("last_reason")
        m[nm]["last_error"] = u.get("last_error")
        m[nm]["last_checked_at"] = u.get("last_checked_at")

    return sorted(m.values(), key=lambda x: x["character_name"])


def filter_pending_for_retry(items: List[Dict[str, Any]], *, include_api_errors: bool, include_not_found: bool) -> List[str]:
    out = []
    for it in items:
        nm = (it.get("character_name") or "").strip()
        if not nm:
            continue
        reason = (it.get("last_reason") or "").strip()
        if reason == "ocid_not_found" and include_not_found:
            out.append(nm)
        elif reason.startswith("api_error") and include_api_errors:
            out.append(nm)
        elif reason in ("timeout", "exception") and include_api_errors:
            out.append(nm)
    return out


# -----------------------------
# DB read candidates
# -----------------------------
async def load_candidates(world: str, limit: int, include_complete: bool) -> List[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")

        async with db.execute(
            """
            SELECT
              cl.character_name AS list_name,
              c.ocid,
              c.character_name,
              c.character_gender,
              c.character_class,
              c.character_class_level,
              c.character_level,
              c.character_exp,
              c.character_exp_rate,
              c.character_guild_name,
              c.character_image,
              c.character_date_create,
              c.liberation_quest_clear,
              c.access_flag
            FROM character_list cl
            LEFT JOIN characters c
              ON c.world_name = cl.world_name
             AND c.character_name = cl.character_name
            WHERE cl.world_name = ?
            ORDER BY cl.character_name
            """,
            (world,),
        ) as cur:
            rows = await cur.fetchall()

    out = []
    for r in rows:
        d = {
            "list_name": r[0],
            "ocid": r[1],
            "character_name": r[2],
            "character_gender": r[3],
            "character_class": r[4],
            "character_class_level": r[5],
            "character_level": r[6],
            "character_exp": r[7],
            "character_exp_rate": r[8],
            "character_guild_name": r[9],
            "character_image": r[10],
            "character_date_create": r[11],
            "liberation_quest_clear": r[12],
            "access_flag": r[13],
        }

        if is_blank(d.get("ocid")):
            out.append(d)
            continue

        if include_complete:
            out.append(d)
        else:
            if is_incomplete_basic(d):
                out.append(d)

    if limit and limit > 0:
        out = out[: int(limit)]
    return out


# -----------------------------
# API fetch one (robust error classification)
# -----------------------------
async def fetch_one_basic(
    sem: asyncio.Semaphore,
    client: NexonClient,
    session: aiohttp.ClientSession,
    world: str,
    row: dict,
) -> dict:
    """
    回傳：
      ok: bool
      reason:
        - ok
        - ocid_not_found
        - basic_not_found
        - api_error_xxx
        - timeout
        - exception
      list_name, ocid, basic, err
    """
    async with sem:
        name = (row.get("list_name") or "").strip()
        ocid = row.get("ocid")

        try:
            # 1) ocid 不存在 => 先拿 ocid
            if is_blank(ocid):
                try:
                    ocid_json = await client.get_ocid(session, name)
                except asyncio.TimeoutError:
                    return {"ok": False, "reason": "timeout", "list_name": name, "ocid": None, "basic": None, "err": "get_ocid timeout"}
                except Exception as e:
                    # 將 API 例外分類為 api_error（不要刪）
                    return {"ok": False, "reason": "api_error_get_ocid", "list_name": name, "ocid": None, "basic": None, "err": repr(e)}

                ocid = (ocid_json or {}).get("ocid")
                if not ocid:
                    # 這種才算「查不到 ocid」
                    return {"ok": False, "reason": "ocid_not_found", "list_name": name, "ocid": None, "basic": None, "err": None}

            # 2) 拿 basic
            try:
                basic = await client.get_basic(session, ocid)
            except asyncio.TimeoutError:
                return {"ok": False, "reason": "timeout", "list_name": name, "ocid": ocid, "basic": None, "err": "get_basic timeout"}
            except Exception as e:
                return {"ok": False, "reason": "api_error_get_basic", "list_name": name, "ocid": ocid, "basic": None, "err": repr(e)}

            if not basic:
                return {"ok": False, "reason": "basic_not_found", "list_name": name, "ocid": ocid, "basic": None, "err": None}

            return {"ok": True, "reason": "ok", "list_name": name, "ocid": ocid, "basic": basic, "err": None}

        except Exception as e:
            return {"ok": False, "reason": "exception", "list_name": name, "ocid": ocid, "basic": None, "err": repr(e)}


# -----------------------------
# DB write (single writer)
# -----------------------------
async def write_basic_results(world: str, results: List[dict]) -> dict:
    now = now_iso()

    ok = [r for r in results if isinstance(r, dict) and r.get("ok")]
    fail = [r for r in results if isinstance(r, dict) and not r.get("ok")]

    inserted_or_updated = 0

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")
        await db.execute("PRAGMA journal_mode=WAL;")

        for r in ok:
            basic = r.get("basic") or {}
            ocid = r.get("ocid")
            list_name = r.get("list_name")

            access_flag_raw = str(basic.get("access_flag") or "").strip().lower()
            access_flag = 1 if access_flag_raw == "true" else 0
            liberation = to_int01(basic.get("liberation_quest_clear"))

            params = (
                ocid,
                basic.get("character_name") or list_name,
                world,
                basic.get("character_gender"),
                basic.get("character_class"),
                safe_int(basic.get("character_class_level"), default=0),
                safe_int(basic.get("character_level"), default=0),
                safe_int(basic.get("character_exp"), default=0),
                safe_float(basic.get("character_exp_rate")),
                basic.get("character_guild_name"),
                basic.get("character_image"),
                basic.get("character_date_create"),
                liberation,
                access_flag,
                now,
            )

            await db.execute(
                """
                INSERT INTO characters (
                  ocid, character_name, world_name,
                  character_gender, character_class, character_class_level,
                  character_level, character_exp, character_exp_rate,
                  character_guild_name, character_image, character_date_create,
                  liberation_quest_clear, access_flag, updated_at
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(ocid) DO UPDATE SET
                  character_name=excluded.character_name,
                  world_name=excluded.world_name,
                  character_gender=COALESCE(excluded.character_gender, characters.character_gender),
                  character_class=COALESCE(excluded.character_class, characters.character_class),
                  character_class_level=CASE
                    WHEN excluded.character_class_level IS NOT NULL AND excluded.character_class_level <> 0
                      THEN excluded.character_class_level
                    ELSE characters.character_class_level
                  END,
                  character_level=CASE
                    WHEN excluded.character_level IS NOT NULL AND excluded.character_level > COALESCE(characters.character_level,0)
                      THEN excluded.character_level
                    ELSE COALESCE(characters.character_level,0)
                  END,
                  character_exp=CASE
                    WHEN excluded.character_exp IS NOT NULL AND excluded.character_exp > COALESCE(characters.character_exp,0)
                      THEN excluded.character_exp
                    ELSE COALESCE(characters.character_exp,0)
                  END,
                  character_exp_rate=COALESCE(excluded.character_exp_rate, characters.character_exp_rate),
                  character_guild_name=COALESCE(excluded.character_guild_name, characters.character_guild_name),
                  character_image=COALESCE(excluded.character_image, characters.character_image),
                  character_date_create=COALESCE(excluded.character_date_create, characters.character_date_create),
                  liberation_quest_clear=excluded.liberation_quest_clear,
                  access_flag=excluded.access_flag,
                  updated_at=excluded.updated_at
                """,
                params,
            )
            inserted_or_updated += 1

        await db.commit()

    fail_reason = {}
    for r in fail:
        reason = r.get("reason") or "unknown"
        fail_reason[reason] = fail_reason.get(reason, 0) + 1

    return {
        "total": len(results),
        "ok": len(ok),
        "fail": len(fail),
        "inserted_or_updated": inserted_or_updated,
        "fail_reason": fail_reason,
    }


# -----------------------------
# Delete from character_list (after manual retries)
# -----------------------------
async def delete_from_character_list(world: str, names: List[str]) -> int:
    if not names:
        return 0

    # 逐批刪，避免 SQLite 參數過多
    def chunked(lst, n=500):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    deleted = 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=30000;")
        await db.execute("PRAGMA journal_mode=WAL;")

        for ch in chunked(names, 500):
            await db.executemany(
                "DELETE FROM character_list WHERE world_name=? AND character_name=?",
                [(world, nm) for nm in ch]
            )
            deleted += len(ch)

        await db.commit()
    return deleted


# -----------------------------
# main runner
# -----------------------------
async def run_once(
    api_key: str,
    world: str,
    concurrency: int,
    limit: int,
    force: bool,
    print_every: int,
    pending_file: Path,
    retry_from_pending: bool,
    include_api_errors: bool,
    include_not_found: bool,
    delete_after_attempts: int,
    apply_delete: bool,
):
    start = datetime.now()
    print("=" * 72, flush=True)
    print(f"[{fmt(start)}] basic 回填開始｜世界={world}｜concurrency={concurrency}｜limit={limit or 'ALL'}｜force={force}", flush=True)
    print(f"[{fmt(datetime.now())}] pending_file={pending_file}", flush=True)

    # 讀取 pending（用來累積 attempts）
    pending_data = load_pending(pending_file)
    old_items = pending_data.get("items") or []

    # 決定本輪要處理的名字集合
    if retry_from_pending:
        names = filter_pending_for_retry(old_items, include_api_errors=include_api_errors, include_not_found=include_not_found)
        # 以 names 回建 candidates（只做 ocid/basic 補抓，不再從 DB 全掃）
        candidates = [{"list_name": nm, "ocid": None} for nm in names]
        if limit and limit > 0:
            candidates = candidates[: int(limit)]
        print(f"[{fmt(datetime.now())}] 本輪來源：pending｜待重試數={len(candidates)}", flush=True)
    else:
        candidates = await load_candidates(world=world, limit=limit, include_complete=force)
        print(f"[{fmt(datetime.now())}] 本輪來源：DB 掃描｜候選角色數={len(candidates)}", flush=True)

    total = len(candidates)
    if total == 0:
        print(f"[{fmt(datetime.now())}] 無需處理（候選=0）", flush=True)
        return

    sem = asyncio.Semaphore(max(1, int(concurrency)))
    client = NexonClient(api_key)

    results = []
    ok_cnt = 0
    fail_cnt = 0

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_one_basic(sem, client, session, world, row)
            for row in candidates
        ]

        for idx, coro in enumerate(asyncio.as_completed(tasks), start=1):
            r = await coro
            results.append(r)

            if r.get("ok"):
                ok_cnt += 1
            else:
                fail_cnt += 1

            if print_every > 0 and (idx % print_every == 0 or idx == total):
                print(f"[{fmt(datetime.now())}] 進度 {idx}/{total}｜ok={ok_cnt}｜fail={fail_cnt}", flush=True)

    # 成功的寫 DB
    rep = await write_basic_results(world, results)

    # 更新 pending：只記錄「失敗者」
    updates: Dict[str, Dict[str, Any]] = {}
    for r in results:
        if r.get("ok"):
            continue
        nm = (r.get("list_name") or "").strip()
        if not nm:
            continue
        updates[nm] = {
            "attempts_inc": 1,
            "last_reason": r.get("reason"),
            "last_error": r.get("err"),
            "last_checked_at": now_iso(),
        }

    new_items = merge_pending(old_items, updates)

    # 另外：把本輪「成功」的從 pending 移除（避免一直掛在上面）
    success_names = {(r.get("list_name") or "").strip() for r in results if r.get("ok")}
    if success_names:
        new_items = [it for it in new_items if (it.get("character_name") or "").strip() not in success_names]

    # 寫出 pending 檔
    save_pending(pending_file, world, new_items)
    export_pending_csv(pending_file.with_suffix(".csv"), new_items)

    # 若要刪除：只刪 attempts >= delete_after_attempts 且 last_reason == ocid_not_found
    to_delete = []
    if apply_delete and delete_after_attempts > 0:
        for it in new_items:
            if int(it.get("attempts") or 0) >= int(delete_after_attempts) and (it.get("last_reason") or "") == "ocid_not_found":
                nm = (it.get("character_name") or "").strip()
                if nm:
                    to_delete.append(nm)

        if to_delete:
            deleted = await delete_from_character_list(world, to_delete)
            print(f"[{fmt(datetime.now())}] 已從 character_list 刪除（ocid_not_found 且 attempts>={delete_after_attempts}）：{deleted} 人", flush=True)

            # 刪除後也同步從 pending 移除（避免殘留）
            to_delete_set = set(to_delete)
            new_items2 = [it for it in new_items if (it.get("character_name") or "").strip() not in to_delete_set]
            save_pending(pending_file, world, new_items2)
            export_pending_csv(pending_file.with_suffix(".csv"), new_items2)
        else:
            print(f"[{fmt(datetime.now())}] 未達刪除門檻（attempts>={delete_after_attempts} 且 ocid_not_found）的人數=0", flush=True)

    end = datetime.now()
    print(
        f"[{fmt(end)}] basic 回填完成｜耗時={(end-start).total_seconds():.1f}s｜"
        f"候選={total}｜成功={rep['ok']}｜失敗={rep['fail']}｜寫入={rep['inserted_or_updated']}",
        flush=True
    )
    if rep.get("fail_reason"):
        print(f"失敗原因統計：{rep['fail_reason']}", flush=True)

    print(f"[{fmt(datetime.now())}] pending 檔已更新：{pending_file}（以及 {pending_file.with_suffix('.csv')}）", flush=True)
    if not apply_delete:
        print(f"[{fmt(datetime.now())}] 本輪未執行刪除（未加 --apply-delete）", flush=True)


def parse_args():
    p = argparse.ArgumentParser(description="Backfill MapleStory character basic info + OCID retry list & optional delete.")
    p.add_argument("--world", default=WORLD_DEFAULT, help="world_name（預設：挑戰者）")
    p.add_argument("--concurrency", type=int, default=50, help="API 並行數（建議 20~80）")
    p.add_argument("--limit", type=int, default=0, help="本次最多處理幾筆（0=全部）")
    p.add_argument("--force", action="store_true", help="強制重抓（包含 basic 已完整者也重抓）")
    p.add_argument("--print-every", type=int, default=200, help="每 N 筆輸出一次進度（0=不輸出）")

    # 新增：pending 機制
    p.add_argument("--pending-file", default="pending_ocid.json", help="問題清單檔案（JSON）")
    p.add_argument("--retry-pending", action="store_true", help="只針對 pending 清單重試（不掃 DB）")
    p.add_argument("--include-api-errors", action="store_true", help="重試時包含 api_error/timeout/exception（預設不包含）")
    p.add_argument("--include-not-found", action="store_true", help="重試時包含 ocid_not_found（預設不包含）")

    # 新增：刪除機制（保守：需手動 apply）
    p.add_argument("--delete-after-attempts", type=int, default=0, help="累積 attempts >= N 且 ocid_not_found 才可刪除（0=不刪）")
    p.add_argument("--apply-delete", action="store_true", help="實際執行刪除（不加則只產生名單）")

    return p.parse_args()


async def main():
    args = parse_args()
    api_key = load_api_key()

    pending_file = Path(args.pending_file).resolve()

    # retry 模式若沒指定包含哪些，預設只重試 ocid_not_found（較符合你的操作習慣）
    include_api_errors = bool(args.include_api_errors)
    include_not_found = bool(args.include_not_found)

    if args.retry_pending and (not include_api_errors and not include_not_found):
        include_not_found = True

    try:
        await run_once(
            api_key=api_key,
            world=args.world,
            concurrency=args.concurrency,
            limit=args.limit,
            force=args.force,
            print_every=args.print_every,
            pending_file=pending_file,
            retry_from_pending=args.retry_pending,
            include_api_errors=include_api_errors,
            include_not_found=include_not_found,
            delete_after_attempts=int(args.delete_after_attempts or 0),
            apply_delete=bool(args.apply_delete),
        )
    except Exception:
        print(f"[{fmt(datetime.now())}] [ERROR] 發生例外：", flush=True)
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
