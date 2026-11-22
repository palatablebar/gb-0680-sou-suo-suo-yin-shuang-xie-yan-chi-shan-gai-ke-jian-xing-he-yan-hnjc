"""核验水位的持久化：状态表、幂等扫描批次、事件台账、空档台账、隔离行。

所有写入在单事务内完成；同一 scan_key 重放只回显既有批次，不新增或改换结论。
"""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.config import DATA_DIR
from app.errors import DomainError

STATE_ID = "singleton"

SCHEMA = """
CREATE TABLE IF NOT EXISTS wm_state(
  id TEXT PRIMARY KEY,
  observed_frontier TEXT,
  coverage_frontier TEXT,
  bucket_seconds INTEGER NOT NULL,
  allowed_lateness_seconds INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wm_scans(
  scan_key TEXT PRIMARY KEY,
  seq INTEGER NOT NULL UNIQUE,
  scan_time TEXT NOT NULL,
  input_sha256 TEXT NOT NULL,
  params TEXT NOT NULL,
  created_at TEXT NOT NULL,
  metrics TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wm_events(
  event_id TEXT PRIMARY KEY,
  doc_id TEXT NOT NULL,
  version TEXT NOT NULL,
  op TEXT NOT NULL,
  side TEXT NOT NULL,
  event_time TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  first_scan_key TEXT NOT NULL,
  source_line INTEGER NOT NULL,
  raw TEXT NOT NULL,
  late INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS wm_gaps(
  bucket_start TEXT PRIMARY KEY,
  reason TEXT NOT NULL,
  detail TEXT NOT NULL,
  updated_by_scan TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wm_quarantine(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scan_key TEXT NOT NULL,
  source_line INTEGER,
  reason TEXT NOT NULL,
  detail TEXT NOT NULL,
  raw TEXT NOT NULL
);
"""


def connect():
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(Path(DATA_DIR) / "records.db", timeout=20)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(SCHEMA)
    return db


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def scan_key_for(raw, params, scan_time):
    body = raw + json.dumps(params, sort_keys=True, ensure_ascii=False).encode() + str(scan_time).encode()
    return hashlib.sha256(body).hexdigest()[:24]


def get_state(db):
    row = db.execute("SELECT * FROM wm_state WHERE id=?", (STATE_ID,)).fetchone()
    return dict(row) if row else None


def existing_scan(db, scan_key):
    row = db.execute("SELECT * FROM wm_scans WHERE scan_key=?", (scan_key,)).fetchone()
    return dict(row) if row else None


def _load_events(db):
    return [dict(r) for r in db.execute("SELECT * FROM wm_events ORDER BY event_time,event_id")]


def _load_gaps(db):
    return [dict(r) for r in db.execute("SELECT * FROM wm_gaps ORDER BY bucket_start")]


def _load_quarantine(db, scan_key=None):
    if scan_key:
        rows = db.execute("SELECT * FROM wm_quarantine WHERE scan_key=? ORDER BY id", (scan_key,))
    else:
        rows = db.execute("SELECT * FROM wm_quarantine ORDER BY id")
    return [dict(r) for r in rows]


def commit_scan(scan_key, scan_time, raw, params, parsed, observed_frontier, coverage_frontier):
    """单事务合并一次扫描。重复 scan_key 直接回显，保证重跑幂等。"""
    with connect() as db:
        prior = existing_scan(db, scan_key)
        if prior:
            return _snapshot(db, prior, replayed=True)
        state = get_state(db)
        seq = db.execute("SELECT COALESCE(MAX(seq),0)+1 FROM wm_scans").fetchone()[0]
        db.execute(
            "INSERT INTO wm_scans VALUES(?,?,?,?,?,?,?)",
            (scan_key, seq, scan_time.isoformat(), hashlib.sha256(raw).hexdigest(),
             json.dumps(params, ensure_ascii=False), now_iso(),
             json.dumps(parsed["metrics"], ensure_ascii=False)),
        )
        for event in parsed["events"]:
            db.execute(
                "INSERT OR IGNORE INTO wm_events VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (event["event_id"], event["doc_id"], event["version"], event["op"], event["side"],
                 event["event_time"].isoformat(), event["observed_at"].isoformat(), scan_key,
                 event["source_line"], json.dumps(event["raw"], ensure_ascii=False),
                 1 if event["late"] else 0),
            )
        for row in parsed["quarantine"]:
            db.execute(
                "INSERT INTO wm_quarantine(scan_key,source_line,reason,detail,raw) VALUES(?,?,?,?,?)",
                (scan_key, row.get("_line"), row.get("reason", ""), row.get("detail", ""),
                 json.dumps(row.get("_raw", []), ensure_ascii=False)),
            )
        for gap in parsed["gaps"]:
            db.execute(
                "INSERT INTO wm_gaps(bucket_start,reason,detail,updated_by_scan) VALUES(?,?,?,?) "
                "ON CONFLICT(bucket_start) DO UPDATE SET reason=excluded.reason,detail=excluded.detail,"
                "updated_by_scan=excluded.updated_by_scan",
                (gap["bucket_start"], gap["reason"], gap["detail"], scan_key),
            )
        # 本轮仍在观测窗口内、但已不在空档集合（通常是迟到事件补入）的陈旧记录予以核销
        active_gaps = {gap["bucket_start"] for gap in parsed["gaps"]}
        stale = [r["bucket_start"] for r in db.execute("SELECT bucket_start FROM wm_gaps")]
        for bucket_start in stale:
            if bucket_start not in active_gaps and bucket_start < (
                    observed_frontier.isoformat() if observed_frontier else ""):
                db.execute("DELETE FROM wm_gaps WHERE bucket_start=?", (bucket_start,))
        db.execute(
            "INSERT INTO wm_state(id,observed_frontier,coverage_frontier,bucket_seconds,"
            "allowed_lateness_seconds,updated_at) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET observed_frontier=excluded.observed_frontier,"
            "coverage_frontier=excluded.coverage_frontier,updated_at=excluded.updated_at",
            (STATE_ID, observed_frontier.isoformat() if observed_frontier else None,
             coverage_frontier.isoformat() if coverage_frontier else None,
             params["bucket_seconds"], params["allowed_lateness_seconds"], now_iso()),
        )
        scan_row = existing_scan(db, scan_key)
        return _snapshot(db, scan_row, replayed=False, state_before=state)


def load_snapshot():
    with connect() as db:
        state = get_state(db)
        if state is None:
            return None
        latest = db.execute("SELECT * FROM wm_scans ORDER BY seq DESC LIMIT 1").fetchone()
        return _snapshot(db, dict(latest) if latest else None)


def _snapshot(db, scan_row, replayed=False, state_before=None):
    state = get_state(db)
    events = _load_events(db)
    observed_frontier = datetime.fromisoformat(state["observed_frontier"]) if state and state["observed_frontier"] else None
    coverage_frontier = datetime.fromisoformat(state["coverage_frontier"]) if state and state["coverage_frontier"] else None
    for event in events:
        event_time = datetime.fromisoformat(event["event_time"])
        event["late"] = bool(event["late"])
        # 迟到身份在到达时判定并持久化，不随后续水位前移而被改写成普通已覆盖
        if event["late"]:
            event["classification"] = "LATE_BACKFILL"
        elif coverage_frontier and event_time < coverage_frontier:
            event["classification"] = "COVERED"
        elif observed_frontier and event_time < observed_frontier:
            event["classification"] = "IN_WINDOW"
        else:
            event["classification"] = "FUTURE_EVENT"
        event["raw"] = json.loads(event["raw"])
    gaps = _load_gaps(db)
    quarantine = _load_quarantine(db)
    for row in quarantine:
        row["raw"] = json.loads(row["raw"])
    scans = [dict(r) for r in db.execute(
        "SELECT scan_key,seq,scan_time,input_sha256,params,created_at,metrics FROM wm_scans ORDER BY seq")]
    for row in scans:
        row["metrics"] = json.loads(row["metrics"])
        row["params"] = json.loads(row["params"])
    scan_out = dict(scan_row) if scan_row else {}
    if isinstance(scan_out.get("metrics"), str):
        scan_out["metrics"] = json.loads(scan_out["metrics"])
    if isinstance(scan_out.get("params"), str):
        scan_out["params"] = json.loads(scan_out["params"])
    return {
        "watermark": {
            "observed_frontier": observed_frontier.isoformat() if observed_frontier else None,
            "coverage_frontier": coverage_frontier.isoformat() if coverage_frontier else None,
            "bucket_seconds": state["bucket_seconds"] if state else None,
            "allowed_lateness_seconds": state["allowed_lateness_seconds"] if state else None,
            "moved": False if replayed or state_before is None else
            state_before.get("coverage_frontier") != (state or {}).get("coverage_frontier"),
        },
        "scan": {**scan_out, "replayed": replayed},
        "events": events,
        "gaps": gaps,
        "quarantine": quarantine,
        "scans": scans,
        "counts": {
            "events": len(events),
            "late_backfill": sum(e["classification"] == "LATE_BACKFILL" for e in events),
            "covered": sum(e["classification"] == "COVERED" for e in events),
            "in_window": sum(e["classification"] == "IN_WINDOW" for e in events),
            "future": sum(e["classification"] == "FUTURE_EVENT" for e in events),
            "gap_empty": sum(g["reason"] == "GAP_EMPTY" for g in gaps),
            "uncovered_pending": sum(g["reason"] == "UNCOVERED_PENDING" for g in gaps),
            "quarantine": len(quarantine),
        },
    }


def reset():
    with connect() as db:
        for table in ("wm_state", "wm_scans", "wm_events", "wm_gaps", "wm_quarantine"):
            db.execute(f"DELETE FROM {table}")
