"""核验水位测试：双水位裁决、迟到回补、幂等重放、空档与未覆盖原因、页面/归档一致。"""
import csv
import io

import pytest

from app.config import COLUMNS
from app.watermark import service
from app.watermark.archive import build_watermark_archive
from app.watermark.rules import (
    advance_coverage,
    floor_bucket,
    is_late_arrival,
    parse_scan,
    safety_margin,
)
from app.values import instant

HEADER = ",".join(COLUMNS + ["observed_at"])


def csv_block(*rows):
    line = HEADER + "\n"
    for r in rows:
        line += ",".join(r) + "\n"
    return line.encode()


def row(event_id, source_time, observed_at, doc="d1", note="n"):
    return (event_id, doc, "1", "upsert", source_time, "-", "source", "{}", "1",
            note, observed_at)


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    from app.watermark import store
    monkeypatch.setattr(store, "DATA_DIR", tmp_path)
    service.reset_all()
    return tmp_path


# ---------- 纯规则：水位何时前移 ----------

def test_coverage_never_advances_beyond_observed():
    obs = instant("2026-11-16T10:30:00Z")
    cov = advance_coverage(obs, instant("2026-11-16T08:00:00Z"), 3600, 3600)
    # 观测前沿 10:30，回退 1 小时 => 09:30 对齐桶 => 09:00，绝不越过 10:30
    assert cov == instant("2026-11-16T09:00:00Z")
    assert cov <= obs


def test_coverage_monotonic_only():
    first = advance_coverage(instant("2026-11-16T12:00:00Z"), None, 3600, 3600)
    second = advance_coverage(instant("2026-11-16T11:00:00Z"), first, 3600, 3600)
    assert second == first  # 观测前沿倒退，水位不退


def test_safety_margin_at_least_one_bucket():
    assert safety_margin(3600, 120) == 3600
    assert safety_margin(3600, 7200) == 7200


def test_late_arrival_uses_bucket_close():
    et = instant("2026-11-16T10:10:00Z")
    # 桶 [10:00,11:00) 在 11:00 关闭；12:00 才到达 => 迟到
    assert is_late_arrival(et, instant("2026-11-16T12:00:00Z"),
                           instant("2026-11-16T11:00:00Z"), 3600) is True
    # 关闭前到达 => 不算迟到
    assert is_late_arrival(et, instant("2026-11-16T10:30:00Z"),
                           instant("2026-11-16T11:00:00Z"), 3600) is False
    # 水位尚未越过该桶 => 不算迟到
    assert is_late_arrival(et, instant("2026-11-16T12:00:00Z"),
                           instant("2026-11-16T10:00:00Z"), 3600) is False


def test_floor_bucket_half_open():
    assert floor_bucket(instant("2026-11-16T10:59:59Z"), 3600) == instant("2026-11-16T10:00:00Z")
    assert floor_bucket(instant("2026-11-16T11:00:00Z"), 3600) == instant("2026-11-16T11:00:00Z")


# ---------- 诉求一：已覆盖范围内仍能补见迟到事件，且不被跳过 ----------

def test_late_event_in_covered_range_is_backfilled_not_skipped(isolated):
    b1 = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T10:30:00Z"))
    s1 = service.run_scan(b1, {"scan_time": "2026-11-16T11:30:00Z"})
    # 首次建档锚定首个事件桶起点
    assert s1["watermark"]["coverage_frontier"] == "2026-11-16T10:00:00+00:00"

    # 同属 [10:00,11:00) 桶的事件，在桶随第二次扫描关闭后才到达
    b2 = csv_block(row("e2", "2026-11-16T10:10:00Z", "2026-11-16T12:00:00Z", note="late"))
    s2 = service.run_scan(b2, {"scan_time": "2026-11-16T12:30:00Z"})
    ids = {e["event_id"]: e["classification"] for e in s2["events"]}
    assert ids["e2"] == "LATE_BACKFILL"  # 补见而非跳过
    assert s2["counts"]["late_backfill"] == 1
    # 迟到身份持久化，后续快照不改写
    s3 = service.snapshot()
    assert next(e for e in s3["events"] if e["event_id"] == "e2")["classification"] == "LATE_BACKFILL"


def test_watermark_advances_only_on_maturity(isolated):
    b1 = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T10:30:00Z"))
    s1 = service.run_scan(b1, {"scan_time": "2026-11-16T11:30:00Z"})
    # 首次建档锚定首个事件桶；安全余量 1h 下 10 点桶尚未成熟
    assert s1["watermark"]["coverage_frontier"] == "2026-11-16T10:00:00+00:00"
    b2 = csv_block(row("e2", "2026-11-16T11:05:00Z", "2026-11-16T11:30:00Z"))
    s2 = service.run_scan(b2, {"scan_time": "2026-11-16T12:30:00Z"})
    # 观测前沿推进到 11:30，回退 1h 对齐 => 10 点桶成熟关闭
    assert s2["watermark"]["coverage_frontier"] == "2026-11-16T10:00:00+00:00"
    b3 = csv_block(row("e3", "2026-11-16T12:05:00Z", "2026-11-16T12:30:00Z"))
    s3 = service.run_scan(b3, {"scan_time": "2026-11-16T13:30:00Z"})
    assert s3["watermark"]["coverage_frontier"] == "2026-11-16T11:00:00+00:00"
    assert s3["watermark"]["moved"] is True


# ---------- 诉求二：重跑同一批不新增或改换结论 ----------

def test_replay_same_batch_is_idempotent(isolated):
    b1 = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T10:30:00Z"))
    service.run_scan(b1, {"scan_time": "2026-11-16T11:30:00Z"})
    b2 = csv_block(row("e2", "2026-11-16T10:10:00Z", "2026-11-16T12:00:00Z"))
    first = service.run_scan(b2, {"scan_time": "2026-11-16T12:30:00Z"})
    replay = service.run_scan(b2, {"scan_time": "2026-11-16T12:30:00Z"})
    assert replay["scan"]["replayed"] is True
    assert len(replay["scans"]) == len(first["scans"]) == 2
    assert replay["counts"] == first["counts"]
    assert [(e["event_id"], e["classification"]) for e in replay["events"]] == \
           [(e["event_id"], e["classification"]) for e in first["events"]]
    # 水位值完全一致；moved 仅描述提交当时是否前移，重放不再前移
    assert replay["watermark"]["coverage_frontier"] == first["watermark"]["coverage_frontier"]
    assert replay["watermark"]["observed_frontier"] == first["watermark"]["observed_frontier"]
    assert first["watermark"]["moved"] is True and replay["watermark"]["moved"] is False


def test_cross_batch_duplicate_is_quarantined_not_overwriting(isolated):
    b1 = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T10:30:00Z"))
    service.run_scan(b1, {"scan_time": "2026-11-16T11:30:00Z"})
    b2 = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T12:00:00Z", note="dup"))
    s = service.run_scan(b2, {"scan_time": "2026-11-16T12:30:00Z"})
    assert any(q["reason"] == "DUPLICATE_EVENT" for q in s["quarantine"])
    assert sum(e["event_id"] == "e1" for e in s["events"]) == 1


# ---------- 诉求三：原始事件、空档、未覆盖原因都能追回 ----------

def test_gap_empty_and_uncovered_reasons_recorded(isolated):
    # 首扫：10 点桶有事件
    b1 = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T10:30:00Z"))
    service.run_scan(b1, {"scan_time": "2026-11-16T11:30:00Z"})
    # 第二扫：观测前沿推进到 13:05。11 点桶为空且本轮成熟=>空档；12 点桶为空未成熟=>未覆盖
    b2 = csv_block(row("e2", "2026-11-16T10:20:00Z", "2026-11-16T13:05:00Z", note="late"))
    s = service.run_scan(b2, {"scan_time": "2026-11-16T13:30:00Z"})
    reasons = {g["bucket_start"][11:16]: g["reason"] for g in s["gaps"]}
    assert reasons.get("11:00") == "GAP_EMPTY"
    assert reasons.get("12:00") == "UNCOVERED_PENDING"
    # 第三扫：一个事件补入 12 点空桶 => 该桶未覆盖记录被核销，不再出现在台账
    b3 = csv_block(row("e3", "2026-11-16T12:40:00Z", "2026-11-16T14:05:00Z"))
    s2 = service.run_scan(b3, {"scan_time": "2026-11-16T14:30:00Z"})
    reasons2 = {g["bucket_start"][11:16]: g["reason"] for g in s2["gaps"]}
    assert "12:00" not in reasons2  # 迟到补入后核销未覆盖
    assert reasons2.get("13:00") == "UNCOVERED_PENDING"
    # 原始事件仍全部可追回
    assert {e["event_id"] for e in s2["events"]} == {"e1", "e2", "e3"}


def test_quarantine_preserves_raw_line(isolated):
    bad = csv_block(row("e1", "not-a-time", "2026-11-16T10:30:00Z"))
    s = service.run_scan(bad, {"scan_time": "2026-11-16T11:30:00Z"})
    assert s["quarantine"][0]["reason"] == "INVALID_TIME"
    assert s["quarantine"][0]["raw"][0] == "e1"


def test_no_arrival_does_not_advance_watermark(isolated):
    b1 = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T10:30:00Z"))
    s1 = service.run_scan(b1, {"scan_time": "2026-11-16T11:30:00Z"})
    empty = (HEADER + "\n").encode()
    s2 = service.run_scan(empty, {"scan_time": "2026-11-16T12:30:00Z"})
    assert s2["watermark"] == s1["watermark"]  # 无到达，不前移
    assert s2["scan"]["metrics"]["no_arrival"] is True


def test_future_arrival_rejected(isolated):
    bad = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T20:00:00Z"))
    s = service.run_scan(bad, {"scan_time": "2026-11-16T11:30:00Z"})
    assert s["quarantine"][0]["reason"] == "FUTURE_ARRIVAL"


# ---------- 页面覆盖结论与归档导出一致 ----------

def test_archive_matches_snapshot(isolated):
    b1 = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T10:30:00Z"))
    service.run_scan(b1, {"scan_time": "2026-11-16T11:30:00Z"})
    b2 = csv_block(row("e2", "2026-11-16T10:10:00Z", "2026-11-16T12:00:00Z"))
    snap = service.run_scan(b2, {"scan_time": "2026-11-16T12:30:00Z"})
    text = build_watermark_archive(snap)
    rows = list(csv.reader(io.StringIO(text.lstrip("﻿"))))
    header_meta = rows[0]
    assert snap["watermark"]["coverage_frontier"] in header_meta
    event_ids = {r[2] for r in rows if r[1] == "event"}
    gap_ids = {r[2] for r in rows if r[1] == "gap"}
    assert event_ids == {e["event_id"] for e in snap["events"]}
    assert gap_ids == {g["bucket_start"] for g in snap["gaps"]}
    # 摘要行计数与快照完全一致
    summary = next(r for r in rows if r[1] == "summary")
    import json
    assert json.loads(summary[8]) == snap["counts"]
    # 迟到事件在导出中保留回补结论
    late_row = next(r for r in rows if r[2] == "e2")
    assert late_row[5] == "迟到回补"


def test_page_and_archive_share_snapshot(isolated, client):
    b = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T10:30:00Z"))
    service.run_scan(b, {"scan_time": "2026-11-16T11:30:00Z"})
    page = client.get("/watermark")
    assert page.status_code == 200
    api = client.get("/api/watermark").get_json()
    archive = client.get("/api/watermark/archive")
    assert archive.status_code == 200
    page_text = page.get_data(as_text=True)
    # 页面展示的覆盖水位与 API、归档同源
    assert api["watermark"]["coverage_frontier"][11:16] in page_text
    assert api["watermark"]["coverage_frontier"] in archive.data.decode()


def test_archive_404_when_empty(isolated, client):
    assert client.get("/api/watermark/archive").status_code == 404


def test_parse_accepts_default_observed_at_param():
    raw = (",".join(COLUMNS) + "\n"
           "e1,d1,1,upsert,2026-11-16T10:05:00Z,-,source,{},1,n\n").encode()
    parsed = parse_scan(raw, {"observed_at": "2026-11-16T10:30:00Z"},
                        instant("2026-11-16T11:30:00Z"))
    assert parsed["events"][0]["observed_at"] == instant("2026-11-16T10:30:00Z")


def test_http_scan_flow_and_replay(isolated, client):
    b = csv_block(row("e1", "2026-11-16T10:05:00Z", "2026-11-16T10:30:00Z"))
    r1 = client.post("/api/watermark/scan",
                     json={"csv": b.decode(), "parameters": {"scan_time": "2026-11-16T11:30:00Z"}})
    assert r1.status_code == 200
    r2 = client.post("/api/watermark/scan",
                     json={"csv": b.decode(), "parameters": {"scan_time": "2026-11-16T11:30:00Z"}})
    assert r2.get_json()["scan"]["replayed"] is True
    assert client.post("/api/watermark/reset").get_json() == {"reset": True}
    assert client.get("/api/watermark").get_json().get("empty") is True
