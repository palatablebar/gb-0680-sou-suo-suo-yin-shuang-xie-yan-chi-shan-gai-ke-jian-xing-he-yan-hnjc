"""扫描编排：参数裁决、双水位推进、迟到标记、空档重建与幂等提交。"""
from datetime import datetime, timezone

from app.errors import DomainError
from app.values import instant
from app.watermark import store
from app.watermark.rules import (
    DEFAULT_BUCKET_SECONDS,
    DEFAULT_LATENESS_SECONDS,
    advance_coverage,
    floor_bucket,
    int_param,
    is_late_arrival,
    parse_scan,
    rebuild_gaps,
)


def _params(raw_params):
    params = raw_params or {}
    bucket = int_param(params.get("bucket_seconds"), DEFAULT_BUCKET_SECONDS, "桶宽需为正整数秒")
    lateness = int_param(params.get("allowed_lateness_seconds"), DEFAULT_LATENESS_SECONDS,
                         "允许迟到量需为正整数秒")
    return {
        "bucket_seconds": bucket,
        "allowed_lateness_seconds": lateness,
        "observed_at": params.get("observed_at") or None,
        "scan_time": params.get("scan_time") or None,
    }


def run_scan(raw, raw_params):
    params = _params(raw_params)
    scan_time = instant(params["scan_time"]) if params["scan_time"] else datetime.now(timezone.utc)
    key = store.scan_key_for(raw, {k: v for k, v in params.items() if k != "scan_time"}, scan_time)
    with store.connect() as db:
        prior = store.existing_scan(db, key)
        if prior:
            return store._snapshot(db, prior, replayed=True)
        state = store.get_state(db)
    bucket = params["bucket_seconds"]
    lateness = params["allowed_lateness_seconds"]
    if state and state["bucket_seconds"] != bucket:
        raise DomainError("BUCKET_LOCKED", "桶宽在建档后不可变更，如需调整请重置水位台账")
    parsed = parse_scan(raw, params, scan_time)
    with store.connect() as db:
        state = store.get_state(db)
        old_observed = datetime.fromisoformat(state["observed_frontier"]) if state and state["observed_frontier"] else None
        old_coverage = datetime.fromisoformat(state["coverage_frontier"]) if state and state["coverage_frontier"] else None
    batch_max = parsed["metrics"]["batch_observed_max"]
    batch_max = datetime.fromisoformat(batch_max) if batch_max else None
    # 观测前沿：只进不退的最大到达时刻
    observed_frontier = max(filter(None, [old_observed, batch_max]), default=None)
    if old_coverage is None:
        # 首次建档：覆盖水位锚定首个事件桶起点，不回退到无事件的更早空桶
        starts = [floor_bucket(e["event_time"], bucket) for e in parsed["events"]]
        coverage_frontier = min(starts) if starts else None
    else:
        # 后续扫描：观测前沿回退安全余量并对齐桶边界，只进不退、不越观测前沿
        coverage_frontier = advance_coverage(observed_frontier, old_coverage, bucket, lateness)
    # 迟到判定基于推进后的覆盖水位：事件所在桶已关闭、且事件在关闭时点之后才到达=补见。
    # 同批内桶关闭前已经到达的事件 observed_at < 桶关闭时点，仍按正常事件处理，不误判迟到。
    with store.connect() as db:
        existing_ids = {r["event_id"] for r in db.execute("SELECT event_id FROM wm_events")}
    accepted, cross_duplicates = [], []
    for event in parsed["events"]:
        if event["event_id"] in existing_ids:
            cross_duplicates.append({
                "_line": event["source_line"], "_sources": [event["source_line"]],
                "_raw": event["raw"], "reason": "DUPLICATE_EVENT",
                "detail": "事件号已在此前批次建档，重复提交不覆盖原结论",
            })
        else:
            accepted.append(event)
    parsed["events"] = accepted
    parsed["quarantine"].extend(cross_duplicates)
    parsed["metrics"]["quarantine"] = len(parsed["quarantine"])
    parsed["metrics"]["accepted"] = len(accepted)
    for event in parsed["events"]:
        event["late"] = is_late_arrival(
            event["event_time"], event["observed_at"], coverage_frontier, bucket)
    all_events = _all_known_events(parsed["events"])
    parsed["gaps"] = rebuild_gaps(all_events, observed_frontier, coverage_frontier, bucket)
    return store.commit_scan(key, scan_time, raw, params, parsed, observed_frontier, coverage_frontier)


def _all_known_events(new_events):
    """空档重建需基于全量已知事件，而非仅本批；事件从持久台账回读合并。"""
    with store.connect() as db:
        known = [
            {"event_time": datetime.fromisoformat(r["event_time"]),
             "observed_at": datetime.fromisoformat(r["observed_at"])}
            for r in db.execute("SELECT event_time,observed_at FROM wm_events")
        ]
    known.extend({"event_time": e["event_time"], "observed_at": e["observed_at"]} for e in new_events)
    return known


def snapshot():
    return store.load_snapshot()


def reset_all():
    store.reset()
