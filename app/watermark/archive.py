"""核验水位的证据导出。页面结论与本导出均取自 service.snapshot() 的同一快照，保证一致。"""
import csv
import io

REASON_TEXT = {
    "GAP_EMPTY": "已覆盖空档：桶内无事件",
    "UNCOVERED_PENDING": "未覆盖：桶未成熟，迟到窗口未关闭",
    "NO_ARRIVAL": "无到达：本批没有任何有效事件，水位不前移",
    "LATE_BACKFILL": "迟到回补：到达时事件时间已低于覆盖水位，已补见不跳过",
    "COVERED": "已覆盖：事件时间在覆盖水位之前并已裁定",
    "IN_WINDOW": "观察窗内：已到达但所在桶尚未成熟",
    "FUTURE_EVENT": "前沿之外：事件时间不低于观测前沿",
}

CLASS_TEXT = {
    "LATE_BACKFILL": "迟到回补",
    "COVERED": "已覆盖",
    "IN_WINDOW": "观察窗内",
    "FUTURE_EVENT": "前沿之外",
}


def build_watermark_archive(snap):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    wm = snap["watermark"]
    writer.writerow(["#watermark", "observed_frontier", wm["observed_frontier"],
                     "coverage_frontier", wm["coverage_frontier"],
                     "bucket_seconds", wm["bucket_seconds"],
                     "allowed_lateness_seconds", wm["allowed_lateness_seconds"],
                     "moved", wm["moved"]])
    writer.writerow(["run_id", "kind", "key", "event_time", "observed_at",
                     "classification", "reason", "source_lines", "evidence"])
    scan_id = (snap.get("scan") or {}).get("scan_key", "")
    for event in snap["events"]:
        writer.writerow([
            scan_id, "event", event["event_id"], event["event_time"], event["observed_at"],
            CLASS_TEXT.get(event["classification"], event["classification"]),
            REASON_TEXT.get(event["classification"], ""),
            event["source_line"], _json(event["raw"]),
        ])
    for gap in snap["gaps"]:
        writer.writerow([
            scan_id, "gap", gap["bucket_start"], gap["bucket_start"], "",
            gap["reason"], REASON_TEXT.get(gap["reason"], gap["detail"]),
            gap["updated_by_scan"], _json({"detail": gap["detail"]}),
        ])
    for row in snap["quarantine"]:
        writer.writerow([
            row["scan_key"], "quarantine", "", "", "",
            row["reason"], row["detail"], row.get("source_line", ""), _json(row["raw"]),
        ])
    writer.writerow([
        scan_id, "summary", "counts", "", "", "", "", "", _json(snap["counts"]),
    ])
    return "﻿" + stream.getvalue()


def _json(value):
    import json
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
