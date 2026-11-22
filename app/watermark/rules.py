"""水位裁决纯规则层。

裁决要点：
- 事件时间(source_time)与到达时间(observed_at)分离；到达时间只决定巡检看到哪里，
  不能直接当作已核验水位。
- observed_frontier：观测前沿=最大到达时刻，只进不退。
- coverage_frontier：覆盖水位=观测前沿回退安全余量后对齐桶边界，只进不退、永不越过观测前沿。
- 安全余量=max(允许迟到量, 桶宽)，保证一个成熟桶之后仍有完整一轮机会补见迟到事件。
- 已覆盖区间内到达的迟到事件标记 late_backfill，可补见、可重算，不回退水位。
所有结论仅取决于输入与显式 scan_time，重放同一批次结果逐字节一致。
"""
import csv, io
from datetime import datetime, timedelta, timezone
from app.config import COLUMNS
from app.errors import DomainError
from app.modules.m1_streams.rules import decode
from app.reasons import reject
from app.values import instant

WM_COLUMNS = COLUMNS + ["observed_at"]
DEFAULT_BUCKET_SECONDS = 3600
DEFAULT_LATENESS_SECONDS = 3600
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def int_param(value, default, code_message):
    if value in (None, ""):
        return default
    try:
        result = int(str(value))
    except (TypeError, ValueError):
        raise DomainError("INVALID_NUMBER", code_message)
    if result <= 0:
        raise DomainError("INVALID_NUMBER", code_message)
    return result


def floor_bucket(moment, bucket_seconds):
    seconds = int((moment - EPOCH).total_seconds())
    return EPOCH + timedelta(seconds=(seconds // bucket_seconds) * bucket_seconds)


def safety_margin(bucket_seconds, allowed_lateness_seconds):
    # 余量至少一个桶宽：成熟桶关闭后仍保留一整轮回看窗口承接迟到事件
    return max(int(bucket_seconds), int(allowed_lateness_seconds))


def advance_coverage(observed_frontier, coverage_frontier, bucket_seconds, allowed_lateness_seconds):
    """覆盖水位前移的唯一入口：只进不退，且永不越过观测前沿。"""
    if observed_frontier is None:
        return coverage_frontier
    base = coverage_frontier or floor_bucket(observed_frontier, bucket_seconds)
    candidate = floor_bucket(
        observed_frontier - timedelta(seconds=safety_margin(bucket_seconds, allowed_lateness_seconds)),
        bucket_seconds,
    )
    cap = floor_bucket(observed_frontier, bucket_seconds)
    return max(base, min(candidate, cap))


def parse_scan(raw, params, scan_time):
    """解析一次小时增量扫描，返回事件、隔离行与本批观测前沿。不触碰任何持久状态。"""
    text, encoding = decode(raw)
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, None)
    if header not in (COLUMNS, WM_COLUMNS):
        raise DomainError("BAD_HEADER", "表头不匹配，需为双写事件列，可追加 observed_at 列")
    has_obs = header == WM_COLUMNS
    default_observed = params.get("observed_at")
    events, quarantine, seen = [], [], set()
    max_observed = None
    input_rows = duplicate = 0
    for cells in reader:
        input_rows += 1
        if not cells or not any(c.strip() for c in cells):
            continue
        line = reader.line_num
        row = {"_line": line, "_sources": [line], "_raw": cells}
        if len(cells) != len(header):
            quarantine.append(reject(row, "BAD_COLUMNS", "列数不匹配"))
            continue
        data = dict(zip(header, [c.strip() for c in cells]))
        try:
            event_time = instant(data["source_time"])
        except DomainError as error:
            quarantine.append(reject(row, error.code, error.message))
            continue
        observed_text = data.get("observed_at") if has_obs else None
        observed_text = observed_text or default_observed
        if not observed_text:
            quarantine.append(reject(row, "EMPTY_VALUE", "缺少到达时刻 observed_at"))
            continue
        try:
            observed_at = instant(observed_text)
        except DomainError as error:
            quarantine.append(reject(row, error.code, error.message))
            continue
        if observed_at > scan_time:
            quarantine.append(reject(row, "FUTURE_ARRIVAL", "到达时刻晚于本次扫描时刻，不予采信"))
            continue
        event_id = data["event_id"]
        if not event_id:
            quarantine.append(reject(row, "EMPTY_VALUE", "业务键为空"))
            continue
        if event_id in seen:
            duplicate += 1
            quarantine.append(reject(row, "DUPLICATE_EVENT", "同批事件号重复，保留首条"))
            continue
        seen.add(event_id)
        event = {
            "event_id": event_id,
            "doc_id": data.get("doc_id", ""),
            "version": data.get("version", ""),
            "op": data.get("op", ""),
            "side": data.get("side", ""),
            "event_time": event_time,
            "observed_at": observed_at,
            "source_line": line,
            "raw": cells,
        }
        events.append(event)
        max_observed = observed_at if max_observed is None else max(max_observed, observed_at)
    return {
        "events": events,
        "quarantine": quarantine,
        "metrics": {
            "input": input_rows,
            "accepted": len(events),
            "quarantine": len(quarantine),
            "duplicate": duplicate,
            "encoding": encoding,
            "batch_observed_max": max_observed.isoformat() if max_observed else None,
            "no_arrival": max_observed is None,
        },
    }


def is_late_arrival(event_time, observed_at, coverage_frontier, bucket_seconds):
    """事件所在桶已随覆盖水位成熟关闭，事件却在关闭之后才到达 => 迟到回补。

    覆盖水位采用区间 [start,end) 语义：event_time 所属桶的关闭时点为
    floor(event_time)+桶宽。当覆盖水位已越过该关闭时点、且到达时刻不早于关闭时点，
    说明桶是在事件缺席的情况下被关闭的，事件属于补见而非正常到达，绝不跳过。
    """
    if coverage_frontier is None:
        return False
    bucket_close = floor_bucket(event_time, bucket_seconds) + timedelta(seconds=bucket_seconds)
    return coverage_frontier >= bucket_close and observed_at >= bucket_close


def classify_event(event, coverage_frontier, observed_frontier):
    if event["late"]:
        return "LATE_BACKFILL"
    if event["event_time"] < coverage_frontier:
        return "COVERED"
    if event["event_time"] < observed_frontier:
        return "IN_WINDOW"
    return "FUTURE_EVENT"


def rebuild_gaps(events, observed_frontier, coverage_frontier, bucket_seconds):
    """在观测前沿以内逐桶核对：已覆盖空桶=空档，未过水位桶=未覆盖（等待成熟）。"""
    if observed_frontier is None:
        return []
    occupied = {floor_bucket(event["event_time"], bucket_seconds) for event in events}
    starts = [floor_bucket(event["event_time"], bucket_seconds) for event in events]
    start = min(starts) if starts else floor_bucket(observed_frontier, bucket_seconds)
    end = floor_bucket(observed_frontier, bucket_seconds)
    rows = []
    bucket = start
    while bucket < end:
        if bucket in occupied:
            pass  # 含事件桶既不是空档也不是未覆盖，结论在事件台账
        elif bucket >= coverage_frontier:
            rows.append({"bucket_start": bucket.isoformat(), "reason": "UNCOVERED_PENDING",
                         "detail": "空桶未成熟，迟到窗口尚未关闭"})
        else:
            rows.append({"bucket_start": bucket.isoformat(), "reason": "GAP_EMPTY",
                         "detail": "已覆盖桶内无任何事件，登记空档"})
        bucket += timedelta(seconds=bucket_seconds)
    return rows
