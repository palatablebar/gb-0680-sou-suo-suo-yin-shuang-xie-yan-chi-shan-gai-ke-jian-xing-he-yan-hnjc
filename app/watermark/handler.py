from flask import Blueprint, jsonify, render_template, request

from app.config import MODULES, RULE_VERSION, SHORT, TITLE
from app.errors import DomainError
from app.watermark import service
from app.watermark.archive import build_watermark_archive

bp = Blueprint("watermark", __name__)


def _raw_from_payload(payload):
    if "csv" in payload:
        csv_text = payload["csv"]
        if not isinstance(csv_text, str):
            raise DomainError("BAD_CSV", "CSV需要文本")
        return csv_text.encode()
    sample = payload.get("sample", "dirty.csv")
    from app.pipeline import load_sample
    return load_sample(sample)


@bp.get("/api/watermark")
def get_watermark():
    snap = service.snapshot()
    return jsonify(snap or {"empty": True, "rule_version": RULE_VERSION})


@bp.post("/api/watermark/scan")
def post_scan():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        raise DomainError("BAD_BODY", "请求需要对象")
    params = payload.get("parameters", {})
    if not isinstance(params, dict):
        raise DomainError("BAD_PARAMETERS", "参数需要对象")
    raw = _raw_from_payload(payload)
    return jsonify(service.run_scan(raw, params))


@bp.post("/api/watermark/upload")
def upload_scan():
    from flask import request as flask_request
    file = flask_request.files.get("file")
    if not file:
        raise DomainError("NO_FILE", "请选择CSV文件")
    params = {k: v for k, v in flask_request.args.items()}
    return jsonify(service.run_scan(file.read(), params))


@bp.post("/api/watermark/reset")
def reset_watermark():
    service.reset_all()
    return jsonify({"reset": True})


@bp.get("/api/watermark/archive")
def watermark_archive():
    snap = service.snapshot()
    if snap is None:
        raise DomainError("NOT_FOUND", "尚无核验水位，请先执行扫描", 404)
    from flask import current_app
    return current_app.response_class(
        build_watermark_archive(snap),
        mimetype="text/csv",
        headers={"Content-Disposition": 'attachment; filename="watermark-evidence.csv"'},
    )


@bp.get("/watermark")
def watermark_page():
    snap = service.snapshot()
    return render_template(
        "watermark.html",
        title=TITLE, short=SHORT, modules=MODULES, index=len(MODULES),
        snap=snap, rule_version=RULE_VERSION, active_watermark=True,
    )
