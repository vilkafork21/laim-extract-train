"""Строгий генератор совместимого ``selection`` schema v2."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, NoReturn


SCHEMA_VERSION = 2
NS_PER_DAY = 86_400 * 10**9
MAX_NS = 2**63 - 1
SCAN_BACK_DAYS = 3
SCAN_FORWARD_DAYS = 1
MSK = timezone(timedelta(hours=3), name="MSK")

SOURCE = {
    "source_id": "cdio_prod",
    "label": "Прод-витрина ЦДИО",
    "table": "t_team_cdiotraces.aef_trace_expanded",
    "agent_column": "agent_id",
    "version_column": "distributive",
    "start_column": "start_time_ns",
    "end_column": "end_time_ns",
    "trace_column": "trace_id",
    "span_column": "span_id",
}

IDENTITY_FIELDS = (
    "schema_version",
    "agent_ci",
    "distributive",
    "date_from",
    "date_to",
    "tz",
    "ts_ns",
    "te_ns",
    "scan_lo_ns",
    "scan_hi_ns",
    "modes",
    "unit",
    "target_consumer",
    "require_agent_span",
    "vol_max",
    "hard_max_spans",
    "test_pct",
    "split_method",
    "seed",
    "detector_enabled",
    "consistency_mode",
    "strict_full_trace",
)

REQUIRED_SELECTION_KEYS = {
    *IDENTITY_FIELDS,
    "source",
    "selection_id",
    "run_id",
    "artifact_scope",
    "event_id",
    "params_source",
}


def fail(message: str) -> NoReturn:
    raise ValueError(f"профиль временной выборки: {message}")


def parse_json_object(value: Any, name: str) -> dict[str, Any]:
    if isinstance(value, str) and value.strip():
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            fail(f"{name} пришёл строкой, но это не JSON: {error}")
    if not isinstance(value, dict) or not value:
        fail(f"{name} должен быть непустым JSON object")
    return dict(value)


def parse_date(value, name: str) -> date:
    text = str(value or "").strip()
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", text):
        fail(f"{name} должна быть датой ГГГГ-ММ-ДД, получено: {text!r}")
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        fail(f"{name} должна быть датой ГГГГ-ММ-ДД, получено: {text!r}")


def timezone_for(code: str):
    if code == "Europe/Moscow":
        return MSK
    if code == "UTC":
        return timezone.utc
    fail(f"tz должен быть Europe/Moscow или UTC, получено: {code!r}")


def safe_value(value, name: str, max_len: int = 1000) -> str:
    text = str(value or "").strip()
    if len(text) > max_len:
        fail(f"{name} длиннее {max_len} символов")
    for forbidden in ("'", '"', ";", "--", "\\", "\n", "\r"):
        if forbidden in text:
            fail(f"недопустимая последовательность {forbidden!r} в {name}")
    return text


def canonical_agent(value) -> str:
    agent = safe_value(value, "agent_id").upper()
    if not re.fullmatch(r"C[IE][0-9]{6,12}", agent):
        fail(f"agent_id должен быть CI/CE и 6–12 цифр, получено: {agent!r}")
    return agent


def _day_start_ns(day: date, tz) -> int:
    moment = datetime(day.year, day.month, day.day, tzinfo=tz)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = moment.astimezone(timezone.utc) - epoch
    seconds = delta.days * 86_400 + delta.seconds
    return seconds * 10**9 + delta.microseconds * 1000


def _canonical_hash(payload: dict) -> str:
    raw = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def selection_id(selection: dict) -> str:
    payload = {key: selection[key] for key in IDENTITY_FIELDS}
    payload["source"] = {
        key: value for key, value in selection["source"].items() if key != "label"
    }
    return _canonical_hash(payload)


def build_selection(
    *,
    agent_id,
    distributive,
    date_from,
    date_to,
    tz: str,
    profile_role: str,
    expected_min_traces: int,
) -> dict:
    """Собрать v2 manifest, не открывая оператору защитные параметры."""
    agent = canonical_agent(agent_id)
    version = safe_value(distributive, "distributive")
    if not version:
        fail("distributive обязателен для пары agent × distributive")

    tz_code = str(tz or "").strip()
    tzinfo = timezone_for(tz_code)
    day_from = (
        date_from if isinstance(date_from, date) else parse_date(date_from, "date_from")
    )
    day_to = date_to if isinstance(date_to, date) else parse_date(date_to, "date_to")
    if day_from > day_to:
        fail(f"date_from {day_from} позже date_to {day_to}")

    ts_ns = _day_start_ns(day_from, tzinfo)
    te_ns = _day_start_ns(day_to + timedelta(days=1), tzinfo)
    scan_lo_ns = ts_ns - SCAN_BACK_DAYS * NS_PER_DAY
    scan_hi_ns = te_ns + SCAN_FORWARD_DAYS * NS_PER_DAY
    if scan_lo_ns < 0:
        fail("период до 1970-01-04 не поддерживается наносекундным контрактом")
    if scan_hi_ns > MAX_NS:
        fail("период не помещается в signed BIGINT наносекунд витрины")
    semantic = {
        "schema_version": SCHEMA_VERSION,
        "source": dict(SOURCE),
        "agent_ci": agent,
        "distributive": version,
        "date_from": day_from.isoformat(),
        "date_to": day_to.isoformat(),
        "tz": tz_code,
        "ts_ns": ts_ns,
        "te_ns": te_ns,
        "scan_lo_ns": scan_lo_ns,
        "scan_hi_ns": scan_hi_ns,
        # full + before эквивалентны принадлежности по max(end_time_ns).
        "modes": ["full", "before"],
        "unit": "trace",
        "target_consumer": "dataset_converter",
        "require_agent_span": True,
        "vol_max": 0,
        "hard_max_spans": 0,
        # Поля split обязательны в v2, но не используются при detector_enabled=false.
        "test_pct": 20,
        "split_method": "random",
        "seed": "42",
        "detector_enabled": False,
        "consistency_mode": "snapshot_table",
        "strict_full_trace": True,
    }
    digest = selection_id(semantic)
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        + "_"
        + uuid.uuid4().hex[:8]
    )
    selection = {
        **semantic,
        "selection_id": digest,
        "run_id": run_id,
        "artifact_scope": f"sel_{digest[:12]}_{run_id[-8:]}",
        "event_id": "",
        "params_source": "manual",
    }
    report_meta = {
        "schema_version": SCHEMA_VERSION,
        "profile_role": profile_role,
        "selection_id": digest,
        "run_id": run_id,
        "source_id": SOURCE["source_id"],
        "table": SOURCE["table"],
        "agent_ci": agent,
        "distributive": version,
        "date_from": day_from.isoformat(),
        "date_to": day_to.isoformat(),
        "tz": tz_code,
        "expected_min_traces": int(expected_min_traces),
    }
    return {"selection": selection, "report_meta": report_meta}


def validate_selection(value: Any, name: str) -> dict[str, Any]:
    """Проверить входной selection теми же инвариантами, что ожидает Query v2."""
    selection = parse_json_object(value, name)
    missing = sorted(REQUIRED_SELECTION_KEYS - set(selection))
    if missing:
        fail(f"{name} не содержит обязательные поля: {missing}")
    if selection.get("schema_version") != SCHEMA_VERSION:
        fail(f"{name}.schema_version должен быть {SCHEMA_VERSION}")
    source = selection.get("source")
    if not isinstance(source, dict):
        fail(f"{name}.source должен быть object")
    for key, source_expected in SOURCE.items():
        if key not in source:
            fail(f"{name}.source не содержит поле {key!r}")
        if key != "label" and source[key] != source_expected:
            fail(
                f"{name}.source.{key}={source.get(key)!r}, "
                f"ожидалось {source_expected!r}"
            )
    if not isinstance(source["label"], str) or not source["label"].strip():
        fail(f"{name}.source.label должен быть непустой строкой")

    agent = canonical_agent(selection["agent_ci"])
    if selection["agent_ci"] != agent:
        fail(f"{name}.agent_ci должен быть записан канонически как {agent}")
    version = safe_value(selection["distributive"], f"{name}.distributive")
    if not version:
        fail(f"{name}.distributive не должен быть пустым")
    if selection["distributive"] != version:
        fail(f"{name}.distributive содержит внешние пробелы")
    day_from = parse_date(selection["date_from"], f"{name}.date_from")
    day_to = parse_date(selection["date_to"], f"{name}.date_to")
    if selection["date_from"] != day_from.isoformat():
        fail(f"{name}.date_from должен быть строкой ГГГГ-ММ-ДД")
    if selection["date_to"] != day_to.isoformat():
        fail(f"{name}.date_to должен быть строкой ГГГГ-ММ-ДД")
    if day_from > day_to:
        fail(f"{name}: date_from позже date_to")
    tz_code = str(selection["tz"]).strip()
    if selection["tz"] != tz_code:
        fail(f"{name}.tz содержит внешние пробелы")
    tzinfo = timezone_for(tz_code)
    expected_ts = _day_start_ns(day_from, tzinfo)
    expected_te = _day_start_ns(day_to + timedelta(days=1), tzinfo)
    expected_scan_hi = expected_te + SCAN_FORWARD_DAYS * NS_PER_DAY
    if expected_scan_hi > MAX_NS:
        fail(f"{name}: период не помещается в signed BIGINT наносекунд витрины")
    for field in ("ts_ns", "te_ns", "scan_lo_ns", "scan_hi_ns"):
        if isinstance(selection[field], bool) or not isinstance(selection[field], int):
            fail(f"{name}.{field} должен быть целым числом наносекунд")
    if selection["ts_ns"] != expected_ts or selection["te_ns"] != expected_te:
        fail(f"{name}: наносекундные границы не соответствуют бизнес-датам")
    if selection["scan_lo_ns"] != expected_ts - SCAN_BACK_DAYS * NS_PER_DAY:
        fail(f"{name}.scan_lo_ns не соответствует защитному окну")
    if selection["scan_hi_ns"] != expected_scan_hi:
        fail(f"{name}.scan_hi_ns не соответствует защитному окну")

    expected_invariants = {
        "modes": ["full", "before"],
        "unit": "trace",
        "target_consumer": "dataset_converter",
        "require_agent_span": True,
        "vol_max": 0,
        "hard_max_spans": 0,
        "test_pct": 20,
        "split_method": "random",
        "seed": "42",
        "detector_enabled": False,
        "consistency_mode": "snapshot_table",
        "strict_full_trace": True,
    }
    for key, invariant_expected in expected_invariants.items():
        if selection.get(key) != invariant_expected:
            fail(
                f"{name}.{key}={selection.get(key)!r}, ожидалось {invariant_expected!r}"
            )
    computed = selection_id(selection)
    if selection.get("selection_id") != computed:
        fail(f"{name}.selection_id не соответствует содержимому selection")
    run_id = selection.get("run_id")
    if not isinstance(run_id, str) or not re.fullmatch(
        r"[0-9]{8}T[0-9]{6}_[0-9a-f]{8}", run_id
    ):
        fail(f"{name}.run_id имеет неподдерживаемый формат")
    expected_scope = f"sel_{computed[:12]}_{run_id[-8:]}"
    if selection.get("artifact_scope") != expected_scope:
        fail(f"{name}.artifact_scope не соответствует selection_id/run_id")
    if selection.get("event_id") != "" or selection.get("params_source") != "manual":
        fail(f"{name} должен быть сформирован ручным fixed/rolling профилем")
    return selection


def same_identity(left: dict, right: dict) -> bool:
    source_fields = tuple(key for key in SOURCE if key != "label")
    return (
        all(
            left["source"].get(key) == right["source"].get(key) for key in source_fields
        )
        and left["agent_ci"] == right["agent_ci"]
        and left["distributive"] == right["distributive"]
        and left["tz"] == right["tz"]
    )
