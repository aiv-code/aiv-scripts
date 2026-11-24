#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 AIV Cluster Log Consolidator v3.1  -  join by time + trace + user + run across hosts
================================================================================
 Pure Python 3.7+ standard library. Runs on RHEL / Rocky / Alma 8, 9, 10 and Windows.

 INPUTS
   TCP 5044  plain-text lines (any common format)
   TCP 5045  JSON lines  ({"ts":..., "hostname":..., "channel":"db", ...})
   Both ports accept both formats; the port only decides the default channel.
   File watch (--watch / LOG_INPUT_DIR)
     nested folders by default; mixed plain+JSON lines; continuous tail

 OUTPUTS (in LOG_OUTPUT_DIR)
   consolidated.log    ONE human-readable log from every server, sorted by time
   consolidated.jsonl  the same events as structured JSON, one per line
   all-plain / core / db / data / security .log   raw copies (TCP only)

 WEB UI  http://<host>:8088
   Live      time-ordered stream, filters re-apply to history
   Traces    strong traces only (skips Schedule/Scheduler/empty), cross-host
   Users     every user, their traces and errors
   Flow      click a trace / user / run: server lanes, hop latency, slowest hop
   Export    download the current filtered view as .log / .jsonl / .csv

 HOW SERVERS ARE MERGED + LINED UP
   Lines from different servers arrive out of order (network lag, shipper
   batching). Each line is held briefly and released in timestamp order once
   every *active* server has reported past that timestamp (a per-source
   watermark). A silent server never blocks the others for more than
   REORDER_IDLE_SEC, and nothing is held longer than REORDER_MAX_HOLD_SEC.
   After release, missing user/trace/run on a hop are filled from a short
   correlation index keyed by the same trace or runKey seen on other hosts
   (so API + Kafka + job lines for one Flow stitch together in the UI).

 AIV plain format - matches aiv/.../logback-spring.xml:
   CONSOLE / all-plain:
     %d %-5level [%thread] %logger{0}
     [lane=${LANE} host=${HOSTNAME} role=... pid=... port=...
      trace=%X{traceid} user=%X{actor} dept=%X{deptCode} run=%X{runKey}
      (plain also: from=%X{fromNode} src=%X{sourceClass})] - %msg
   JSON channel files also carry MDC: traceid, actor, deptCode, runKey, fromNode
   plus customFields hostname/role/pid/port/lane.

 EXTRA MODES
   --demo                   simulate 4 servers with skewed clocks and lag
   --watch DIR              continuously tail nested log files (mixed format)
                            default: /var/lib/aiv/logs when that folder exists
   --merge A.log B.log -o   merge existing log files offline into one
================================================================================
"""

import argparse
import asyncio
import csv
import datetime
import gzip
import heapq
import io
import json
import os
import random
import re
import shutil
import signal
import sys
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, urlparse


# ==============================================================================
# Configuration (all overridable with environment variables)
# ==============================================================================
def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def _parse_offsets(spec):
    out = {}
    for part in (spec or "").split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            try:
                out[k.strip()] = float(v)
            except ValueError:
                pass
    return out


# AIV writes its logs to /var/lib/aiv/logs. Used when --watch / LOG_INPUT_DIR is not given.
DEFAULT_WATCH_DIR = "/var/lib/aiv/logs"
DEFAULT_OUTPUT_DIR = "/var/log/aiv-consolidated"
LOG_OUTPUT_DIR = os.environ.get(
    "LOG_OUTPUT_DIR",
    DEFAULT_OUTPUT_DIR if os.path.isdir(DEFAULT_OUTPUT_DIR) else os.path.join(os.getcwd(), "logs"))
LOG_INPUT_DIR = os.environ.get(
    "LOG_INPUT_DIR", DEFAULT_WATCH_DIR if os.path.isdir(DEFAULT_WATCH_DIR) else "")
BIND_ADDR = os.environ.get("CONSOLIDATOR_BIND", "0.0.0.0")
HTTP_BIND_ADDR = os.environ.get("CONSOLIDATOR_HTTP_BIND", BIND_ADDR)
PLAIN_PORT = _env_int("CONSOLIDATOR_PLAIN_PORT", 5044)
JSON_PORT = _env_int("CONSOLIDATOR_JSON_PORT", 5045)
HTTP_PORT = _env_int("CONSOLIDATOR_HTTP_PORT", 8088)

BATCH_FLUSH_INTERVAL = _env_float("BATCH_FLUSH_INTERVAL", 0.5)
MAX_BATCH_LINES = _env_int("MAX_BUFFER_LINES", 5000)
MAX_MEMORY_HISTORY = _env_int("MAX_MEMORY_HISTORY", 50000)
# the newest lines of EACH host are also kept, so a one-host filter is answered from memory
MEMORY_PER_HOST = _env_int("MEMORY_PER_HOST", 20000)
HOST_RELOAD_MB = _env_int("HOST_RELOAD_MB", 128)      # how far back a restart reads to refill them
QUEUE_LIMIT = _env_int("WRITER_QUEUE_LIMIT", 500000)
MAX_LINE_BYTES = _env_int("MAX_LINE_BYTES", 1024 * 1024)
MAX_MSG_CHARS = _env_int("MAX_MSG_CHARS", 16000)
RETENTION_DAYS = _env_int("ARCHIVE_RETENTION_DAYS", 30)

REORDER_IDLE_SEC = _env_float("REORDER_IDLE_SEC", 2.0)
REORDER_MAX_HOLD_SEC = _env_float("REORDER_MAX_HOLD_SEC", 10.0)
FUTURE_TOLERANCE_SEC = _env_float("FUTURE_TOLERANCE_SEC", 60.0)

# File watch: nested tree, mixed formats, continuous tail (overridable via CLI)
WATCH_STRUCTURE = os.environ.get("WATCH_STRUCTURE", "nested").lower()  # nested|flat
WATCH_FORMAT = os.environ.get("WATCH_FORMAT", "mixed").lower()         # mixed|json|plain
WATCH_MODE = os.environ.get("WATCH_MODE", "continuous").lower()        # continuous|once
WATCH_POLL_SEC = _env_float("WATCH_POLL_SEC", 1.0)
WATCH_DISCOVER_SEC = _env_float("WATCH_DISCOVER_SEC", 5.0)
WATCH_FROM_START = os.environ.get("WATCH_FROM_START", "0").lower() in ("1", "true", "yes", "y")
# When continuous starts at EOF, still load this many recent bytes so the UI is not empty.
WATCH_CATCHUP_BYTES = _env_int("WATCH_CATCHUP_BYTES", 2 * 1024 * 1024)

# Timestamps without a timezone: "local" (consolidator's zone) or "utc"
NAIVE_TS_UTC = os.environ.get("NAIVE_TS_TZ", "local").lower() == "utc"
# Timestamps written to consolidated.log / UI: "local" or "utc"
OUT_UTC = os.environ.get("CONSOLIDATED_TZ", "local").lower() == "utc"
# Per-server clock correction in seconds, e.g. "db01=-1.2,web02=+0.4"
HOST_OFFSETS = _parse_offsets(os.environ.get("HOST_TIME_OFFSETS", ""))

KNOWN_CHANNELS = ("core", "db", "data", "security")
RAW_FILES = ["all-plain.log"] + [c + ".log" for c in KNOWN_CHANNELS]
CONSOLIDATED_LOG = "consolidated.log"
CONSOLIDATED_JSONL = "consolidated.jsonl"
ALL_FILES = RAW_FILES + [CONSOLIDATED_LOG, CONSOLIDATED_JSONL]
WATCH_EXTS = {".log", ".txt", ".jsonl", ".out"}
# App logback rolls into LOG_DIR/archived/*.log.gz - include those.
# Still never walk the consolidator's own output archived/ (feedback loop).
WATCH_SKIP_DIRS = {"__pycache__", ".git", "node_modules", ".svn"}
# Never re-ingest consolidator outputs. App channel files (core.log etc.) stay watchable.
WATCH_SKIP_NAMES = {CONSOLIDATED_LOG, CONSOLIDATED_JSONL, "birt", "birt.1", "birt.lck", "birt.1.lck"}
# logback-spring.xml rolls: all-plain.YYYY-MM-DD.i.log.gz, core.YYYY-MM-DD.i.log.gz, ...
WATCH_INCLUDE_ARCHIVED = os.environ.get("WATCH_INCLUDE_ARCHIVED", "1").lower() in (
    "1", "true", "yes", "y")
# Skip rolled .gz archives older than this many days on first sight (0 = read all).
WATCH_ARCHIVE_DAYS = _env_float("WATCH_ARCHIVE_DAYS", 1.0)
# all-plain.log and the channel files (core/db/security...) carry the same events.
# Keep the first copy, drop the same line when it shows up from a different file.
DEDUP_ACROSS_FILES = os.environ.get("DEDUP_ACROSS_FILES", "1").lower() in ("1", "true", "yes", "y")
DEDUP_CACHE = _env_int("DEDUP_CACHE", 300000)
# Remembers read positions so a restart does not re-read (and duplicate) old lines.
WATCH_STATE_FILE = os.environ.get("WATCH_STATE_FILE", "")
HEARTBEAT_SEC = _env_float("HEARTBEAT_SEC", 60.0)


# ==============================================================================
# Parsing: turn any log line (plain or JSON) into one normalised event
# ==============================================================================
LEVEL_RANK = {"TRACE": 5, "DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40, "FATAL": 50}
LEVEL_ALIASES = {
    "WARNING": "WARN", "ERR": "ERROR", "SEVERE": "ERROR", "CRIT": "FATAL",
    "CRITICAL": "FATAL", "EMERG": "FATAL", "ALERT": "FATAL", "NOTICE": "INFO",
    "FINE": "DEBUG", "VERBOSE": "TRACE",
}
BUNYAN_LEVELS = {10: "TRACE", 20: "DEBUG", 30: "INFO", 40: "WARN", 50: "ERROR", 60: "FATAL"}

MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}

RE_ISO = re.compile(
    r"(?<!\d)(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:[.,](\d{1,9}))?(Z|[+-]\d{2}:?\d{2})?")
RE_APACHE = re.compile(
    r"(?<!\d)(\d{2})/([A-Z][a-z]{2})/(\d{4}):(\d{2}):(\d{2}):(\d{2})(?: ([+-]\d{4}))?")
RE_SYSLOG = re.compile(r"\b([A-Z][a-z]{2})\s{1,2}(\d{1,2}) (\d{2}):(\d{2}):(\d{2})\b")
RE_LEVEL = re.compile(r"\b(TRACE|DEBUG|INFO|NOTICE|WARN(?:ING)?|ERROR|ERR|SEVERE|CRIT(?:ICAL)?|FATAL|EMERG|ALERT)\b")
RE_KV = re.compile(r"""(?<![\w.\-])([A-Za-z_][\w.\-]*)=("[^"]*"|'[^']*'|[^\s\]\[,;"']+)""")
# AIV logback allows empty values: trace= user= dept= run=
RE_KV_ALLOW_EMPTY = re.compile(r"""(?<![\w.\-])([A-Za-z_][\w.\-]*)=("[^"]*"|'[^']*'|[^\s\]\[,;"']*)""")
RE_KV_COLON = re.compile(
    r"\b(trace[_-]?id|request[_-]?id|correlation[_-]?id|user[_-]?id|username)\s*:\s*([\w\-.@]+)", re.I)
# Full AIV line from logback CONSOLE_PATTERN / PLAIN_PATTERN
#   2026-11-11 09:37:09.385 INFO  [thread] logger [lane=... host=... trace=... user=... run=...] - msg
RE_AIV_LINE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,9})?)\s+"
    r"(?P<level>TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL)\s+"
    r"\[(?P<thread>[^\]]*)\]\s+"
    r"(?P<logger>\S+)\s+"
    r"\[(?P<mdc>[^\]]*)\]\s+-\s+(?P<msg>.*)$",
    re.I,
)
# Fallback: MDC bracket only (when line is truncated / wrapped)
RE_AIV_BRACKET = re.compile(
    r"\[((?:[^\]]*?\b(?:lane|host|role|pid|port|trace|user|dept|run|from|src)=)[^\]]*)\]")
RE_USER_IN_MSG = re.compile(r"""\b(?:user[_-]?id|username|user|login)["']?\s*[=:]\s*["']?([\w\-.@]+)""", re.I)
RE_HOSTNAME = re.compile(r"^[A-Za-z0-9][\w.\-]*$")
RE_EPOCH = re.compile(r"^\d{9,19}(\.\d+)?$")

KEYMAP = {
    "host": ("host", "hostname", "server", "node", "nodename", "machine", "publishernode", "fromnode"),
    "trace": ("traceid", "trace_id", "trace", "requestid", "request_id", "reqid", "req_id",
              "correlationid", "correlation_id", "x-request-id", "txid", "transaction_id"),
    "user": ("user", "userid", "user_id", "uid", "username", "usr", "account", "loginid", "login",
             "actor", "owner"),
    "svc": ("service", "app", "application", "component", "module", "logger"),
    "run": ("run", "runkey", "run_key", "runid", "run_id"),
    "dept": ("dept", "deptcode", "dept_code", "dc", "vdc"),
    "lane": ("lane",),
    "from": ("from", "fromnode", "from_node"),
}
KEY_TO_FIELD = {k: field for field, keys in KEYMAP.items() for k in keys}
TS_KEYS = ("@timestamp", "timestamp", "ts", "time", "datetime", "asctime", "logtime", "t")
LEVEL_KEYS = ("level", "severity", "lvl", "levelname", "loglevel")
MSG_KEYS = ("message", "msg", "log", "text", "event", "error")

# Sentinels that must not become "traces" in the UI (collide across every job/host).
WEAK_TRACES = frozenset({
    "", "-", "null", "none", "n/a", "unknown",
    "schedule", "scheduler", "kafka", "internal", "spring auth", "auth",
    "api", "role", "queue",
})
WEAK_USERS = frozenset({"", "-", "null", "none", "n/a", "unknown", "system", "anonymous"})


def is_strong_trace(v):
    if not v:
        return False
    s = str(v).strip()
    return s.lower() not in WEAK_TRACES and len(s) >= 8


def is_strong_user(v):
    if not v:
        return False
    s = str(v).strip()
    return s.lower() not in WEAK_USERS


def is_strong_run(v):
    if not v:
        return False
    s = str(v).strip()
    return s.lower() not in WEAK_TRACES and ("_" in s or len(s) >= 3)


def _tz_from_str(s):
    if not s:
        return None
    if s == "Z":
        return datetime.timezone.utc
    sign = 1 if s[0] == "+" else -1
    digits = s[1:].replace(":", "")
    return datetime.timezone(sign * datetime.timedelta(hours=int(digits[:2]), minutes=int(digits[2:4])))


def _build_epoch(y, mo, d, hh, mm, ss, us, tz):
    try:
        if tz is None and NAIVE_TS_UTC:
            tz = datetime.timezone.utc
        return datetime.datetime(y, mo, d, hh, mm, ss, us, tzinfo=tz).timestamp()
    except (ValueError, OverflowError, OSError):
        return None


def parse_ts_text(line):
    """Find a timestamp in free text. Returns (epoch, kind, end_pos) or None."""
    head = line[:256]
    m = RE_ISO.search(head)
    if m:
        y, mo, d, hh, mm, ss, frac, tz = m.groups()
        us = int((frac + "000000")[:6]) if frac else 0
        ts = _build_epoch(int(y), int(mo), int(d), int(hh), int(mm), int(ss), us, _tz_from_str(tz))
        if ts is not None:
            return ts, "iso", m.end()
    m = RE_APACHE.search(head[:160])
    if m and m.group(2) in MONTHS:
        d, mon, y, hh, mm, ss, tz = m.groups()
        ts = _build_epoch(int(y), MONTHS[mon], int(d), int(hh), int(mm), int(ss), 0, _tz_from_str(tz))
        if ts is not None:
            return ts, "apache", m.end()
    m = RE_SYSLOG.search(head[:100])
    if m and m.group(1) in MONTHS:
        mon, d, hh, mm, ss = m.groups()
        year = datetime.datetime.now().year
        ts = _build_epoch(year, MONTHS[mon], int(d), int(hh), int(mm), int(ss), 0, None)
        if ts is not None and ts > time.time() + 86400:  # "Dec 31" read in January
            ts = _build_epoch(year - 1, MONTHS[mon], int(d), int(hh), int(mm), int(ss), 0, None)
        if ts is not None:
            return ts, "syslog", m.end()
    return None


def _epoch_num(x):
    for div in (1, 1e3, 1e6, 1e9):  # seconds, ms, us, ns
        t = x / div
        if 946684800 <= t <= 4102444800:
            return t
    return None


def coerce_ts(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return _epoch_num(float(v))
    if isinstance(v, str):
        s = v.strip()
        if RE_EPOCH.match(s):
            return _epoch_num(float(s))
        r = parse_ts_text(s)
        return r[0] if r else None
    return None


def normalize_level(v):
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return BUNYAN_LEVELS.get(int(v))
    s = str(v).strip().upper()
    s = LEVEL_ALIASES.get(s, s)
    return s if s in LEVEL_RANK else None


def _clean(v):
    s = str(v).strip().strip("\"'")
    if not s or s.lower() in ("-", "null", "none", "nil", "n/a", "unknown"):
        return None
    return s[:128]


def _apply_fields(ev, flat):
    for key, val in flat.items():
        field = KEY_TO_FIELD.get(key)
        if not field:
            continue
        cleaned = _clean(val)
        if cleaned and not ev.get(field):
            ev[field] = cleaned
        elif cleaned and field == "user" and cleaned != ev.get("user"):
            # second identity on the same line (e.g. user=42 username=alice): keep both searchable
            alt = ev.get("uname") or ""
            if cleaned.lower() not in alt.lower():
                ev["uname"] = (alt + " " + cleaned).strip()[:200]


def _extract_kv_map(text, allow_empty=False):
    """Parse key=value tokens; AIV brackets often have empty values (trace= user=)."""
    flat = {}
    rx = RE_KV_ALLOW_EMPTY if allow_empty else RE_KV
    for m in rx.finditer(text[:2000]):
        flat.setdefault(m.group(1).lower(), m.group(2))
    return flat


def _apply_aiv_mdc(ev, mdc_text):
    """Apply logback MDC bracket fields (trace/user/run/host/lane/from/src/...)."""
    flat = _extract_kv_map(mdc_text, allow_empty=True)
    _apply_fields(ev, flat)
    if flat.get("host"):
        cleaned = _clean(flat["host"])
        if cleaned:
            ev["host"] = cleaned
    return True


def _fill_from_aiv_line(ev, line):
    """Parse a full AIV logback CONSOLE/PLAIN line. Returns True if matched."""
    m = RE_AIV_LINE.match(line.rstrip("\r\n"))
    if not m:
        return False
    ts = coerce_ts(m.group("ts").replace(",", "."))
    if ts is not None:
        ev["ts"] = ts
    ev["level"] = normalize_level(m.group("level"))
    logger = m.group("logger")
    if logger and not ev.get("svc"):
        ev["svc"] = logger[:128]
    _apply_aiv_mdc(ev, m.group("mdc"))
    msg = m.group("msg")
    if msg is not None:
        ev["msg"] = msg
    return True


def _fill_from_aiv_bracket(ev, line):
    """Fallback: MDC bracket only (truncated / continuation-adjacent lines)."""
    m = RE_AIV_BRACKET.search(line[:2000])
    if not m:
        return False
    return _apply_aiv_mdc(ev, m.group(1))


def _is_scalar(v):
    return isinstance(v, (str, int, float)) and not isinstance(v, bool)


def _fill_from_json(ev, p):
    ch = p.get("channel")
    if isinstance(ch, str) and ch.strip():
        ev["ch"] = ch.strip().lower()
    flat, nested = {}, []
    for k, v in p.items():
        if isinstance(v, dict):
            nested.append(v)
        elif _is_scalar(v):
            flat[str(k).lower()] = v
    for d in nested:  # one level of nesting (e.g. "context": {"user": ...})
        for k, v in d.items():
            if _is_scalar(v):
                flat.setdefault(str(k).lower(), v)
    for k in TS_KEYS:
        if k in flat:
            ts = coerce_ts(flat[k])
            if ts:
                ev["ts"] = ts
                break
    _apply_fields(ev, flat)
    for k in LEVEL_KEYS:
        if k in flat:
            ev["level"] = normalize_level(flat[k])
            if ev["level"]:
                break
    msg = None
    for k in MSG_KEYS:
        if k in flat and str(flat[k]).strip():
            msg = str(flat[k])
            break
    ev["msg"] = msg if msg is not None else json.dumps(p, ensure_ascii=False, separators=(",", ":"))
    if not ev["level"]:
        m = RE_LEVEL.search(ev["msg"][:300])
        ev["level"] = normalize_level(m.group(1)) if m else None


def _fill_from_text(ev, line):
    # Prefer exact AIV logback line shape (CONSOLE_PATTERN / PLAIN_PATTERN)
    if _fill_from_aiv_line(ev, line):
        return
    found = parse_ts_text(line)
    ts_end = 0
    if found:
        ev["ts"], kind, ts_end = found
        if kind == "syslog":  # "Nov  9 12:00:01 web01 app[123]: msg"
            tok = line[ts_end:].split(None, 1)
            if tok and not tok[0].endswith(":") and RE_HOSTNAME.match(tok[0]):
                ev["host"] = tok[0]
    # AIV bracket fallback (handles empty trace=/user=/run=)
    _fill_from_aiv_bracket(ev, line)
    flat = _extract_kv_map(line[:1500], allow_empty=False)
    for m in RE_KV_COLON.finditer(line[:1500]):
        flat.setdefault(m.group(1).lower().replace("-", "_"), m.group(2))
    _apply_fields(ev, flat)
    if ev["ts"] is None:
        for k in TS_KEYS:
            if k in flat:
                ev["ts"] = coerce_ts(flat[k].strip("\"'"))
                if ev["ts"]:
                    break
    for k in LEVEL_KEYS:
        if k in flat:
            ev["level"] = normalize_level(flat[k].strip("\"'"))
            if ev["level"]:
                break
    if not ev["level"]:
        m = RE_LEVEL.search(line[ts_end:ts_end + 300] if ts_end else line[:300])
        ev["level"] = normalize_level(m.group(1)) if m else None


def fmt_ts(ts):
    if OUT_UTC:
        dt = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
    else:
        dt = datetime.datetime.fromtimestamp(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + "%03d" % (dt.microsecond // 1000)


def parse_record(line, default_ch, src, prev=None, arrival=None, clamp=True, fmt_mode=None):
    """Normalise one log line into an event dict.

    fmt_mode: None/"mixed" (auto), "json" (JSON-first), "plain" (text only).
    """
    now = arrival if arrival is not None else time.time()
    mode = (fmt_mode or WATCH_FORMAT or "mixed").lower()
    ev = {"id": 0, "ts": None, "t": "", "host": None, "level": None, "trace": None,
          "user": None, "uname": None, "run": None, "dept": None, "lane": None, "from": None,
          "svc": None, "ch": default_ch, "msg": line,
          "late": False, "tsrc": "log", "cont": False, "enriched": False}
    payload = None
    s = line.strip()
    if mode != "plain" and s.startswith("{") and s.endswith("}"):
        try:
            payload = json.loads(s)
        except ValueError:
            payload = None
        if not isinstance(payload, dict):
            payload = None
    if payload is not None:
        _fill_from_json(ev, payload)
    elif mode == "json" and s.startswith("{"):
        # Broken JSON line: keep raw message, still try text fields as fallback
        _fill_from_text(ev, line)
    else:
        _fill_from_text(ev, line)

    if ev["ch"] != "plain" and ev["ch"] not in KNOWN_CHANNELS:
        ev["ch"] = "core"

    # user / username written inside the message text: make it searchable by the User filter too
    for um in RE_USER_IN_MSG.finditer(ev["msg"][:1500]):
        uv = _clean(um.group(1))
        if not uv:
            continue
        if not ev["user"]:
            ev["user"] = uv
        elif uv.lower() != ev["user"].lower() and uv.lower() not in (ev.get("uname") or "").lower():
            ev["uname"] = ((ev.get("uname") or "") + " " + uv).strip()[:200]

    # Drop weak sentinels so they do not fake-join unrelated hosts
    if ev["trace"] and not is_strong_trace(ev["trace"]):
        ev["trace"] = None
    if ev["user"] and not is_strong_user(ev["user"]):
        ev["user"] = None
    if ev["run"] and not is_strong_run(ev["run"]):
        ev["run"] = None

    is_cont = False
    if ev["ts"] is None:
        if payload is None and prev is not None:
            # No timestamp on a plain line: stack trace / wrapped line. Stick it to the line before.
            is_cont = True
            ev["cont"] = True
            ev["ts"], ev["tsrc"] = prev["ts"], "inherit"
            for f in ("host", "trace", "user", "uname", "svc", "run", "dept", "lane", "from"):
                ev[f] = ev[f] or prev.get(f)
            ev["level"] = ev["level"] or prev["level"]
            if ev["ch"] == "plain" or ev["ch"] == default_ch:
                ev["ch"] = prev["ch"]
        else:
            ev["ts"], ev["tsrc"] = now, "arrival"
    ev["host"] = ev["host"] or src
    ev["level"] = ev["level"] or "INFO"

    if not is_cont:
        if clamp and ev["ts"] > now + FUTURE_TOLERANCE_SEC:
            ev["ts"], ev["tsrc"] = now, "arrival"
        ev["ts"] += HOST_OFFSETS.get(ev["host"], 0.0)

    ev["msg"] = ev["msg"].replace("\r", " ").replace("\n", " ")[:MAX_MSG_CHARS]
    ev["t"] = fmt_ts(ev["ts"])
    return ev


CSV_COLS = ["time", "host", "level", "trace", "user", "run", "service", "channel", "message"]


def format_line(ev):
    """One line of consolidated.log - always shows join keys for cross-host grep."""
    return "%s | %-12s | %-5s | trace=%s | user=%s | run=%s | %s\n" % (
        ev["t"], ev["host"], ev["level"],
        ev["trace"] or "-", ev["user"] or "-", ev.get("run") or "-", ev["msg"])


def format_event(ev, fmt, csv_writer=None):
    if fmt == "jsonl":
        return json.dumps(ev, ensure_ascii=False, separators=(",", ":")) + "\n"
    if fmt == "csv":
        csv_writer.writerow([ev["t"], ev["host"], ev["level"], ev["trace"] or "", ev["user"] or "",
                             ev.get("run") or "", ev["svc"] or "", ev["ch"], ev["msg"]])
        return ""
    return format_line(ev)


# ==============================================================================
# Hub: ordering, storage, fan-out
# ==============================================================================
class SSEClient:
    __slots__ = ("q", "dead")

    def __init__(self):
        self.q = asyncio.Queue(maxsize=5000)
        self.dead = False

    def push(self, ev):
        try:
            self.q.put_nowait(ev)
        except asyncio.QueueFull:
            self.dead = True  # browser reconnects and catches up from Last-Event-ID


class Conn:
    __slots__ = ("prev", "dropping")

    def __init__(self):
        self.prev = None
        self.dropping = False


class Hub:
    def __init__(self):
        self.pending = []                 # heap of (ts, seq, arrival, event)
        self.seq = 0
        self.release_id = int(time.time() * 1000)
        self.store = deque(maxlen=MAX_MEMORY_HISTORY)
        self.host_ring = {}               # host -> deque of its newest events
        self.host_filled = set()          # hosts whose ring was filled from the file
        self.host_complete = set()        # hosts whose ring holds EVERY event of that host
        self.clients = set()
        self.conn_tasks = set()
        self.sources = {}                 # host -> [last_ts_seen, last_arrival]
        self.stats = {}                   # host -> {"events", "errors", "last"}
        self.queues = {name: asyncio.Queue(maxsize=QUEUE_LIMIT) for name in ALL_FILES}
        self.last_released_ts = 0.0
        self.total_in = 0
        self.total_out = 0
        self.late = 0
        self.dropped = 0
        self.enriched = 0
        self.dup_dropped = 0
        self.seen = {}                    # (host, ms, level, msg) -> source file
        self.written = {}                 # output file -> time of last successful write
        self.started = time.time()
        # Cross-host join: fill missing user/trace/run from peers of the same Flow.
        # trace -> {user, run, hosts}; run -> {trace, user}; user kept incidental
        self.corr_by_trace = {}
        self.corr_by_run = {}
        self.corr_max = 20000

    # ---- correlation (line up user / trace / run across servers) ----------
    def _corr_put(self, index, key, **fields):
        if not key:
            return
        row = index.get(key)
        if row is None:
            if len(index) >= self.corr_max:
                # drop an arbitrary old entry (FIFO-ish via popitem on 3.7+)
                try:
                    index.pop(next(iter(index)))
                except (StopIteration, KeyError):
                    index.clear()
            row = index[key] = {}
        for k, v in fields.items():
            if v and not row.get(k):
                row[k] = v
        index[key] = row

    def enrich(self, ev):
        """Fill missing join keys from other hosts that already logged this Flow."""
        if ev.get("cont"):
            return
        changed = False
        tr, rn, us = ev.get("trace"), ev.get("run"), ev.get("user")
        if is_strong_trace(tr):
            peer = self.corr_by_trace.get(tr) or {}
            if not us and peer.get("user"):
                ev["user"], us, changed = peer["user"], peer["user"], True
            if not rn and peer.get("run"):
                ev["run"], rn, changed = peer["run"], peer["run"], True
        if is_strong_run(rn):
            peer = self.corr_by_run.get(rn) or {}
            if not is_strong_trace(tr) and peer.get("trace"):
                ev["trace"], tr, changed = peer["trace"], peer["trace"], True
            if not us and peer.get("user"):
                ev["user"], us, changed = peer["user"], peer["user"], True
        # Learn for later hops (API -> Kafka -> job on another node)
        if is_strong_trace(tr):
            self._corr_put(self.corr_by_trace, tr, user=us, run=rn)
        if is_strong_run(rn):
            self._corr_put(self.corr_by_run, rn, trace=tr, user=us)
        if changed:
            ev["enriched"] = True
            self.enriched += 1

    # ---- ingest ----------------------------------------------------------
    def ingest(self, raw_bytes, default_ch, src, conn, copy_raw=True, fmt_mode=None, srcfile=None):
        line = raw_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
        if not line.strip():
            return
        now = time.time()
        ev = parse_record(line, default_ch, src, conn.prev, now, fmt_mode=fmt_mode)
        conn.prev = ev
        if DEDUP_ACROSS_FILES and srcfile:
            if ev["cont"]:
                if conn.dropping:        # stack trace of a line we already dropped
                    self.dup_dropped += 1
                    return
            else:
                conn.dropping = False
                key = (ev["host"], int(ev["ts"] * 1000), ev["level"], ev["msg"][:200])
                owner = self.seen.get(key)
                if owner is None:
                    self.seen[key] = srcfile
                    if len(self.seen) > DEDUP_CACHE:
                        try:
                            self.seen.pop(next(iter(self.seen)))
                        except (StopIteration, KeyError):
                            self.seen.clear()
                elif owner != srcfile:   # same event already read from another file
                    conn.dropping = True
                    self.dup_dropped += 1
                    return
        self.total_in += 1

        # File watch must not rewrite into the same on-disk sources (loop risk).
        if copy_raw:
            raw_file = "all-plain.log" if ev["ch"] == "plain" else ev["ch"] + ".log"
            self._enqueue(raw_file, line + "\n")

        self.seq += 1
        heapq.heappush(self.pending, (ev["ts"], self.seq, now, ev))

        h = ev["host"]
        old = self.sources.get(h)
        self.sources[h] = [max(ev["ts"], old[0]) if old else ev["ts"], now]
        st = self.stats.setdefault(h, {"events": 0, "errors": 0, "last": now})
        st["last"] = now
        if not ev["cont"]:
            st["events"] += 1
            if LEVEL_RANK.get(ev["level"], 0) >= 40:
                st["errors"] += 1

    def _enqueue(self, name, text):
        try:
            self.queues[name].put_nowait(text)
        except asyncio.QueueFull:
            self.dropped += 1

    # ---- ordered release -------------------------------------------------
    def release(self, force=False):
        now = time.time()
        heap = self.pending
        if not heap:
            return
        active = [v[0] for v in self.sources.values() if now - v[1] <= REORDER_IDLE_SEC]
        watermark = min(active) if active else float("inf")
        hold_cut = now - REORDER_MAX_HOLD_SEC
        while heap:
            ts, _, arrived, ev = heap[0]
            if not force and ts > watermark and arrived > hold_cut:
                break
            heapq.heappop(heap)
            self._emit(ev)

    def _emit(self, ev):
        self.enrich(ev)
        self.release_id += 1
        ev["id"] = self.release_id
        if ev["ts"] < self.last_released_ts - 0.001:
            ev["late"] = True
            self.late += 1
        else:
            self.last_released_ts = ev["ts"]
        self.remember(ev)
        self.total_out += 1
        self._enqueue(CONSOLIDATED_LOG, format_line(ev))
        self._enqueue(CONSOLIDATED_JSONL, format_event(ev, "jsonl"))
        for c in list(self.clients):
            c.push(ev)

    async def release_loop(self):
        while True:
            await asyncio.sleep(0.2)
            try:
                self.release()
            except Exception as e:  # never let the sequencer die
                print("[ERROR] release loop: %s" % e, file=sys.stderr)

    def remember(self, ev):
        self.store.append(ev)
        ring = self.host_ring.get(ev["host"])
        if ring is None:
            if len(self.host_ring) >= 200:      # never let junk hosts grow memory
                return
            ring = self.host_ring[ev["host"]] = deque(maxlen=MEMORY_PER_HOST)
        ring.append(ev)

    def merge_ring(self, host, older, complete=False):
        """Put events read from the file (oldest first) in front of what the ring already holds."""
        ring = self.host_ring.get(host)
        if ring is None and len(self.host_ring) >= 200:
            return
        cur = list(ring) if ring else []
        first = cur[0]["id"] if cur else float("inf")
        merged = [e for e in older if e.get("id", 0) < first] + cur
        self.host_ring[host] = deque(merged, maxlen=MEMORY_PER_HOST)
        if complete and len(merged) < MEMORY_PER_HOST:
            self.host_complete.add(host)

    # ---- warm start ------------------------------------------------------
    def load_history(self):
        path = os.path.join(LOG_OUTPUT_DIR, CONSOLIDATED_JSONL)
        if not os.path.isfile(path):
            return
        try:
            size = os.path.getsize(path)
            with open(path, "rb") as f:
                back = max(64, HOST_RELOAD_MB) * 1024 * 1024
                if size > back:
                    f.seek(size - back)
                    f.readline()
                for bline in f:
                    try:
                        ev = json.loads(bline)
                        if isinstance(ev, dict) and "ts" in ev and "host" in ev:
                            self.remember(ev)
                            self.release_id = max(self.release_id, int(ev.get("id", 0)))
                    except (ValueError, TypeError):
                        continue
            print("[INFO] Reloaded %d recent events from %s" % (len(self.store), CONSOLIDATED_JSONL))
        except OSError as e:
            print("[WARN] Could not reload history: %s" % e, file=sys.stderr)


hub = None  # created inside main() so asyncio objects bind to the running loop


# ==============================================================================
# File writers, rotation
# ==============================================================================
def _append_lines(path, lines):
    with open(path, "a", encoding="utf-8", errors="replace", newline="") as f:
        f.writelines(lines)


async def file_writer_task(name, queue):
    path = os.path.join(LOG_OUTPUT_DIR, name)
    loop = asyncio.get_running_loop()
    try:
        while True:
            batch = [await queue.get()]
            await asyncio.sleep(BATCH_FLUSH_INTERVAL)
            while len(batch) < MAX_BATCH_LINES:
                try:
                    batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            for attempt in range(1, 6):   # keep the batch through a short disk/mount error
                try:
                    await loop.run_in_executor(None, _append_lines, path, batch)
                    if hub is not None:
                        hub.written[name] = time.time()
                    break
                except Exception as e:   # not only OSError: the writer must never die
                    print("[ERROR] Writer failure on %s (try %d/5): %s" % (name, attempt, e),
                          file=sys.stderr)
                    await asyncio.sleep(min(2 * attempt, 10))
            else:
                print("[ERROR] Dropped %d lines for %s after 5 failed writes" % (len(batch), name),
                      file=sys.stderr)
                if hub is not None:
                    hub.dropped += len(batch)
            for _ in batch:
                queue.task_done()
    except asyncio.CancelledError:
        pass


def _rotate_one(name, label, archived_dir):
    src = os.path.join(LOG_OUTPUT_DIR, name)
    if not os.path.isfile(src) or os.path.getsize(src) == 0:
        return
    tmp = src + ".rotating"
    os.replace(src, tmp)  # atomic: the writer simply starts a fresh file
    dest = os.path.join(archived_dir, "%s.%s.gz" % (name, label))
    with open(tmp, "rb") as f_in, gzip.open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(tmp)
    print("[INFO] Rotated and compressed: %s" % dest)


def _prune_archive(archived_dir):
    cutoff = time.time() - RETENTION_DAYS * 86400
    for fn in os.listdir(archived_dir):
        p = os.path.join(archived_dir, fn)
        try:
            if fn.endswith(".gz") and os.path.getmtime(p) < cutoff:
                os.remove(p)
        except OSError:
            pass


async def daily_rotation_task():
    loop = asyncio.get_running_loop()
    while True:
        now = datetime.datetime.now()
        midnight = datetime.datetime.combine(now.date() + datetime.timedelta(days=1), datetime.time.min)
        await asyncio.sleep((midnight - now).total_seconds() + 2)
        label = (midnight.date() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        archived_dir = os.path.join(LOG_OUTPUT_DIR, "archived")
        for name in ALL_FILES:
            try:
                await loop.run_in_executor(None, _rotate_one, name, label, archived_dir)
            except Exception as e:
                print("[ERROR] Rotation failed for %s: %s" % (name, e), file=sys.stderr)
        try:
            await loop.run_in_executor(None, _prune_archive, archived_dir)
        except Exception:
            pass


# ==============================================================================
# TCP ingestion
# ==============================================================================
async def _close(writer):
    try:
        writer.close()
        await writer.wait_closed()
    except (Exception, asyncio.CancelledError):
        pass


async def handle_stream(reader, writer, default_ch):
    hub.conn_tasks.add(asyncio.current_task())
    peer = writer.get_extra_info("peername")
    src = peer[0] if peer else "unknown"
    conn = Conn()
    buf = b""
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            buf += data
            *lines, buf = buf.split(b"\n")  # split bytes first: never cut a UTF-8 character
            for bl in lines:
                hub.ingest(bl, default_ch, src, conn)
            if len(buf) > MAX_LINE_BYTES:
                hub.ingest(buf, default_ch, src, conn)
                buf = b""
        if buf.strip():
            hub.ingest(buf, default_ch, src, conn)  # last line without trailing newline
    except (ConnectionError, asyncio.IncompleteReadError, OSError):
        pass
    finally:
        hub.conn_tasks.discard(asyncio.current_task())
        await _close(writer)


async def handle_plain_connection(reader, writer):
    await handle_stream(reader, writer, "plain")


async def handle_json_connection(reader, writer):
    await handle_stream(reader, writer, "core")


# ==============================================================================
# Queries over the in-memory store
# ==============================================================================
def _num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def parse_filters(q):
    f = {
        "hosts": set(x for x in q.get("host", "").split(",") if x and x != "ALL"),
        "minrank": LEVEL_RANK.get(q.get("level", "").upper(), 0),
        "user": q.get("user", "").strip().lower(),
        "trace": q.get("trace", "").strip().lower(),
        "run": q.get("run", "").strip().lower(),
        "q": q.get("q", "").strip().lower(),
        "from": 0.0,
        "to": 0.0,
    }
    since = _num(q.get("since"))
    if since > 0:  # relative to the newest event, so it works with skewed clocks
        ref = hub.store[-1]["ts"] if hub.store else time.time()
        f["from"] = ref - since
    else:
        f["from"] = _num(q.get("from"))
    f["to"] = _num(q.get("to"))
    return f


def ev_matches(ev, f, level=True, user=True, trace=True, run=True, text=True):
    if f["hosts"] and ev["host"] not in f["hosts"]:
        return False
    if f["from"] and ev["ts"] < f["from"]:
        return False
    if f["to"] and ev["ts"] > f["to"]:
        return False
    if level and f["minrank"] and LEVEL_RANK.get(ev["level"], 0) < f["minrank"]:
        return False
    if user and f["user"] and f["user"] not in ((ev["user"] or "") + " " + (ev.get("uname") or "")).lower():
        if not any(f["user"] in (m.group(1) or "").lower() for m in RE_USER_IN_MSG.finditer(ev["msg"][:3000])):
            return False
    if trace and f["trace"] and f["trace"] not in (ev["trace"] or "").lower():
        return False
    if run and f.get("run") and f["run"] not in (ev.get("run") or "").lower():
        return False
    if text and f["q"]:
        hay = "%s %s %s %s %s %s" % (
            ev["msg"], ev["host"], ev["trace"] or "", ev["user"] or "", ev.get("uname") or "", ev.get("run") or "")
        if f["q"] not in hay.lower():
            return False
    return True


def summarize(kind, f, limit):
    """Group events by trace, user, or run. Weak/sentinel traces are skipped."""
    if kind == "trace":
        key, others = "trace", ("user", "run")
    elif kind == "run":
        key, others = "run", ("trace", "user")
    else:
        key, others = "user", ("trace", "run")
    groups = {}
    for ev in hub.store:
        k = ev.get(key)
        if not k:
            continue
        if key == "trace" and not is_strong_trace(k):
            continue
        if key == "run" and not is_strong_run(k):
            continue
        if key == "user" and not is_strong_user(k):
            continue
        if not ev_matches(ev, f, level=False, user=False, trace=False, run=False, text=False):
            continue
        g = groups.get(k)
        if g is None:
            g = groups[k] = {"id": k, "first": ev["ts"], "last": ev["ts"], "events": 0, "errors": 0,
                             "maxrank": 0, "hosts": {}, "related": {}, "qhit": False, "ohit": False}
        g["first"] = min(g["first"], ev["ts"])
        g["last"] = max(g["last"], ev["ts"])
        g["hosts"].setdefault(ev["host"], ev["ts"])
        rank = LEVEL_RANK.get(ev["level"], 0)
        g["maxrank"] = max(g["maxrank"], rank)
        if not ev["cont"]:
            g["events"] += 1
            if rank >= 40:
                g["errors"] += 1
        for o in others:
            ov = ev.get(o)
            if ov:
                g["related"][ov] = True
        if f["q"] and not g["qhit"] and f["q"] in ("%s %s" % (ev["msg"], ev["host"])).lower():
            g["qhit"] = True
        # related filter: match any other join key in the query
        for o in others:
            if f.get(o) and f[o] in (ev.get(o) or "").lower():
                g["ohit"] = True
        if not any(f.get(o) for o in others):
            g["ohit"] = True
    rows = []
    for g in groups.values():
        if f["minrank"] and g["maxrank"] < f["minrank"]:
            continue
        if f.get(key) and f[key] not in g["id"].lower():
            continue
        if f["q"] and not g["qhit"]:
            continue
        if any(f.get(o) for o in others) and not g["ohit"]:
            continue
        # Prefer hosts in first-seen time order so path = real hop sequence
        rows.append({
            "id": g["id"], "t_first": fmt_ts(g["first"]), "t_last": fmt_ts(g["last"]),
            "last": g["last"], "duration_ms": int((g["last"] - g["first"]) * 1000),
            "events": g["events"], "errors": g["errors"],
            "hosts": sorted(g["hosts"], key=g["hosts"].get),
            "related": len(g["related"]), "related_sample": list(g["related"])[:5],
        })
    rows.sort(key=lambda r: r["last"], reverse=True)
    return rows[:limit]


def build_flow(q):
    trace = q.get("trace", "").strip()
    user = q.get("user", "").strip()
    run = q.get("run", "").strip()
    if trace:
        kind, ident = "trace", trace
        evs = [e for e in hub.store if e.get("trace") == trace]
        # Also pull peers that share the same runKey but lost trace on one hop
        runs = {e.get("run") for e in evs if is_strong_run(e.get("run"))}
        if runs:
            extra = [e for e in hub.store
                     if e.get("run") in runs and e not in evs]
            evs.extend(extra)
    elif run:
        kind, ident = "run", run
        evs = [e for e in hub.store if e.get("run") == run]
    elif user:
        kind, ident = "user", user
        ul = user.lower()
        evs = [e for e in hub.store
               if (e.get("user") or "").lower() == ul or ul in (e.get("uname") or "").lower().split()]
    else:
        return None
    # Stable cross-host order: time, then host, then id
    evs.sort(key=lambda e: (e["ts"], e.get("host") or "", e.get("id") or 0))
    hosts, hops = [], []
    errors = 0
    for e in evs:
        if e["host"] not in hosts:
            hosts.append(e["host"])
        is_err = LEVEL_RANK.get(e["level"], 0) >= 40 and not e["cont"]
        errors += 1 if is_err else 0
        if hops and hops[-1]["host"] == e["host"]:
            h = hops[-1]
            h["end"], h["n"] = e["ts"], h["n"] + 1
            h["errors"] += 1 if is_err else 0
        else:
            gap = int((e["ts"] - hops[-1]["end"]) * 1000) if hops else 0
            hops.append({"host": e["host"], "start": e["ts"], "end": e["ts"], "n": 1,
                         "errors": 1 if is_err else 0, "gap_ms": gap})
    related = []
    for e in evs:
        for o in ("user", "trace", "run"):
            if o == kind:
                continue
            v = e.get(o)
            if v and v not in related:
                related.append(v)
    start = evs[0]["ts"] if evs else 0
    end = evs[-1]["ts"] if evs else 0
    return {
        "kind": kind, "id": ident, "events": evs[-3000:],
        "summary": {"count": len(evs), "errors": errors, "hosts": hosts, "related": related[:50],
                    "start": start, "end": end, "duration_ms": int((end - start) * 1000)},
        "hops": hops,
    }


HIDE_JUNK_HOSTS = _env_int("HIDE_JUNK_HOSTS", 1)
HIDE_IP_MAX_EVENTS = _env_int("HIDE_IP_MAX_EVENTS", 100)
_RE_IPV4 = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
_RE_GOOD_HOST = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _junk_host(name, events):
    """Port scanners and garbled lines create odd 'hosts': bare IPs with a few events,
    or names with illegal characters. Hide them from the host chips and list."""
    if not HIDE_JUNK_HOSTS:
        return False
    if not _RE_GOOD_HOST.match(name or ""):
        return True
    return bool(_RE_IPV4.match(name)) and events <= HIDE_IP_MAX_EVENTS


def api_stats():
    now = time.time()
    files = []
    for name in [CONSOLIDATED_LOG, CONSOLIDATED_JSONL]:
        try:
            files.append({"name": name, "size": os.path.getsize(os.path.join(LOG_OUTPUT_DIR, name))})
        except OSError:
            files.append({"name": name, "size": 0})
    return {
        "uptime": int(now - hub.started), "received": hub.total_in, "consolidated": hub.total_out,
        "late": hub.late, "dropped": hub.dropped, "enriched": hub.enriched,
        "duplicates_dropped": hub.dup_dropped,
        "last_write_age_sec": {n: round(now - t, 1) for n, t in hub.written.items()},
        "pending": len(hub.pending),
        "stored": len(hub.store), "viewers": len(hub.clients), "files": files,
        "output_directory": os.path.abspath(LOG_OUTPUT_DIR),
        "timezone": "UTC" if OUT_UTC else "server local time",
        "servers": {h: {"events": s["events"], "errors": s["errors"], "age": round(now - s["last"], 1)}
                    for h, s in hub.stats.items() if not _junk_host(h, s["events"])},
    }


SEARCH_MAX_SEC = _env_float("SEARCH_MAX_SEC", 10.0)
SEARCH_CACHE_SEC = _env_float("SEARCH_CACHE_SEC", 20.0)
SEARCH_THREADS = _env_int("SEARCH_THREADS", 3)
_search_pool = ThreadPoolExecutor(max_workers=max(1, SEARCH_THREADS), thread_name_prefix="search")
_search_cache = {}            # query key -> (time, result)
_search_cache_lock = threading.Lock()


def _cache_get(key):
    with _search_cache_lock:
        hit = _search_cache.get(key)
        if hit and time.time() - hit[0] < SEARCH_CACHE_SEC:
            return hit[1]
        _search_cache.pop(key, None)
    return None


def _cache_put(key, result):
    with _search_cache_lock:
        if len(_search_cache) >= 48:
            oldest = min(_search_cache, key=lambda k: _search_cache[k][0])
            _search_cache.pop(oldest, None)
        _search_cache[key] = (time.time(), result)


def search_memory(f, limit, offset, snap, all_history=False):
    """Answer from the newest events kept in memory. Returns None when memory cannot
    prove the page is complete (then the caller reads the file on disk)."""
    need = offset + limit + 1
    found = []
    scanned = 0
    for ev in reversed(snap):
        scanned += 1
        if ev_matches(ev, f):
            found.append(ev)
            if len(found) >= need:
                break
    complete = len(found) >= need
    if not complete and all_history:
        complete = True        # memory holds every event of this host: no match means no match
    if not complete and snap and f["from"] and snap[0]["ts"] < f["from"]:
        complete = True        # the whole time window lies inside memory
    if not complete:
        return None
    page = found[offset:offset + limit]
    page.reverse()
    return {"events": page, "matched": len(page), "scanned": scanned,
            "has_more": len(found) > offset + limit, "partial": False, "source": "memory"}


def _rev_lines(fh, size, block=4 * 1024 * 1024):
    """Yield the lines of an open binary file from the last one to the first."""
    pos, tail = size, b""
    while pos > 0:
        n = min(block, pos)
        pos -= n
        fh.seek(pos)
        parts = (fh.read(n) + tail).split(b"\n")
        tail = parts[0]
        for ln in reversed(parts[1:]):
            yield ln
    if tail:
        yield tail


RE_TS_FIELD = re.compile(rb'"ts":([0-9.]+)')


def _rev_candidates(fh, size, needle, stat, deadline, cancel, tmin, block=4 * 1024 * 1024):
    """Yield, newest first, only the lines that contain `needle` (lower-case bytes).

    Each block is searched with bytes.rfind, which runs at C speed, so lines without the
    text are never split or parsed. This is what makes rare host/user/text filters fast.
    """
    pos, tail = size, b""
    while pos > 0:
        if time.time() > deadline or (cancel is not None and cancel.is_set()):
            stat["partial"] = True
            return
        n = min(block, pos)
        pos -= n
        fh.seek(pos)
        data = fh.read(n) + tail
        if pos > 0:
            nl = data.find(b"\n")
            if nl < 0:                      # one very long line: keep reading backwards
                tail = data
                continue
            tail, reg = data[:nl], data[nl + 1:]
        else:
            tail, reg = b"", data
        stat["scanned"] += reg.count(b"\n") + 1
        low = reg.lower()
        end = len(reg)
        while end > 0:
            i = low.rfind(needle, 0, end)
            if i < 0:
                break
            ls = reg.rfind(b"\n", 0, i) + 1
            le = reg.find(b"\n", i)
            if le < 0:
                le = len(reg)
            yield reg[ls:le]
            end = ls
        if tmin and pos > 0:                 # whole block older than the time window: stop
            first = reg[:reg.find(b"\n")] if b"\n" in reg else reg
            m = RE_TS_FIELD.search(first)
            if m and float(m.group(1)) < tmin:
                stat["done"] = True
                return


def search_disk(f, limit, offset=0, cancel=None):
    """Search the whole consolidated.jsonl (all history on disk) with the dashboard filters.

    Reads from the newest line backwards and stops once `limit` matches are found, so
    common filters answer fast. Runs in a worker thread: the web UI and the log
    ingestion keep working while it scans. Returns matches in time order.
    """
    path = os.path.join(LOG_OUTPUT_DIR, CONSOLIDATED_JSONL)
    needles = []
    for key in ("q", "user", "trace", "run"):
        v = f.get(key)
        # cheap raw-text pre-check; skipped when the text could be JSON-escaped on disk
        if v and '"' not in v and "\\" not in v:
            needles.append(v.encode("utf-8", "replace"))
    host_needle = None
    if len(f["hosts"]) == 1:
        h = next(iter(f["hosts"]))
        if '"' not in h and "\\" not in h:
            host_needle = ('"host":"%s"' % h).encode("utf-8", "replace")
    out = []
    scanned = 0
    seen_matches = 0
    partial = False
    has_more = False
    deadline = time.time() + SEARCH_MAX_SEC
    # fast path: search for the most selective text with bytes.rfind (needs 3+ characters)
    cands = [n.lower() for n in needles] + ([host_needle.lower()] if host_needle else [])
    cands = [c for c in cands if len(c) >= 3]
    fast = max(cands, key=len) if cands else None
    stat = {"scanned": 0, "partial": False, "done": False}
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if fast is not None:
                lines = _rev_candidates(fh, size, fast, stat, deadline, cancel,
                                        (f["from"] - 3600) if f["from"] else 0)
            else:
                lines = _rev_lines(fh, size)
            for raw in lines:
                if fast is None:
                    scanned += 1
                    if scanned % 20000 == 0 and (time.time() > deadline or (cancel is not None and cancel.is_set())):
                        partial = True
                        break
                if host_needle is not None and host_needle not in raw:
                    continue
                if needles:
                    low = raw.lower()
                    if any(n not in low for n in needles):
                        continue
                try:
                    ev = json.loads(raw)
                except ValueError:
                    continue
                if not (isinstance(ev, dict) and "ts" in ev and "host" in ev):
                    continue
                if f["from"] and ev["ts"] < f["from"] - 3600:
                    break                      # older than the time window (with a safety margin)
                if ev_matches(ev, f):
                    seen_matches += 1
                    if seen_matches <= offset:
                        continue               # belongs to an earlier (newer) page
                    out.append(ev)
                    if len(out) > limit:       # one extra line proves there is a next page
                        out.pop()
                        has_more = True
                        break
    except OSError:
        pass
    if fast is not None:
        scanned = stat["scanned"]
        partial = partial or stat["partial"]
    out.reverse()
    return {"events": out, "matched": len(out), "scanned": scanned, "has_more": has_more,
            "partial": partial, "source": "disk"}


def fill_host_ring(host):
    """Read the newest MEMORY_PER_HOST events of one host from the file (oldest first)."""
    if '"' in host or "\\" in host:
        return [], False
    path = os.path.join(LOG_OUTPUT_DIR, CONSOLIDATED_JSONL)
    needle = ('"host":"%s"' % host).lower().encode("utf-8", "replace")
    out = []
    stat = {"scanned": 0, "partial": False, "done": False}
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            for raw in _rev_candidates(fh, size, needle, stat, time.time() + 30, None, 0):
                try:
                    ev = json.loads(raw)
                except ValueError:
                    continue
                if isinstance(ev, dict) and ev.get("host") == host and "ts" in ev:
                    out.append(ev)
                    if len(out) >= MEMORY_PER_HOST:
                        break
    except OSError:
        stat["partial"] = True
    out.reverse()
    return out, (len(out) < MEMORY_PER_HOST and not stat["partial"])


def run_search(f, limit, offset, snap, cancel, key, ring=None, ring_all=False):
    """Worker-thread entry: cache, then memory, then disk."""
    hit = _cache_get(key)
    if hit is not None:
        out = dict(hit)
        out["cached"] = True
        return out
    res = search_memory(f, limit, offset, ring, ring_all) if ring else None
    if res is None:
        res = search_memory(f, limit, offset, snap)
    if res is None:
        res = search_disk(f, limit, offset, cancel)
    if not res.get("partial"):
        _cache_put(key, res)
    return res


# ==============================================================================
# HTTP server
# ==============================================================================
STATUS_TEXT = {200: "OK", 204: "No Content", 400: "Bad Request", 404: "Not Found",
               500: "Internal Server Error"}


def _json(obj, status=200):
    return status, "application/json; charset=utf-8", json.dumps(
        obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), {}


def route(path, q):
    if path == "/api/events":
        f = parse_filters(q)
        limit = max(1, min(int(_num(q.get("limit"), 2000)), 20000))
        matched = [e for e in hub.store if ev_matches(e, f)]
        return _json({"events": matched[-limit:], "matched": len(matched),
                      "max_id": hub.store[-1]["id"] if hub.store else 0})
    if path == "/api/traces":
        return _json({"rows": summarize("trace", parse_filters(q), int(_num(q.get("limit"), 300)))})
    if path == "/api/users":
        return _json({"rows": summarize("user", parse_filters(q), int(_num(q.get("limit"), 300)))})
    if path == "/api/runs":
        return _json({"rows": summarize("run", parse_filters(q), int(_num(q.get("limit"), 300)))})
    if path == "/api/flow":
        flow = build_flow(q)
        return _json(flow) if flow else _json({"error": "pass trace=, user=, or run="}, 400)
    if path == "/api/stats":
        return _json(api_stats())
    if path == "/api/export":
        fmt = q.get("format", "log")
        if fmt not in ("log", "jsonl", "csv"):
            fmt = "log"
        f = parse_filters(q)
        buf = io.StringIO()
        writer = None
        if fmt == "csv":
            writer = csv.writer(buf)
            writer.writerow(CSV_COLS)
        for e in hub.store:
            if ev_matches(e, f):
                buf.write(format_event(e, fmt, writer))
        name = "aiv-consolidated-%s.%s" % (time.strftime("%Y%m%d-%H%M%S"), fmt)
        ctype = {"log": "text/plain", "jsonl": "application/x-ndjson", "csv": "text/csv"}[fmt]
        return (200, ctype + "; charset=utf-8", buf.getvalue().encode("utf-8"),
                {"Content-Disposition": 'attachment; filename="%s"' % name})
    if path == "/favicon.ico":
        return 204, "image/x-icon", b"", {}
    if path in ("/", "/index.html"):
        return 200, "text/html; charset=utf-8", DASHBOARD_HTML.encode("utf-8"), {}
    return _json({"error": "not found"}, 404)


async def http_send(writer, status, ctype, body, extra=None):
    head = ["HTTP/1.1 %d %s" % (status, STATUS_TEXT.get(status, "OK")),
            "Content-Type: " + ctype, "Content-Length: %d" % len(body),
            "Cache-Control: no-store", "Connection: close"]
    for k, v in (extra or {}).items():
        head.append("%s: %s" % (k, v))
    writer.write(("\r\n".join(head) + "\r\n\r\n").encode("latin-1") + body)
    await writer.drain()


async def read_request(reader):
    line = await asyncio.wait_for(reader.readline(), 10)
    if not line:
        return None
    parts = line.decode("latin-1").split()
    if len(parts) < 2:
        return None
    headers = {}
    for _ in range(100):
        h = await asyncio.wait_for(reader.readline(), 10)
        if h in (b"\r\n", b"\n", b""):
            break
        if b":" in h:
            k, v = h.decode("latin-1").split(":", 1)
            headers[k.strip().lower()] = v.strip()
    url = urlparse(parts[1])
    q = {k: v[0] for k, v in parse_qs(url.query, keep_blank_values=True).items()}
    return parts[0], url.path, q, headers


def _sse(ev):
    return ("id: %d\ndata: %s\n\n" % (
        ev["id"], json.dumps(ev, ensure_ascii=False, separators=(",", ":")))).encode("utf-8")


async def serve_sse(writer, q, headers):
    try:
        after = int(headers.get("last-event-id") or q.get("after") or 0)
    except ValueError:
        after = 0
    client = SSEClient()
    # No await between the snapshot and registering: no gap, no duplicates.
    backlog = [e for e in hub.store if e["id"] > after][-5000:]
    hub.clients.add(client)
    try:
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                     b"Cache-Control: no-cache\r\nConnection: keep-alive\r\n"
                     b"X-Accel-Buffering: no\r\n\r\nretry: 2000\n\n")
        for ev in backlog:
            writer.write(_sse(ev))
        await writer.drain()
        while True:
            try:
                ev = await asyncio.wait_for(client.q.get(), 15)
            except asyncio.TimeoutError:
                writer.write(b": ping\n\n")  # keep-alive; also detects dead browsers
                await writer.drain()
                continue
            writer.write(_sse(ev))
            if client.q.empty():
                await writer.drain()
                if client.dead:
                    break
    finally:
        hub.clients.discard(client)


async def handle_http_request(reader, writer):
    hub.conn_tasks.add(asyncio.current_task())
    try:
        req = await read_request(reader)
        if req is None:
            return
        _method, path, q, headers = req
        if path == "/stream":
            await serve_sse(writer, q, headers)
            return
        if path == "/api/search":
            try:
                flt = parse_filters(q)
                lim = max(1, min(int(_num(q.get("limit"), 200)), 5000))
                off = max(0, int(_num(q.get("offset"), 0)))
                cancel = threading.Event()
                # the browser closes the socket when it cancels a request: stop scanning then
                watcher = asyncio.ensure_future(reader.read(1))
                watcher.add_done_callback(lambda _t: cancel.set())
                key = json.dumps(sorted(q.items()), ensure_ascii=False) if isinstance(q, dict) else str(q)
                snap = tuple(hub.store)
                ring, ring_all = None, False
                if len(flt["hosts"]) == 1:
                    h0 = next(iter(flt["hosts"]))
                    r0 = hub.host_ring.get(h0)
                    ring = tuple(r0) if r0 else None
                    ring_all = bool(r0) and h0 in hub.host_complete and len(r0) < MEMORY_PER_HOST
                try:
                    result = await asyncio.get_running_loop().run_in_executor(
                        _search_pool, run_search, flt, lim, off, snap, cancel, key, ring, ring_all)
                finally:
                    watcher.cancel()
                status, ctype, body, extra = _json(result)
                if result.get("source") == "disk" and len(flt["hosts"]) == 1:
                    h = next(iter(flt["hosts"]))
                    if h not in hub.host_filled:          # next filters on this host come from memory
                        hub.host_filled.add(h)
                        fut = asyncio.get_running_loop().run_in_executor(_search_pool, fill_host_ring, h)

                        def _filled(ft, h=h):
                            try:
                                evs, whole = ft.result()
                                hub.merge_ring(h, evs, whole)
                            except Exception:
                                hub.host_filled.discard(h)
                        fut.add_done_callback(_filled)
            except Exception as e:
                status, ctype, body, extra = _json({"error": str(e)}, 500)
            await http_send(writer, status, ctype, body, extra)
            return
        try:
            status, ctype, body, extra = route(path, q)
        except Exception as e:
            status, ctype, body, extra = _json({"error": str(e)}, 500)
        await http_send(writer, status, ctype, body, extra)
    except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError, OSError):
        pass
    except asyncio.CancelledError:
        pass
    finally:
        hub.conn_tasks.discard(asyncio.current_task())
        await _close(writer)


# ==============================================================================
# Dashboard
# ==============================================================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
<meta charset="UTF-8">
<title>AIV Cluster Log Console</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<script>
  (function () {
    var t = 'light';
    try { t = localStorage.getItem('aivTheme') || 'light'; } catch (e) {}
    document.documentElement.setAttribute('data-theme', t === 'dark' ? 'dark' : 'light');
  })();
</script>
<style>
  :root {
    color-scheme: dark;
    --log-bg: #0a0f14; --hover: rgba(255,255,255,.08); --hair: rgba(255,255,255,.04);
    --err-bg: rgba(255,122,122,.06); --hit: rgba(94,234,212,.07);
    --c-trace: #c4a8ff; --c-user: #7dd3fc; --c-run: #86efac; --c-name: #fcd34d; --mark: #f5c542;
    --bg: #0e1319; --panel: #161d26; --panel2: #1c2530; --line: #28323f;
    --text: #dbe3ec; --muted: #8392a4; --accent: #5eead4; --accent-ink: #062924;
    --err: #ff7a7a; --warn: #f4c15a; --dbg: #6b7a8c;
    --ui: "Segoe UI Variable Text", "Segoe UI", system-ui, -apple-system, "Helvetica Neue", sans-serif;
    --mono: ui-monospace, "Cascadia Mono", "SF Mono", Menlo, Consolas, "DejaVu Sans Mono", monospace;
  }
  :root[data-theme="light"] {
    color-scheme: light;
    --bg: #f3f5f8; --panel: #ffffff; --panel2: #edf0f4; --line: #d3dae3;
    --text: #1b2733; --muted: #5a6a7b; --accent: #0f766e; --accent-ink: #ffffff;
    --err: #c62828; --warn: #9a6700; --dbg: #7b8794;
    --log-bg: #ffffff; --hover: rgba(15,30,50,.07); --hair: rgba(15,30,50,.06);
    --err-bg: rgba(198,40,40,.07); --hit: rgba(15,118,110,.10);
    --c-trace: #6d28d9; --c-user: #0369a1; --c-run: #15803d; --c-name: #a16207; --mark: #ffe066;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; }
  body { font-family: var(--ui); background: var(--bg); color: var(--text); display: flex; flex-direction: column; overflow: hidden; font-size: 13px; }
  button, input, select { font: inherit; color: inherit; background: var(--panel2); border: 1px solid var(--line); border-radius: 5px; padding: 5px 10px; outline: none; }
  input:focus, select:focus, button:focus-visible { border-color: var(--accent); }
  button { cursor: pointer; }
  button:hover { border-color: var(--muted); }
  button.primary { background: var(--accent); color: var(--accent-ink); border-color: var(--accent); font-weight: 600; }
  header { background: var(--panel); border-bottom: 1px solid var(--line); padding: 10px 16px; display: flex; flex-wrap: wrap; gap: 10px 18px; align-items: center; }
  .brand { font-weight: 650; font-size: 15px; display: flex; align-items: center; gap: 10px; }
  .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--dbg); display: inline-block; }
  .dot.ok { background: #34d399; } .dot.warn { background: var(--warn); } .dot.bad { background: var(--err); }
  .tabs { display: flex; gap: 2px; background: var(--bg); border: 1px solid var(--line); border-radius: 7px; padding: 2px; }
  .tabs button { border: none; background: transparent; color: var(--muted); padding: 5px 14px; }
  .tabs button.active { background: var(--panel2); color: var(--text); font-weight: 600; }
  .controls { display: flex; flex-wrap: nowrap; gap: 6px; align-items: center; flex: 1 1 100%; min-width: 0; }
  .controls > * { flex: 0 0 auto; }
  .controls button, .controls select { padding: 5px 8px; }
  #f-q { flex: 1 1 150px; width: 0; min-width: 90px; } #f-user, #f-trace, #f-run { flex: 1 1 90px; width: 0; min-width: 64px; }
  @media (min-width: 1800px) { .controls { flex: 0 1 auto; margin-left: auto; } #f-q { width: 200px; } #f-user, #f-trace, #f-run { width: 120px; } }
  @media (max-width: 1100px) { .controls { flex-wrap: wrap; } #f-q { flex-basis: 200px; } #f-user, #f-trace, #f-run { flex-basis: 110px; } }
  #nodes { display: flex; gap: 8px; padding: 7px 16px; border-bottom: 1px solid var(--line); background: var(--bg); overflow-x: auto; min-height: 36px; align-items: center; }
  .node { display: flex; gap: 7px; align-items: center; padding: 3px 10px; border: 1px solid var(--line); border-radius: 14px; cursor: pointer; white-space: nowrap; background: var(--panel); }
  .node.sel { border-color: var(--accent); }
  .node .n { color: var(--muted); font-variant-numeric: tabular-nums; }
  .node .e { color: var(--err); font-variant-numeric: tabular-nums; }
  .hint { color: var(--muted); }
  main { flex: 1; position: relative; min-height: 0; }
  .view { position: absolute; inset: 0; overflow: auto; }
  .hidden { display: none !important; }
  #log-container { padding: 8px 14px; font-family: var(--mono); font-size: 12.5px; line-height: 1.55; background: var(--log-bg); }
  .log-row { white-space: pre-wrap; word-break: break-word; padding: 1px 0 1px 8px; border-left: 2px solid transparent;
    content-visibility: auto; contain-intrinsic-size: auto 22px; }
  .log-row .more { color: var(--c-trace); cursor: pointer; text-decoration: underline; }
  .log-row .ts { color: var(--muted); }
  .log-row .host, .hopchip { color: #fff; padding: 0 7px; border-radius: 3px; font-size: 11.5px; cursor: pointer; }
  .log-row .lvl { display: inline-block; min-width: 5ch; }
  .lvl-DEBUG, .lvl-TRACE { color: var(--dbg); }
  .lvl-WARN { color: var(--warn); border-left-color: var(--warn); }
  .lvl-ERROR, .lvl-FATAL { color: var(--err); border-left-color: var(--err); background: var(--err-bg); }
  .log-row.cont { padding-left: 34px; opacity: .85; }
  .log-row.late .ts::after { content: " (late)"; color: var(--warn); }
  .chip { cursor: pointer; border-bottom: 1px dotted currentColor; }
  #pager { display: none; gap: 8px; align-items: center; padding: 6px 16px; background: var(--panel); border-bottom: 1px solid var(--line); }
  #pager.on { display: flex; }
  #pager button[disabled] { opacity: .4; cursor: default; }
  #pager .pg { color: var(--text); font-weight: 600; min-width: 64px; text-align: center; }
  #log-container.busy { cursor: progress; }
  #s-search.working { color: var(--warn); }
  mark.hl { background: var(--mark); color: #111; border-radius: 2px; padding: 0 1px; }
  .chip.uname { color: var(--c-name); }
  .chip.trace { color: var(--c-trace); } .chip.user { color: var(--c-user); } .chip.run { color: var(--c-run); }
  .chip:hover { background: var(--hover); }
  table { border-collapse: collapse; width: 100%; }
  th { text-align: left; font-weight: 600; color: var(--muted); padding: 8px 12px; position: sticky; top: 0; background: var(--panel); border-bottom: 1px solid var(--line); z-index: 1; }
  td { padding: 7px 12px; border-bottom: 1px solid var(--line); vertical-align: top; }
  tbody tr { cursor: pointer; } tbody tr:hover { background: var(--panel); }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  td.mono, .mono { font-family: var(--mono); font-size: 12px; }
  td.bad { color: var(--err); font-weight: 600; }
  .empty { padding: 48px 24px; color: var(--muted); max-width: 560px; line-height: 1.6; }
  .path { display: flex; flex-wrap: wrap; gap: 4px; align-items: center; }
  .hopchip { cursor: default; }
  .arrow { color: var(--muted); }
  .stats-bar { background: var(--panel); border-top: 1px solid var(--line); padding: 6px 16px; color: var(--muted); display: flex; gap: 18px; flex-wrap: wrap; font-size: 12px; }
  .stats-bar .sp { margin-left: auto; }
  #drawer { position: absolute; top: 0; right: 0; bottom: 0; width: min(900px, 94vw); background: var(--panel); border-left: 1px solid var(--line); box-shadow: -12px 0 28px rgba(0,0,0,.45); display: flex; flex-direction: column; z-index: 5; }
  #drawer.slide { animation: slidein .16s ease-out; }
  @keyframes slidein { from { transform: translateX(24px); opacity: 0; } to { transform: none; opacity: 1; } }
  .d-head { padding: 14px 18px; border-bottom: 1px solid var(--line); display: flex; flex-direction: column; gap: 10px; }
  .d-title { display: flex; gap: 10px; align-items: baseline; flex-wrap: wrap; }
  .d-title h2 { font-size: 15px; font-weight: 650; font-family: var(--mono); word-break: break-all; }
  .d-actions { margin-left: auto; display: flex; gap: 6px; }
  .d-body { flex: 1; overflow: auto; }
  .slow { color: var(--warn); font-weight: 600; }
  .lanes th { white-space: nowrap; text-align: center; }
  .lanes td { padding: 4px 8px; font-family: var(--mono); font-size: 12px; border-bottom: 1px solid var(--hair); }
  .lanes tbody tr { cursor: default; }
  .lanes td.laneb { border-left: 1px dashed var(--line); max-width: 340px; word-break: break-word; white-space: pre-wrap; }
  .lanes td.laneb.hit { background: var(--hit); }
  .lanes td.t { color: var(--muted); white-space: nowrap; }
  .lanes td.d { color: var(--muted); text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }
  @media (prefers-reduced-motion: reduce) { #drawer.slide { animation: none; } }
</style>
</head>
<body>
<header>
  <div class="brand"><span class="dot" id="conn-dot"></span><span>AIV Cluster Log Console</span></div>
  <div class="tabs" id="tabs">
    <button class="active" data-tab="live">Live</button>
    <button data-tab="traces">Traces</button>
    <button data-tab="users">Users</button>
    <button data-tab="runs">Runs</button>
  </div>
  <div class="controls">
    <input type="text" id="f-q" placeholder="Search messages">
    <input type="text" id="f-user" placeholder="User">
    <input type="text" id="f-trace" placeholder="Trace id">
    <input type="text" id="f-run" placeholder="Run key">
    <select id="f-level" title="Minimum level (in Traces and Users: group contains at least this level)">
      <option value="ALL">All levels</option><option value="DEBUG">Debug and up</option><option value="INFO">Info and up</option>
      <option value="WARN">Warnings and errors</option><option value="ERROR">Errors only</option>
    </select>
    <select id="f-host"><option value="ALL">All servers</option></select>
    <select id="f-since">
      <option value="0">All time</option><option value="300">Last 5 min</option><option value="900">Last 15 min</option>
      <option value="3600">Last hour</option><option value="21600">Last 6 hours</option>
    </select>
    <button id="btn-order" title="Which end of the list the newest line is on">Newest first</button>
    <button id="btn-pause">Pause</button>
    <button id="btn-theme" title="Switch between dark and light mode">Light mode</button>
    <button id="btn-scroll">Auto-scroll: on</button>
    <select id="f-export" title="Download what you see, using the current filters">
      <option value="">Download...</option><option value="log">consolidated .log</option>
      <option value="jsonl">structured .jsonl</option><option value="csv">spreadsheet .csv</option>
    </select>
  </div>
</header>
<div id="pager">
  <button id="pg-first" title="Newest lines">&laquo; First</button>
  <button id="pg-prev">&lsaquo; Newer</button>
  <span class="pg" id="pg-num">Page 1</span>
  <button id="pg-next">Older &rsaquo;</button>
  <select id="pg-size" title="Lines per page">
    <option value="100">100 / page</option><option value="200" selected>200 / page</option>
    <option value="500">500 / page</option><option value="1000">1000 / page</option>
  </select>
  <span class="hint" id="pg-info"></span>
</div>
<div id="nodes"><span class="hint">Waiting for servers to send logs...</span></div>
<main>
  <section class="view" id="view-live"><div id="log-container"></div></section>
  <section class="view hidden" id="view-traces"></section>
  <section class="view hidden" id="view-users"></section>
  <section class="view hidden" id="view-runs"></section>
  <aside id="drawer" class="hidden">
    <div class="d-head">
      <div class="d-title"><h2 id="d-id"></h2><span class="hint" id="d-kind"></span>
        <div class="d-actions">
          <button id="d-live">Filter live view</button>
          <button id="d-dl">Download</button>
          <button id="d-close">Close</button>
        </div>
      </div>
      <div id="d-summary" class="hint"></div>
      <div id="d-path" class="path"></div>
    </div>
    <div class="d-body" id="d-body"></div>
  </aside>
</main>
<div class="stats-bar">
  <span id="s-recv">0 lines received</span>
  <span id="s-late"></span>
  <span id="s-search"></span>
  <span id="s-file"></span>
  <span class="sp" id="s-dir"></span>
</div>

<script>
(function () {
  'use strict';
  var LV = { TRACE: 5, DEBUG: 10, INFO: 20, WARN: 30, ERROR: 40, FATAL: 50 };
  var MAX_EVENTS = 20000, MAX_ROWS = 600, LONG_MSG = 1500;
  var $ = function (id) { return document.getElementById(id); };
  var events = [], lastId = 0, latestTs = 0, paused = false, pendingCount = 0, autoscroll = true;
  var newestFirst = true, lastRow = null;
  var searchAbort = null, searchTimer = null, liveQueue = [], liveTimer = null;
  var searchMode = false, searchEvents = [], searchSeq = 0, page = 1, hasMore = false;
  function pageSize() { return parseInt($('pg-size').value, 10) || 200; }
  try { if (localStorage.getItem('aivLogOrder') === 'oldest') newestFirst = false; } catch (e) {}
  var tab = 'live', flow = null, F = null, hostsSeen = {};
  var container = $('log-container');

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined && text !== null) e.textContent = text;
    return e;
  }
  function hostColor(h) {
    var x = 0;
    for (var i = 0; i < h.length; i++) x = (x * 31 + h.charCodeAt(i)) >>> 0;
    return 'hsl(' + (x % 360) + ',48%,34%)';
  }
  function hostChip(h, cls) {
    var s = el('span', cls || 'hopchip', h);
    s.style.background = hostColor(h);
    return s;
  }
  function fmtDur(ms) {
    if (ms < 1000) return ms + ' ms';
    if (ms < 60000) return (ms / 1000).toFixed(ms < 10000 ? 2 : 1) + ' s';
    return Math.floor(ms / 60000) + ' m ' + Math.round((ms % 60000) / 1000) + ' s';
  }
  function fmtSize(b) {
    if (b < 1024) return b + ' B';
    if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
    return (b / 1048576).toFixed(1) + ' MB';
  }
  function timeOnly(t) { return t && t.length > 11 ? t.slice(11) : t; }

  /* ---------- filters ---------- */
  function readFilters() {
    return {
      q: $('f-q').value.trim().toLowerCase(), level: $('f-level').value, host: $('f-host').value,
      user: $('f-user').value.trim().toLowerCase(), trace: $('f-trace').value.trim().toLowerCase(),
      run: $('f-run').value.trim().toLowerCase(),
      since: parseInt($('f-since').value, 10) || 0
    };
  }
  function matches(ev, f) {
    if (f.host !== 'ALL' && ev.host !== f.host) return false;
    if (f.level !== 'ALL' && (LV[ev.level] || 0) < LV[f.level]) return false;
    if (f.user && !userHit(ev, f.user)) return false;
    if (f.trace && (ev.trace || '').toLowerCase().indexOf(f.trace) < 0) return false;
    if (f.run && (ev.run || '').toLowerCase().indexOf(f.run) < 0) return false;
    if (f.since && ev.ts < latestTs - f.since) return false;
    if (f.q && (ev.msg + ' ' + ev.host + ' ' + (ev.trace || '') + ' ' + (ev.user || '') + ' ' + (ev.uname || '') + ' ' + (ev.run || '')).toLowerCase().indexOf(f.q) < 0) return false;
    return true;
  }
  function filterParams() {
    var f = readFilters(), p = new URLSearchParams();
    if (f.host !== 'ALL') p.set('host', f.host);
    if (f.level !== 'ALL') p.set('level', f.level);
    if (f.user) p.set('user', f.user);
    if (f.trace) p.set('trace', f.trace);
    if (f.run) p.set('run', f.run);
    if (f.q) p.set('q', f.q);
    if (f.since) p.set('since', f.since);
    return p;
  }
  function escRx(x) { return x.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
  function userInMsg(msg, u) {
    var rx = new RegExp('(?:user[_-]?id|username|user|login)["\']?\\s*[:=]\\s*["\']?[\\w\\-.@]*' + escRx(u), 'i');
    return rx.test((msg || '').slice(0, 3000));
  }
  function userHit(ev, u) {
    return ((ev.user || '') + ' ' + (ev.uname || '')).toLowerCase().indexOf(u) >= 0 || userInMsg(ev.msg, u);
  }
  function hlNode(text, terms) {
    var span = document.createDocumentFragment();
    terms = terms.filter(function (t) { return t; });
    if (!terms.length) { span.appendChild(document.createTextNode(text)); return span; }
    var rx = new RegExp('(' + terms.map(escRx).join('|') + ')', 'ig'), last = 0, m;
    while ((m = rx.exec(text)) !== null) {
      if (m[0] === '') { rx.lastIndex++; continue; }
      if (m.index > last) span.appendChild(document.createTextNode(text.slice(last, m.index)));
      var mk = document.createElement('mark'); mk.className = 'hl'; mk.textContent = m[0];
      span.appendChild(mk); last = m.index + m[0].length;
    }
    if (last < text.length) span.appendChild(document.createTextNode(text.slice(last)));
    return span;
  }
  function filtersActive(f) {
    return !!(f.q || f.user || f.trace || f.run || f.level !== 'ALL' || f.host !== 'ALL' || f.since);
  }
  function setPager() {
    var on = searchMode && tab === 'live';
    $('pager').classList.toggle('on', on);
    if (!on) return;
    $('pg-num').textContent = 'Page ' + page;
    $('pg-first').disabled = $('pg-prev').disabled = page <= 1;
    $('pg-next').disabled = !hasMore;
    var n = searchEvents.length, from = (page - 1) * pageSize() + 1;
    $('pg-info').textContent = n ? 'lines ' + from.toLocaleString() + ' - ' + (from + n - 1).toLocaleString() +
      (hasMore ? ' (more on older pages)' : ' (end of results)') : '';
  }
  function applyFilters(keepPage) {
    var box = $('s-search');
    if (!filtersActive(F)) {
      searchSeq++; searchMode = false; searchEvents = []; page = 1; hasMore = false;
      if (searchAbort) { try { searchAbort.abort(); } catch (e) {} }
      if (searchTimer) { clearInterval(searchTimer); searchTimer = null; }
      box.classList.remove('working'); liveQueue = [];
      box.textContent = ''; container.classList.remove('busy'); renderLive(); setPager(); return;
    }
    if (!keepPage) page = 1;
    var seq = ++searchSeq, p = filterParams(), size = pageSize();
    p.set('limit', String(size)); p.set('offset', String((page - 1) * size));
    if (searchAbort) { try { searchAbort.abort(); } catch (e) {} }
    searchAbort = (typeof AbortController !== 'undefined') ? new AbortController() : null;
    if (searchTimer) clearInterval(searchTimer);
    var t0 = Date.now();
    box.textContent = 'Searching all logs...'; box.classList.add('working');
    searchTimer = setInterval(function () {
      box.textContent = 'Searching all logs... ' + Math.round((Date.now() - t0) / 1000) + 's (a new filter cancels this)';
    }, 1000);
    container.classList.add('busy');
    function endSearch() { if (seq === searchSeq) { clearInterval(searchTimer); searchTimer = null; box.classList.remove('working'); } }
    fetch('/api/search?' + p, searchAbort ? { signal: searchAbort.signal } : {}).then(function (r) { return r.json(); }).then(function (d) {
      if (seq !== searchSeq) return;
      endSearch();
      if (d.error) throw new Error(d.error);
      searchMode = true; searchEvents = d.events || []; hasMore = !!d.has_more;
      box.textContent = d.partial ? 'Search stopped after the time limit; narrow the filter or time range' : '';
      container.classList.remove('busy');
      renderLive(); setPager();
      $('view-live').scrollTop = newestFirst ? 0 : $('view-live').scrollHeight;
    }).catch(function () {
      if (seq !== searchSeq) return;
      endSearch();
      searchMode = false; container.classList.remove('busy');
      box.textContent = 'Full search failed, showing recent lines only'; renderLive(); setPager();
    });
  }
  function refilter() {
    F = readFilters();
    if (tab === 'live') applyFilters(false); else { refreshTab(); setPager(); }
    syncNodes();
  }

  /* ---------- live view ---------- */
  function rowFor(ev) {
    var d = el('div', 'log-row lvl-' + ev.level + (ev.cont ? ' cont' : '') + (ev.late ? ' late' : ''));
    if (!ev.cont) {
      var t = el('span', 'ts', timeOnly(ev.t));
      t.title = ev.t + (ev.late ? ' - arrived after newer events from other servers' : '');
      d.appendChild(t); d.appendChild(document.createTextNode(' '));
      var h = hostChip(ev.host, 'host'); h.dataset.host = ev.host; h.title = 'Show only ' + ev.host;
      d.appendChild(h); d.appendChild(document.createTextNode(' '));
      d.appendChild(el('span', 'lvl', ev.level)); d.appendChild(document.createTextNode(' '));
      if (ev.trace) {
        var c = el('span', 'chip trace', 'trace:' + ev.trace); c.dataset.trace = ev.trace; c.title = 'Open flow';
        d.appendChild(c); d.appendChild(document.createTextNode(' '));
      }
      if (ev.user) {
        var u = el('span', 'chip user', ''); u.appendChild(document.createTextNode('user:'));
        u.appendChild(hlNode(ev.user, [F.user, F.q])); u.dataset.user = ev.user; u.title = 'Open user timeline';
        d.appendChild(u); d.appendChild(document.createTextNode(' '));
      }
      if (ev.uname) {
        var un = el('span', 'chip uname', ''); un.appendChild(document.createTextNode('name:'));
        un.appendChild(hlNode(ev.uname, [F.user, F.q])); un.title = 'Username on this line';
        d.appendChild(un); d.appendChild(document.createTextNode(' '));
      }
      if (ev.run) {
        var r = el('span', 'chip run', 'run:' + ev.run); r.dataset.run = ev.run; r.title = 'Open run flow';
        d.appendChild(r); d.appendChild(document.createTextNode(' '));
      }
    }
    var mg = el('span', 'msg', '');
    var full = ev.msg || '';
    if (full.length > LONG_MSG && !(F.q || F.user)) {
      mg.appendChild(document.createTextNode(full.slice(0, 1200) + ' '));
      var more = el('span', 'more', '... show more (' + (full.length - 1200).toLocaleString() + ' chars)');
      more.addEventListener('click', function (e) {
        e.stopPropagation(); mg.innerHTML = ''; mg.appendChild(hlNode(full, [F.user, F.q]));
      });
      mg.appendChild(more);
    } else {
      mg.appendChild(hlNode(full, [F.user, F.q]));
    }
    d.appendChild(mg);
    return d;
  }
  function groupsOf(chron) {
    // a stack-trace line belongs to the line above it: keep each group together
    var gs = [], cur = null;
    chron.forEach(function (e) {
      if (e.cont && cur) cur.push(e); else { cur = [e]; gs.push(cur); }
    });
    return gs;
  }
  function stick() {
    var v = container.parentNode;
    v.scrollTop = newestFirst ? 0 : v.scrollHeight;
  }
  function renderLive() {
    var frag = document.createDocumentFragment(), chron = [];
    var list = searchMode ? searchEvents : events, cap = searchMode ? 1000000 : MAX_ROWS;
    for (var i = list.length - 1; i >= 0 && chron.length < cap; i--) {
      if (searchMode || matches(list[i], F)) chron.push(list[i]);
    }
    chron.reverse();                       // oldest -> newest
    var ordered = chron;
    if (newestFirst) {
      ordered = [];
      groupsOf(chron).reverse().forEach(function (g) { ordered = ordered.concat(g); });
    }
    lastRow = null;
    for (var j = 0; j < ordered.length; j++) { lastRow = rowFor(ordered[j]); frag.appendChild(lastRow); }
    container.innerHTML = '';
    if (!chron.length) {
      container.appendChild(el('div', 'empty', (events.length || searchMode)
        ? (searchMode ? 'No lines match these filters in any log.' : 'No lines match these filters.')
        : 'No logs yet. Watching files (if configured) and TCP 5044 / 5045. New or recent lines will appear here.'));
    }
    container.appendChild(frag);
    lastRow = null;
    liveQueue = []; pendingCount = 0; updatePause(); updateScrollBtn();
    if (autoscroll) stick();
  }
  var scrollQueued = false;
  function queueScroll() {
    if (scrollQueued) return;
    scrollQueued = true;
    requestAnimationFrame(function () {
      scrollQueued = false;
      if (autoscroll) stick();
    });
  }
  function appendLive(ev) {
    // new lines are collected and drawn together a few times a second, not one by one
    liveQueue.push(ev);
    if (liveTimer) return;
    liveTimer = setTimeout(flushLive, 250);
  }
  function flushLive() {
    liveTimer = null;
    var batch = liveQueue; liveQueue = [];
    if (!batch.length) return;
    if (!autoscroll) {                          // reading older lines: keep the list still
      pendingCount += batch.length; updateScrollBtn(); return;
    }
    if (container.firstChild && container.firstChild.className === 'empty') container.innerHTML = '';
    var cap = searchMode ? pageSize() : MAX_ROWS;
    if (batch.length > cap) batch = batch.slice(batch.length - cap);
    for (var i = 0; i < batch.length; i++) {
      var ev = batch[i], row = rowFor(ev);
      if (newestFirst) {
        if (ev.cont && lastRow && lastRow.parentNode === container) container.insertBefore(row, lastRow.nextSibling);
        else container.insertBefore(row, container.firstChild);
      } else {
        container.appendChild(row);
      }
      lastRow = row;
    }
    var over = container.children.length - cap;
    while (over-- > 0) container.removeChild(newestFirst ? container.lastChild : container.firstChild);
    if (autoscroll) stick();
  }
  function updatePause() {
    $('btn-pause').textContent = paused ? 'Resume' + (pendingCount ? ' (' + pendingCount + ' new)' : '') : 'Pause';
    $('btn-pause').className = paused ? 'primary' : '';
  }
  function onEvent(ev) {
    if (ev.id <= lastId) return;
    lastId = ev.id;
    if (ev.ts > latestTs) latestTs = ev.ts;
    events.push(ev);
    if (events.length > MAX_EVENTS + 1000) events.splice(0, events.length - MAX_EVENTS);
    if (!hostsSeen[ev.host]) addHost(ev.host);
    if (tab === 'live' && matches(ev, F)) {
      if (searchMode && page > 1) return;      // older pages stay still; new lines belong to page 1
      if (searchMode) { searchEvents.push(ev); if (searchEvents.length > pageSize() + 50) searchEvents.splice(0, 50); }
      if (paused) { pendingCount++; updatePause(); } else appendLive(ev);
    }
  }
  function addHost(h) {
    hostsSeen[h] = true;
    var o = el('option', '', h); o.value = h;
    $('f-host').appendChild(o);
  }

  /* ---------- traces / users tables ---------- */
  function pathCell(hosts) {
    var d = el('div', 'path');
    hosts.forEach(function (h, i) {
      if (i) d.appendChild(el('span', 'arrow', '\u2192'));
      d.appendChild(hostChip(h));
    });
    return d;
  }
  function buildTable(cols, rows, onClick, emptyMsg) {
    var view = el('div');
    if (!rows.length) { view.appendChild(el('div', 'empty', emptyMsg)); return view; }
    var t = el('table'), thead = el('thead'), tr = el('tr');
    cols.forEach(function (c) { tr.appendChild(el('th', '', c)); });
    thead.appendChild(tr); t.appendChild(thead);
    var tb = el('tbody');
    rows.forEach(function (r) {
      var row = el('tr');
      r.cells.forEach(function (c) {
        var td = el('td', c.cls || '');
        if (c.node) td.appendChild(c.node); else td.textContent = c.text;
        row.appendChild(td);
      });
      row.addEventListener('click', function () { onClick(r.id); });
      tb.appendChild(row);
    });
    t.appendChild(tb); view.appendChild(t);
    return view;
  }
  function loadTable(kind) {
    var p = filterParams(); p.set('limit', '300');
    fetch('/api/' + kind + 's?' + p).then(function (r) { return r.json(); }).then(function (d) {
      if (tab !== kind + 's') return;
      var rows = d.rows.map(function (r) {
        var cells = [{ text: r.id, cls: 'mono' }, { text: timeOnly(r.t_last), cls: 'mono' }];
        if (kind === 'trace' || kind === 'run') cells.push({ text: fmtDur(r.duration_ms), cls: 'num' });
        cells.push({ node: pathCell(r.hosts) });
        cells.push({ text: String(r.events), cls: 'num' });
        cells.push({ text: String(r.errors), cls: 'num' + (r.errors ? ' bad' : '') });
        cells.push({ text: r.related_sample.join(', '), cls: '' });
        return { id: r.id, cells: cells };
      });
      var cols = kind === 'trace'
        ? ['Trace', 'Last seen', 'Duration', 'Path across servers', 'Lines', 'Errors', 'User / run']
        : (kind === 'run'
          ? ['Run key', 'Last seen', 'Duration', 'Path across servers', 'Lines', 'Errors', 'Trace / user']
          : ['User', 'Last seen', 'Servers touched', 'Lines', 'Errors', 'Traces / runs']);
      var msg = kind === 'trace'
        ? 'No strong traces yet. Lines need a real UUID in trace=... (Schedule/Scheduler/empty are ignored). Path shows host hop order by time.'
        : (kind === 'run'
          ? 'No run keys yet. AIV jobs put run={recurid}_{timeschedulerid} in the bracket - that stitches hops when trace is missing on one server.'
          : 'No users yet. Lines need user=... (or actor/owner). Missing user on a hop is filled from the same trace/run on another server.');
      var view = $('view-' + kind + 's'), top = view.scrollTop;
      view.innerHTML = '';
      view.appendChild(buildTable(cols, rows, function (id) { openFlow(kind, id); }, msg));
      view.scrollTop = top;
    }).catch(function () {});
  }
  function refreshTab() {
    if (tab === 'traces') loadTable('trace');
    else if (tab === 'users') loadTable('user');
    else if (tab === 'runs') loadTable('run');
  }

  /* ---------- flow drawer ---------- */
  function openFlow(kind, id) {
    flow = { kind: kind, id: id };
    var d = $('drawer'); d.classList.remove('hidden'); d.classList.add('slide');
    $('d-id').textContent = id;
    $('d-kind').textContent = kind === 'trace' ? 'trace across servers'
      : (kind === 'run' ? 'run key across servers' : 'user timeline');
    $('d-body').innerHTML = ''; $('d-summary').textContent = 'Loading...'; $('d-path').innerHTML = '';
    refreshFlow();
  }
  function closeFlow() { flow = null; $('drawer').classList.add('hidden'); }
  function refreshFlow() {
    if (!flow) return;
    var cur = flow;
    fetch('/api/flow?' + cur.kind + '=' + encodeURIComponent(cur.id)).then(function (r) { return r.json(); })
      .then(function (d) { if (flow === cur && !d.error) renderFlow(d); }).catch(function () {});
  }
  function renderFlow(d) {
    var s = d.summary;
    if (!s.count) {
      $('d-summary').textContent = 'No lines in memory for this ' + d.kind + '. Older lines are in consolidated.jsonl.';
      return;
    }
    $('d-summary').textContent = s.count + ' lines over ' + fmtDur(s.duration_ms) + ', ' + s.hosts.length +
      ' server' + (s.hosts.length === 1 ? '' : 's') + ', ' + s.errors + ' error' + (s.errors === 1 ? '' : 's') +
      (d.kind === 'user' ? ', ' + s.related.length + ' trace' + (s.related.length === 1 ? '' : 's') : '');
    var path = $('d-path'); path.innerHTML = '';
    var slowest = null;
    d.hops.forEach(function (h, i) {
      if (i) {
        var a = el('span', 'arrow' + (h.gap_ms >= 500 ? ' slow' : ''), '\u2192 +' + fmtDur(h.gap_ms));
        path.appendChild(a);
        if (!slowest || h.gap_ms > slowest.gap_ms) slowest = { gap_ms: h.gap_ms, from: d.hops[i - 1].host, to: h.host };
      }
      var chip = hostChip(h.host); chip.textContent = h.host + (h.n > 1 ? ' \u00d7' + h.n : '') + (h.errors ? ' !' : '');
      path.appendChild(chip);
    });
    if (slowest && slowest.gap_ms >= 100) {
      var note = el('div', 'hint', 'Longest wait: ' + slowest.from + ' to ' + slowest.to + ', ' + fmtDur(slowest.gap_ms));
      note.style.flexBasis = '100%';
      path.appendChild(note);
    }
    var showTrace = d.kind !== 'trace', hosts = s.hosts;
    var t = el('table', 'lanes'), head = el('tr');
    ['Time', 'Gap'].concat(showTrace ? ['Trace'] : []).forEach(function (c) { head.appendChild(el('th', '', c)); });
    hosts.forEach(function (h) { var th = el('th'); th.appendChild(hostChip(h)); head.appendChild(th); });
    var thead = el('thead'); thead.appendChild(head); t.appendChild(thead);
    var tb = el('tbody'), prev = null;
    d.events.forEach(function (ev) {
      if (ev.cont) {
        // stack trace lines: add to the previous row's cell instead of a new row
        var last = tb.lastChild;
        if (last) { var cell = last.querySelector('td.hit'); if (cell) cell.textContent += '\n' + ev.msg; }
        return;
      }
      var tr = el('tr', 'lvl-' + ev.level);
      tr.appendChild(el('td', 't', timeOnly(ev.t)));
      var gap = prev === null ? 0 : Math.round((ev.ts - prev) * 1000);
      tr.appendChild(el('td', 'd' + (gap >= 500 ? ' slow' : ''), prev === null ? '' : '+' + fmtDur(gap)));
      if (showTrace) {
        var tc = el('td', 't');
        if (ev.trace) { var c = el('span', 'chip trace', ev.trace); c.addEventListener('click', function () { openFlow('trace', ev.trace); }); tc.appendChild(c); }
        tr.appendChild(tc);
      }
      hosts.forEach(function (h) {
        var td = el('td', 'laneb' + (h === ev.host ? ' hit' : ''), h === ev.host ? ev.msg : '');
        tr.appendChild(td);
      });
      tb.appendChild(tr);
      prev = ev.ts;
    });
    t.appendChild(tb);
    var body = $('d-body'), top = body.scrollTop;
    body.innerHTML = ''; body.appendChild(t); body.scrollTop = top;
  }

  /* ---------- nodes + stats ---------- */
  var lastStats = null;
  function syncNodes() {
    if (!lastStats) return;
    var bar = $('nodes'), names = Object.keys(lastStats.servers).sort();
    bar.innerHTML = '';
    if (!names.length) { bar.appendChild(el('span', 'hint', 'Waiting for servers to send logs...')); return; }
    var total = 0, totalErr = 0;
    names.forEach(function (h) { total += lastStats.servers[h].events; totalErr += lastStats.servers[h].errors; });
    var allNode = el('div', 'node' + ($('f-host').value === 'ALL' ? ' sel' : ''));
    var allChip = el('span', 'hopchip', 'All');
    allChip.style.background = '#3b5b73';
    allNode.appendChild(el('span', 'dot ok')); allNode.appendChild(allChip);
    allNode.appendChild(el('span', 'n', total.toLocaleString()));
    if (totalErr) allNode.appendChild(el('span', 'e', totalErr.toLocaleString() + ' err'));
    allNode.title = 'Show every server together (server1, server2 and all nodes)';
    allNode.addEventListener('click', function () { $('f-host').value = 'ALL'; refilter(); });
    bar.appendChild(allNode);
    names.forEach(function (h) {
      var s = lastStats.servers[h];
      var n = el('div', 'node' + ($('f-host').value === h ? ' sel' : ''));
      var dot = el('span', 'dot ' + (s.age < 15 ? 'ok' : (s.age < 120 ? 'warn' : '')));
      n.appendChild(dot); n.appendChild(hostChip(h)); n.appendChild(el('span', 'n', s.events.toLocaleString()));
      if (s.errors) n.appendChild(el('span', 'e', s.errors + ' err'));
      n.title = s.age < 15 ? 'Sending logs' : 'Last log ' + Math.round(s.age) + ' s ago';
      n.addEventListener('click', function () {
        $('f-host').value = $('f-host').value === h ? 'ALL' : h; refilter();
      });
      bar.appendChild(n);
    });
  }
  function loadStats() {
    fetch('/api/stats').then(function (r) { return r.json(); }).then(function (s) {
      lastStats = s;
      Object.keys(s.servers).forEach(function (h) { if (!hostsSeen[h]) addHost(h); });
      syncNodes();
      $('s-recv').textContent = s.received.toLocaleString() + ' lines received, ' + s.consolidated.toLocaleString() + ' consolidated'
        + (s.enriched ? ', ' + s.enriched.toLocaleString() + ' join-enriched' : '');
      $('s-late').textContent = s.late ? s.late + ' arrived out of order' : '';
      var f = s.files[0];
      $('s-file').textContent = f ? f.name + ' ' + fmtSize(f.size) : '';
      $('s-dir').textContent = s.output_directory + ' \u00b7 times in ' + s.timezone;
    }).catch(function () {});
  }

  /* ---------- wiring ---------- */
  function setTab(name) {
    tab = name;
    Array.prototype.forEach.call($('tabs').children, function (b) { b.classList.toggle('active', b.dataset.tab === name); });
    ['live', 'traces', 'users', 'runs'].forEach(function (v) { $('view-' + v).classList.toggle('hidden', v !== name); });
    if (name === 'live') renderLive(); else refreshTab();
    setPager();
  }
  $('tabs').addEventListener('click', function (e) { if (e.target.dataset.tab) setTab(e.target.dataset.tab); });
  var debounce = null;
  ['f-q', 'f-user', 'f-trace', 'f-run'].forEach(function (id) {
    $(id).addEventListener('input', function () { clearTimeout(debounce); debounce = setTimeout(refilter, 350); });
  });
  ['f-level', 'f-host', 'f-since'].forEach(function (id) { $(id).addEventListener('change', refilter); });
  $('btn-pause').addEventListener('click', function () {
    paused = !paused;
    if (!paused) renderLive(); else updatePause();
  });
  function updateScrollBtn() {
    $('btn-scroll').textContent = 'Auto-scroll: ' + (autoscroll ? 'on' : 'off' + (pendingCount && !paused ? ' (' + pendingCount.toLocaleString() + ' new)' : ''));
  }
  $('btn-scroll').addEventListener('click', function () {
    autoscroll = !autoscroll; updateScrollBtn();
    if (autoscroll) { if (pendingCount) renderLive(); else stick(); }
  });
  $('view-live').addEventListener('scroll', function () {
    var v = $('view-live');
    var atNewest = newestFirst ? v.scrollTop < 40 : v.scrollHeight - v.scrollTop - v.clientHeight < 40;
    if (atNewest !== autoscroll) {
      autoscroll = atNewest; updateScrollBtn();
      if (autoscroll && pendingCount && !paused) renderLive();   // back at the newest line: show what arrived
    }
  });
  function curTheme() { return document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark'; }
  function updateThemeBtn() { $('btn-theme').textContent = curTheme() === 'light' ? 'Dark mode' : 'Light mode'; }
  $('btn-theme').addEventListener('click', function () {
    var next = curTheme() === 'light' ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', next);
    try { localStorage.setItem('aivTheme', next); } catch (e) {}
    updateThemeBtn();
  });
  updateThemeBtn();
  $('pg-first').addEventListener('click', function () { page = 1; applyFilters(true); });
  $('pg-prev').addEventListener('click', function () { if (page > 1) { page--; applyFilters(true); } });
  $('pg-next').addEventListener('click', function () { if (hasMore) { page++; applyFilters(true); } });
  $('pg-size').addEventListener('change', function () { page = 1; applyFilters(true); });
  function updateOrderBtn() { $('btn-order').textContent = newestFirst ? 'Newest first' : 'Oldest first'; }
  $('btn-order').addEventListener('click', function () {
    newestFirst = !newestFirst; updateOrderBtn();
    try { localStorage.setItem('aivLogOrder', newestFirst ? 'newest' : 'oldest'); } catch (e) {}
    autoscroll = true; updateScrollBtn();
    renderLive();
  });
  updateOrderBtn();
  $('f-export').addEventListener('change', function () {
    var fmt = $('f-export').value;
    if (fmt) { var p = filterParams(); p.set('format', fmt); window.location = '/api/export?' + p; }
    $('f-export').value = '';
  });
  container.addEventListener('click', function (e) {
    var d = e.target.dataset;
    if (d.trace) openFlow('trace', d.trace);
    else if (d.user) openFlow('user', d.user);
    else if (d.run) openFlow('run', d.run);
    else if (d.host) { $('f-host').value = d.host; refilter(); }
  });
  $('d-close').addEventListener('click', closeFlow);
  $('d-live').addEventListener('click', function () {
    if (!flow) return;
    $('f-trace').value = flow.kind === 'trace' ? flow.id : '';
    $('f-user').value = flow.kind === 'user' ? flow.id : '';
    $('f-run').value = flow.kind === 'run' ? flow.id : '';
    closeFlow(); setTab('live'); refilter();
  });
  $('d-dl').addEventListener('click', function () {
    if (flow) window.location = '/api/export?format=log&' + flow.kind + '=' + encodeURIComponent(flow.id);
  });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeFlow(); });

  function connect() {
    var es = new EventSource('/stream?after=' + lastId);
    es.onopen = function () { $('conn-dot').className = 'dot ok'; };
    es.onerror = function () { $('conn-dot').className = 'dot bad'; };
    es.onmessage = function (e) { try { onEvent(JSON.parse(e.data)); } catch (err) {} };
  }
  F = readFilters();
  fetch('/api/events?limit=5000').then(function (r) { return r.json(); }).then(function (d) {
    events = d.events || [];
    events.forEach(function (ev) { if (ev.ts > latestTs) latestTs = ev.ts; if (!hostsSeen[ev.host]) addHost(ev.host); });
    lastId = d.max_id || 0;
    renderLive();
  }).catch(function () { renderLive(); }).then(function () { connect(); });
  loadStats();
  setInterval(loadStats, 3000);
  setInterval(function () { refreshTab(); refreshFlow(); }, 3000);
})();
</script>
</body>
</html>
"""


# ==============================================================================
# Demo traffic: four servers, skewed clocks, batched/lagged shipping
# ==============================================================================
async def demo_traffic():
    await asyncio.sleep(0.8)
    users = ["alice", "bob", "carol", "dave", "erin", "frank"]
    cfg = {
        "web01": {"skew": 0.0, "port": PLAIN_PORT, "json": False},
        "app01": {"skew": -0.6, "port": PLAIN_PORT, "json": False},
        "db01": {"skew": 0.9, "port": JSON_PORT, "json": True, "channel": "db"},
        "auth01": {"skew": 0.3, "port": JSON_PORT, "json": True, "channel": "security"},
    }
    buffers = {h: [] for h in cfg}
    writers = {}
    for h, c in cfg.items():
        _r, w = await asyncio.open_connection("127.0.0.1", c["port"])
        writers[h] = w

    def emit(h, level, msg, trace, user, run=None, omit_user=False, omit_trace=False):
        """Emit AIV logback-shaped lines (and JSON) including empty-capable join keys."""
        c = cfg[h]
        ts = time.time() + c["skew"]
        tr = "" if omit_trace else (trace or "")
        us = "" if omit_user else (user or "")
        rn = run or ""
        if c["json"]:
            iso = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            body = {"ts": iso, "hostname": h, "channel": c["channel"], "level": level,
                    "message": msg, "lane": "%s|all|8080" % h}
            if tr:
                body["traceid"] = tr
            if us:
                body["user"] = us
            if rn:
                body["run"] = rn
            buffers[h].append(json.dumps(body) + "\n")
        else:
            stamp = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            # Mirror real AIV plain pattern: trace= user= dept= run=
            buffers[h].append(
                "%s %-5s [http-nio-8080-exec-1] core "
                "[lane=%s|all|8080 host=%s role=all pid=1 port=8080 "
                "trace=%s user=%s dept=Default run=%s from= src=] - %s\n"
                % (stamp, level, h, h, tr, us, rn, msg))

    def emit_raw(h, text):
        buffers[h].append(text + "\n")

    async def shipper(h):
        while True:
            await asyncio.sleep(random.uniform(0.2, 1.4))  # shipper batching / network lag
            data = "".join(buffers[h])
            buffers[h] = []
            if data:
                writers[h].write(data.encode("utf-8"))
                await writers[h].drain()

    async def one_request():
        trace = uuid.uuid4().hex  # full UUID like AuthFilter
        user = random.choice(users)
        run = "%d_%d" % (random.randint(1000, 9999), random.randint(10000, 99999))
        path = random.choice(["/orders", "/orders/42", "/invoices", "/reports/monthly"])
        fail = random.random() < 0.12
        # API node has full join keys
        emit("web01", "INFO", "GET %s received" % path, trace, user, run)
        await asyncio.sleep(random.uniform(0.005, 0.03))
        emit("auth01", "INFO", "token verified for %s" % user, trace, user, run)
        await asyncio.sleep(random.uniform(0.005, 0.02))
        # Worker hop often loses user in real life - consolidator must re-stitch via trace/run
        emit("app01", "INFO", "Kafka consume / handling %s" % path, trace, user, run, omit_user=True)
        await asyncio.sleep(random.uniform(0.01, 0.06))
        emit("db01", "INFO", "SELECT * FROM orders WHERE user_id = ?", trace, user, run, omit_user=True)
        await asyncio.sleep(random.uniform(0.02, 0.4 if not fail else 1.2))
        if fail:
            emit("db01", "ERROR", "deadlock detected, transaction rolled back", trace, user, run, omit_user=True)
            emit("app01", "ERROR", "query failed: deadlock detected", trace, user, run, omit_user=True)
            emit_raw("app01", "\tat com.aiv.OrderRepository.load(OrderRepository.java:88)")
            emit_raw("app01", "\tat com.aiv.OrderService.handle(OrderService.java:211)")
            await asyncio.sleep(0.01)
            emit("web01", "WARN", "GET %s -> 500" % path, trace, user, run)
        else:
            emit("app01", "INFO", "built response for %s" % path, trace, user, run, omit_user=True)
            await asyncio.sleep(0.01)
            emit("web01", "INFO", "GET %s -> 200" % path, trace, user, run)

    async def generator():
        while True:
            await asyncio.sleep(random.expovariate(6))
            asyncio.ensure_future(one_request())

    tasks = [asyncio.ensure_future(shipper(h)) for h in cfg] + [asyncio.ensure_future(generator())]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        raise


# ==============================================================================
# File watch: nested folders, mixed formats, continuous (or once) ingest
# ==============================================================================
def _norm_path(path):
    return os.path.normcase(os.path.abspath(path))


def _channel_from_name(name):
    stem = name.lower()
    for suffix in (".gz", ".log", ".txt", ".jsonl", ".out"):
        if stem.endswith(suffix):
            stem = stem[:-len(suffix)]
    for ch in KNOWN_CHANNELS:
        if stem == ch or stem.startswith(ch + "_") or stem.startswith(ch + "-") or stem.endswith("_" + ch):
            return ch
    return "plain"


def _local_host():
    try:
        import socket
        return socket.gethostname() or "localhost"
    except Exception:
        return "localhost"


def _host_from_rel(rel_path, structure):
    """Infer host from relative path under the watch root.

    nested: <root>/<host>/.../file.log  -> host
            <root>/file.log (no subdir) -> local hostname
    flat:   filename stem (unless it is a known channel name)
    """
    parts = [p for p in rel_path.replace("\\", "/").split("/") if p and p not in (".",)]
    if not parts:
        return _local_host()
    if structure == "nested" and len(parts) >= 2:
        return parts[0]
    if structure == "nested":
        # file directly in the watch folder (e.g. /var/lib/aiv/logs/core.log): this machine
        return _local_host()
    stem = parts[-1]
    for suffix in (".gz", ".log", ".txt", ".jsonl", ".out"):
        if stem.lower().endswith(suffix):
            stem = stem[:-len(suffix)]
            break
    # Avoid labeling lanes as "core"/"db" when the file is just a channel log
    ch = _channel_from_name(parts[-1])
    if stem.lower() == ch or ch in KNOWN_CHANNELS and stem.lower().startswith(ch):
        return _local_host()
    return stem or _local_host()


def _is_consolidator_archive(path, out_root):
    """True for consolidator's own rotated outputs (not app logback archives)."""
    abs_path = _norm_path(path)
    out_arch = _norm_path(os.path.join(out_root, "archived"))
    if not abs_path.startswith(out_arch + os.sep):
        return False
    base = os.path.basename(path).lower()
    # consolidator rotates consolidated.log / consolidated.jsonl / all-plain.log copies
    for prefix in ("consolidated.log", "consolidated.jsonl"):
        if base.startswith(prefix):
            return True
    return False


def _is_watchable_file(path, watch_root, out_root):
    name = os.path.basename(path)
    if name in WATCH_SKIP_NAMES or name.endswith(".lck") or name.endswith(".tmp"):
        return False
    ext = os.path.splitext(name)[1].lower()
    if ext not in WATCH_EXTS and not name.endswith(".log.gz"):
        return False
    abs_path = _norm_path(path)
    # Never ingest consolidator outputs (avoids feedback loops when dirs overlap)
    for out_name in (CONSOLIDATED_LOG, CONSOLIDATED_JSONL):
        if abs_path == _norm_path(os.path.join(out_root, out_name)):
            return False
    if _is_consolidator_archive(path, out_root):
        return False
    # App logback archived/*.log.gz (see logback-spring.xml fileNamePattern)
    parts = abs_path.replace("/", os.sep).split(os.sep)
    if "archived" in [p.lower() for p in parts] and not WATCH_INCLUDE_ARCHIVED:
        return False
    return True


def discover_log_files(root, structure="nested"):
    """Yield absolute paths of log files under root (nested recurse or flat).

    Includes logback ``archived/*.log.gz`` when WATCH_INCLUDE_ARCHIVED is on
    (default). Skips the consolidator's own output archived/ rolls.
    """
    root = os.path.abspath(root)
    out_root = os.path.abspath(LOG_OUTPUT_DIR)
    if not os.path.isdir(root):
        return []
    found = []
    if structure == "flat":
        try:
            names = os.listdir(root)
        except OSError:
            return []
        for name in names:
            path = os.path.join(root, name)
            if os.path.isfile(path) and _is_watchable_file(path, root, out_root):
                found.append(path)
        # flat layout still has logback archived/ next to active logs
        arch = os.path.join(root, "archived")
        if WATCH_INCLUDE_ARCHIVED and os.path.isdir(arch):
            try:
                for name in os.listdir(arch):
                    path = os.path.join(arch, name)
                    if os.path.isfile(path) and _is_watchable_file(path, root, out_root):
                        found.append(path)
            except OSError:
                pass
        return sorted(found)

    out_arch = _norm_path(os.path.join(out_root, "archived"))
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d.lower() not in WATCH_SKIP_DIRS and not d.startswith(".")]
        # Never walk the consolidator's own archived output tree
        if _norm_path(dirpath) == out_arch or _norm_path(dirpath).startswith(out_arch + os.sep):
            dirnames[:] = []
            continue
        if not WATCH_INCLUDE_ARCHIVED:
            dirnames[:] = [d for d in dirnames if d.lower() != "archived"]
        for name in filenames:
            path = os.path.join(dirpath, name)
            if _is_watchable_file(path, root, out_root):
                found.append(path)
    return sorted(found)


def expand_merge_inputs(paths, structure="nested"):
    """Turn a mix of files/dirs into a flat file list for --merge."""
    out = []
    for p in paths:
        if os.path.isdir(p):
            out.extend(discover_log_files(p, structure))
        elif os.path.isfile(p):
            out.append(os.path.abspath(p))
        else:
            print("Not found: %s" % p, file=sys.stderr)
    # unique, stable
    seen, uniq = set(), []
    for p in out:
        key = _norm_path(p)
        if key not in seen:
            seen.add(key)
            uniq.append(p)
    return uniq


class FileTailState:
    __slots__ = ("path", "host", "channel", "offset", "inode", "size", "conn", "partial",
                 "is_gz", "gz_done")

    def __init__(self, path, host, channel, start_at_end=True, catchup_bytes=None):
        self.path = path
        self.host = host
        self.channel = channel
        self.offset = 0
        self.inode = None
        self.size = 0
        self.conn = Conn()
        self.partial = b""
        self.is_gz = path.endswith(".gz")
        self.gz_done = False
        # logback archived/*.log.gz are complete rolls - always read whole file once
        if self.is_gz:
            start_at_end = False
        try:
            st = os.stat(path)
            self.inode = (st.st_dev, st.st_ino)
            self.size = st.st_size
            if start_at_end:
                # Seed from a recent window (not true EOF) so the dashboard has history.
                window = WATCH_CATCHUP_BYTES if catchup_bytes is None else catchup_bytes
                if window <= 0:
                    self.offset = st.st_size
                else:
                    self.offset = max(0, st.st_size - window)
                    if self.offset > 0:
                        # Align to next newline so we do not start mid-line.
                        try:
                            with open(path, "rb") as f:
                                f.seek(self.offset)
                                chunk = f.read(min(65536, st.st_size - self.offset))
                                nl = chunk.find(b"\n")
                                if nl >= 0:
                                    self.offset += nl + 1
                        except OSError:
                            pass
            else:
                self.offset = 0
        except OSError:
            pass

    def read_new_lines(self):
        """Return list of complete lines (bytes) newly appended; handles truncate/rotate.

        Gzip archives (logback ``archived/*.log.gz``) are ingested once in full.
        """
        if self.is_gz:
            if self.gz_done:
                return []
            lines = []
            try:
                with gzip.open(self.path, "rb") as f:
                    for bl in f:
                        bl = bl.rstrip(b"\r\n")
                        if bl.strip():
                            lines.append(bl)
            except OSError as e:
                print("[WARN] watch read gz %s: %s" % (self.path, e), file=sys.stderr)
                return []
            self.gz_done = True
            return lines

        try:
            st = os.stat(self.path)
        except OSError:
            return []
        inode = (st.st_dev, st.st_ino)
        # Only a file that got SMALLER counts as truncated/rotated. A changed inode alone
        # does not: s3fs/FUSE can report a new inode number after its cache expires, and
        # treating that as a rotation re-read the whole file again and again.
        if st.st_size < self.offset:
            self.offset = 0
            self.partial = b""
        self.inode = inode
        self.size = st.st_size
        if st.st_size <= self.offset and not self.partial:
            return []
        lines = []
        try:
            with open(self.path, "rb") as f:
                f.seek(self.offset)
                data = f.read()
                self.offset = f.tell()
        except OSError as e:
            print("[WARN] watch read %s: %s" % (self.path, e), file=sys.stderr)
            return []
        if not data and not self.partial:
            return []
        buf = self.partial + data
        *complete, self.partial = buf.split(b"\n")
        if len(self.partial) > MAX_LINE_BYTES:
            complete.append(self.partial)
            self.partial = b""
        for bl in complete:
            if bl.strip():
                lines.append(bl)
        return lines


def _meta_for_path(path, watch_root, structure):
    rel = os.path.relpath(path, watch_root)
    host = _host_from_rel(rel, structure)
    channel = _channel_from_name(os.path.basename(path))
    return host, channel


def _state_path():
    return WATCH_STATE_FILE or os.path.join(LOG_OUTPUT_DIR, ".watch-state.json")


def _load_state():
    try:
        with open(_state_path(), "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_state(states):
    data = {}
    for key, st in states.items():
        if st.is_gz:
            if st.gz_done:
                data[key] = {"gz_done": True, "size": st.size}
        else:
            data[key] = {"offset": max(0, st.offset - len(st.partial)), "size": st.size}
    tmp = _state_path() + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, _state_path())
    except OSError as e:
        print("[WARN] could not save watch state: %s" % e, file=sys.stderr)


async def file_watch_loop(watch_root, structure="nested", fmt_mode="mixed", from_start=False):
    """Continuously discover + tail nested log files into the hub."""
    watch_root = os.path.abspath(watch_root)
    states = {}  # norm path -> FileTailState
    last_discover = 0.0
    last_save = 0.0
    start_at_end = not from_start
    saved = _load_state()
    loop = asyncio.get_running_loop()
    if saved:
        print("[INFO] Loaded read positions for %d files from %s" % (len(saved), _state_path()))

    def refresh():
        paths = discover_log_files(watch_root, structure)
        alive = set()
        for path in paths:
            key = _norm_path(path)
            alive.add(key)
            if key not in states:
                host, channel = _meta_for_path(path, watch_root, structure)
                states[key] = FileTailState(path, host, channel, start_at_end=start_at_end)
                fst = states[key]
                sv = saved.get(key)
                resumed = False
                try:
                    if fst.is_gz:
                        if sv and sv.get("gz_done") and sv.get("size") == fst.size:
                            fst.gz_done, resumed = True, True
                        elif (not from_start and WATCH_ARCHIVE_DAYS > 0 and
                              time.time() - os.path.getmtime(path) > WATCH_ARCHIVE_DAYS * 86400):
                            fst.gz_done, resumed = True, True   # old archive: skip on first sight
                    elif sv and sv.get("offset") is not None and fst.size >= sv["offset"]:
                        fst.offset, resumed = int(sv["offset"]), True
                except OSError:
                    pass
                if resumed:
                    where = "saved position" if not fst.is_gz else "archive already done/old (skipped)"
                elif path.endswith(".gz"):
                    where = "full archive (once)"
                else:
                    where = "start" if from_start else (
                        "last %dKB" % (WATCH_CATCHUP_BYTES // 1024) if WATCH_CATCHUP_BYTES else "end")
                print("[INFO] Watching %s (host=%s channel=%s from=%s)" % (
                    path, host, channel, where))
        for key in list(states):
            if key not in alive:
                print("[INFO] Stopped watching missing file: %s" % states[key].path)
                del states[key]
        return len(states)

    try:
        while True:
            try:
                now = time.time()
                if now - last_discover >= WATCH_DISCOVER_SEC or not states:
                    refresh()
                    last_discover = now
                for st in list(states.values()):
                    try:
                        # blocking file/gzip reads go to a thread so the web UI stays responsive
                        lines = await loop.run_in_executor(None, st.read_new_lines)
                    except Exception as e:
                        print("[WARN] read %s: %s" % (st.path, e), file=sys.stderr)
                        continue
                    for bl in lines:
                        try:
                            hub.ingest(bl, st.channel, st.host, st.conn, copy_raw=False,
                                       fmt_mode=fmt_mode, srcfile=st.path)
                        except Exception as e:   # one bad line must never stop the watcher
                            print("[WARN] bad line from %s skipped: %s" % (st.path, e), file=sys.stderr)
                if time.time() - last_save >= 5.0:
                    last_save = time.time()
                    _save_state(states)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print("[ERROR] file watch cycle failed, continuing: %r" % (e,), file=sys.stderr)
            await asyncio.sleep(WATCH_POLL_SEC)
    except asyncio.CancelledError:
        _save_state(states)
        raise


# ==============================================================================
# Offline merge:  --merge a.log b.log -o merged.log
# ==============================================================================
def _iter_file_events(path, idx, fmt_mode=None):
    opener = gzip.open if path.endswith(".gz") else open
    stem = os.path.basename(path)
    channel = _channel_from_name(stem)
    for suffix in (".gz", ".log", ".txt", ".jsonl", ".out"):
        if stem.endswith(suffix):
            stem = stem[:-len(suffix)]
    prev = None
    with opener(path, "rt", encoding="utf-8", errors="replace") as f:
        for n, line in enumerate(f):
            line = line.rstrip("\r\n")
            if not line.strip():
                continue
            ev = parse_record(line, channel, stem, prev,
                              arrival=prev["ts"] if prev else 0.0, clamp=False, fmt_mode=fmt_mode)
            prev = ev
            yield (ev["ts"], idx, n, ev)


def run_merge(paths, out_path, fmt, structure="nested", fmt_mode="mixed"):
    files = expand_merge_inputs(paths, structure)
    if not files:
        print("No input files to merge", file=sys.stderr)
        return 2
    count = 0
    with open(out_path, "w", encoding="utf-8", newline="") as fo:
        writer = None
        if fmt == "csv":
            writer = csv.writer(fo)
            writer.writerow(CSV_COLS)
        # each server's own file is already time-ordered, so a k-way merge is enough (and streams)
        for _ts, _idx, _n, ev in heapq.merge(
                *[_iter_file_events(p, i, fmt_mode=fmt_mode) for i, p in enumerate(files)]):
            count += 1
            ev["id"] = count
            fo.write(format_event(ev, fmt, writer))
    print("Merged %d lines from %d files into %s" % (count, len(files), out_path))
    return 0


# ==============================================================================
# Main application lifecycle
# ==============================================================================
async def supervise(name, factory):
    """Restart a background task if it ever dies, and say so in the log."""
    while True:
        try:
            await factory()
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("[ERROR] %s crashed (%r); restarting in 5s" % (name, e), file=sys.stderr)
            await asyncio.sleep(5)


async def heartbeat_loop():
    while True:
        await asyncio.sleep(HEARTBEAT_SEC)
        now = time.time()
        w = hub.written.get(CONSOLIDATED_LOG)
        print("[INFO] heartbeat: received=%d consolidated=%d duplicates_dropped=%d pending=%d "
              "dropped=%d last_write=%s" % (
                  hub.total_in, hub.total_out, hub.dup_dropped, len(hub.pending), hub.dropped,
                  ("%ds ago" % (now - w)) if w else "never"))


def _warn_if_fuse(path):
    """Appending to a log on s3fs rewrites the whole object each time: slow and often stalls."""
    try:
        real = os.path.realpath(path)
        best, fstype = "", ""
        with open("/proc/mounts", "r") as f:
            for ln in f:
                p = ln.split()
                if len(p) >= 3 and (real == p[1] or real.startswith(p[1].rstrip("/") + "/")):
                    if len(p[1]) > len(best):
                        best, fstype = p[1], p[2]
        if fstype.startswith("fuse"):
            print("[WARN] Output directory %s is on a FUSE/s3fs mount (%s). Appends there are slow and "
                  "can stall, so consolidated.log may stop updating. Use a local folder such as "
                  "/var/log/aiv-consolidated (--log-dir)." % (path, best), file=sys.stderr)
    except OSError:
        pass


async def main(demo=False, watch_dir=None, structure="nested", fmt_mode="mixed",
               from_start=False):
    global hub
    try:
        sys.stdout.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass
    os.makedirs(os.path.join(LOG_OUTPUT_DIR, "archived"), exist_ok=True)
    _warn_if_fuse(LOG_OUTPUT_DIR)
    hub = Hub()
    hub.load_history()

    print("=" * 66)
    print("  AIV Cluster Log Consolidator v3.1")
    print("  Join keys          : timestamp + trace + user + run (cross-host enrich)")
    print("  Output directory   : %s" % os.path.abspath(LOG_OUTPUT_DIR))
    print("  Consolidated log   : %s (+ %s)" % (CONSOLIDATED_LOG, CONSOLIDATED_JSONL))
    print("  Plain log stream   : TCP %d" % PLAIN_PORT)
    print("  JSON log stream    : TCP %d" % JSON_PORT)
    print("  Web dashboard      : http://%s:%d" % ("localhost" if HTTP_BIND_ADDR == "0.0.0.0" else HTTP_BIND_ADDR, HTTP_PORT))
    print("  Ordering           : idle %.1fs, max hold %.1fs" % (REORDER_IDLE_SEC, REORDER_MAX_HOLD_SEC))
    print("  Duplicate filter   : %s (same event from all-plain + channel files is kept once)" % (
        "on" if DEDUP_ACROSS_FILES else "off"))
    if watch_dir:
        print("  File watch         : %s" % os.path.abspath(watch_dir))
        print("  Watch structure    : %s" % structure)
        print("  Watch format       : %s" % fmt_mode)
        print("  Watch mode         : continuous (from %s)" % ("start" if from_start else "end"))
    if demo:
        print("  DEMO MODE          : simulated traffic from web01, app01, db01, auth01")
    print("=" * 66)

    writer_tasks = [asyncio.ensure_future(file_writer_task(n, q)) for n, q in hub.queues.items()]
    bg_tasks = [asyncio.ensure_future(supervise("release loop", hub.release_loop)),
                asyncio.ensure_future(supervise("daily rotation", daily_rotation_task)),
                asyncio.ensure_future(heartbeat_loop())]

    plain_srv = await asyncio.start_server(handle_plain_connection, BIND_ADDR, PLAIN_PORT)
    json_srv = await asyncio.start_server(handle_json_connection, BIND_ADDR, JSON_PORT)
    http_srv = await asyncio.start_server(handle_http_request, HTTP_BIND_ADDR, HTTP_PORT)
    if demo:
        bg_tasks.append(asyncio.ensure_future(demo_traffic()))
    if watch_dir:
        bg_tasks.append(asyncio.ensure_future(supervise(
            "file watch",
            lambda: file_watch_loop(watch_dir, structure=structure, fmt_mode=fmt_mode,
                                    from_start=from_start))))

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        pass

    print("\n[INFO] Graceful shutdown initiated...")
    for srv in (plain_srv, json_srv, http_srv):
        srv.close()
    for t in bg_tasks:
        t.cancel()
    for t in list(hub.conn_tasks):
        t.cancel()
    await asyncio.gather(*bg_tasks, *list(hub.conn_tasks), return_exceptions=True)
    hub.release(force=True)  # flush everything still waiting for ordering
    try:
        await asyncio.wait_for(asyncio.gather(*[q.join() for q in hub.queues.values()]), 10)
    except asyncio.TimeoutError:
        print("[WARN] Some buffers did not flush in time", file=sys.stderr)
    for t in writer_tasks:
        t.cancel()
    await asyncio.gather(*writer_tasks, return_exceptions=True)
    print("[INFO] All buffers flushed. Log Consolidator stopped cleanly.")


def cli():
    global LOG_OUTPUT_DIR, LOG_INPUT_DIR, WATCH_STRUCTURE, WATCH_FORMAT, WATCH_MODE, WATCH_FROM_START
    ap = argparse.ArgumentParser(description="AIV Cluster Log Consolidator")
    ap.add_argument("--log-dir", help="output directory (default: $LOG_OUTPUT_DIR, "
                    "/var/log/aiv-consolidated if it exists, else ./logs)")
    ap.add_argument("--watch", metavar="DIR", help="continuously tail log files under DIR (default: $LOG_INPUT_DIR, "
                    "or /var/lib/aiv/logs if it exists)")
    ap.add_argument("--input-dir", metavar="DIR", help="alias for --watch / $LOG_INPUT_DIR")
    ap.add_argument("--structure", choices=["nested", "flat"], default=None,
                    help="log folder layout (default: nested)")
    ap.add_argument("--log-format", choices=["mixed", "json", "plain"], default=None,
                    help="line format for file watch / merge (default: mixed)")
    ap.add_argument("--mode", choices=["continuous", "once"], default=None,
                    help="file processing mode (default: continuous)")
    ap.add_argument("--from-start", action="store_true",
                    help="when watching continuously, also read existing file content first")
    ap.add_argument("--demo", action="store_true", help="run with simulated multi-server traffic")
    ap.add_argument("--merge", nargs="+", metavar="FILE",
                    help="merge existing log files/dirs by timestamp and exit")
    ap.add_argument("-o", "--output", default="merged.log", help="output file for --merge")
    ap.add_argument("--format", choices=["log", "jsonl", "csv"], default="log", help="format for --merge")
    args = ap.parse_args()
    if args.log_dir:
        LOG_OUTPUT_DIR = args.log_dir
    if args.structure:
        WATCH_STRUCTURE = args.structure
    if args.log_format:
        WATCH_FORMAT = args.log_format
    if args.mode:
        WATCH_MODE = args.mode
    if args.from_start:
        WATCH_FROM_START = True

    structure = WATCH_STRUCTURE if WATCH_STRUCTURE in ("nested", "flat") else "nested"
    fmt_mode = WATCH_FORMAT if WATCH_FORMAT in ("mixed", "json", "plain") else "mixed"
    mode = WATCH_MODE if WATCH_MODE in ("continuous", "once") else "continuous"
    watch_dir = args.watch or args.input_dir or LOG_INPUT_DIR or None

    if args.merge:
        sys.exit(run_merge(args.merge, args.output, args.format, structure=structure, fmt_mode=fmt_mode))

    if watch_dir and mode == "once":
        # one-shot: read nested tree into consolidated outputs, then exit (TCP/UI still optional via continuous)
        async def _once():
            global hub
            os.makedirs(os.path.join(LOG_OUTPUT_DIR, "archived"), exist_ok=True)
            hub = Hub()
            writer_tasks = [asyncio.ensure_future(file_writer_task(n, q)) for n, q in hub.queues.items()]
            bg = asyncio.ensure_future(hub.release_loop())
            paths = discover_log_files(watch_dir, structure)
            if not paths:
                print("No log files under %s" % watch_dir, file=sys.stderr)
                return 1
            count = 0
            for path in paths:
                host, channel = _meta_for_path(path, os.path.abspath(watch_dir), structure)
                conn = Conn()
                opener = gzip.open if path.endswith(".gz") else open
                try:
                    with opener(path, "rt", encoding="utf-8", errors="replace") as f:
                        for line in f:
                            line = line.rstrip("\r\n")
                            if not line.strip():
                                continue
                            hub.ingest(line.encode("utf-8"), channel, host, conn,
                                       copy_raw=False, fmt_mode=fmt_mode)
                            count += 1
                except OSError as e:
                    print("[WARN] %s: %s" % (path, e), file=sys.stderr)
            hub.release(force=True)
            bg.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(*[q.join() for q in hub.queues.values()]), 30)
            except asyncio.TimeoutError:
                print("[WARN] Some buffers did not flush in time", file=sys.stderr)
            for t in writer_tasks:
                t.cancel()
            await asyncio.gather(bg, *writer_tasks, return_exceptions=True)
            print("Ingested %d lines from %d files under %s" % (count, len(paths), watch_dir))
            return 0
        try:
            sys.exit(asyncio.run(_once()))
        except KeyboardInterrupt:
            sys.exit(130)

    try:
        asyncio.run(main(demo=args.demo, watch_dir=watch_dir, structure=structure,
                         fmt_mode=fmt_mode, from_start=WATCH_FROM_START))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()