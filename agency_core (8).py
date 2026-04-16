"""
agency_core.py
==============
Agency: NoError
Contains: Manager + Analyzer + Researcher
LangGraph powered — PostgreSQL checkpoints + NOTIFY event system

FIXES APPLIED:
  1. Manager sirf NOTIFY pe wake up — schedule polling removed
  2. Gmail optional — CEO "gmail on"/"gmail off" se control
  3. OpenAI sirf zarurat pe — rule-based filter pehle
  4. Startup message DB mein store — restart pe dobara nahi aata
  5. Schema: task_path + manager_contacted columns added
  6. Browserless + Tor removed — replaced with requests + BeautifulSoup
  7. [NEW] WhatsApp dedup — whatsapp_conv_state tracks last processed msg ID
  8. [NEW] Manager only processes NEW/unread messages, history as context only
  9. [NEW] Researcher heartbeat health check — CEO alerted if silent
 10. [NEW] Unknown WhatsApp numbers get generic LLM reply
 11. [NEW] ALL messages routed through fine-tuned LLM model
"""

import os
import json
import time
import select
import random
import imaplib
import email
import smtplib
import logging
import threading
import schedule
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, timedelta
from typing import TypedDict, Annotated, Optional, Literal
from concurrent.futures import ThreadPoolExecutor, as_completed
from db_pool import wait_for_db, close_pool, db_fetch, db_execute,db_notify, health_check
import operator

import psycopg2
import psycopg2.extensions
import psycopg2.pool
import requests
from bs4 import BeautifulSoup
import redis
from github import Github

from openai import OpenAI, APIError, APITimeoutError, RateLimitError
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
)
log = logging.getLogger("noerror-core")

openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# ============================================================
# CONFIG & SECRETS
# ============================================================

AGENCY_NAME          = "NoError"
OPENAI_MODEL         = "ft:gpt-4o-mini-2024-07-18:personal:trained-agency-manager:DOLBnRws"
DATABASE_URL         = os.environ["DATABASE_URL"]

EVOLUTION_URL        = os.environ.get("EVOLUTION_URL", "")
EVOLUTION_API_KEY    = os.environ.get("EVOLUTION_API_KEY", "")
EVOLUTION_INSTANCE   = os.environ.get("EVOLUTION_INSTANCE", "whatsapp")

CEO_WHATSAPP         = os.environ["CEO_WHATSAPP_NUMBER"]
GMAIL_USER           = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD   = os.environ["GMAIL_APP_PASSWORD"]
GOOGLE_SHEET_URL     = os.environ["GOOGLE_SHEET_URL"]
GITHUB_TOKEN         = os.environ["GITHUB_TOKEN"]
GITHUB_REPO          = os.environ["GITHUB_REPO"]

REDIS_URL            = os.environ.get("REDIS_URL", "redis://localhost:6379")
RESEARCH_CACHE_TTL   = 20 * 60 * 60

# Researcher heartbeat timeout (minutes) — alert CEO if silent longer than this
RESEARCHER_HB_TIMEOUT_MIN = 65

RESEARCH_KEYWORDS = [
    "recent launches of ai", "agentic orchestration",
    "lead generation AI", "best ai frameworks",
    "ai recent updates", "solutions for devlopers",
]

# ── Gmail relevant senders/subjects for rule-based filter ───
GMAIL_IMPORTANT_SUBJECTS = ["payment", "invoice", "urgent", "contract", "project", "client"]
GMAIL_IMPORTANT_SENDERS  = []  # add client emails here e.g. ["client@example.com"]

# ── Human-like request headers ───────────────────────────────
_SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def _human_delay():
    """Random human-like delay between requests."""
    time.sleep(random.uniform(2.5, 6.0))

# ============================================================
# SCHEMA
# ============================================================

SCHEMA_SQL = """
-- LangGraph checkpoint tables
CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id            TEXT        NOT NULL,
    checkpoint_ns        TEXT        NOT NULL DEFAULT '',
    checkpoint_id        TEXT        NOT NULL,
    parent_checkpoint_id TEXT,
    type                 TEXT,
    checkpoint           JSONB,
    metadata             JSONB,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);

ALTER TABLE leads ADD COLUMN IF NOT EXISTS manager_contacted BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS incoming_messages(
    id            SERIAL PRIMARY KEY,
    jid           TEXT NOT NULL,
    message_id    TEXT NOT NULL,
    body          TEXT,
    from_me       BOOLEAN DEFAULT FALSE,
    timestamp     BIGINT,
    is_read       BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(message_id)
);

CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id     TEXT   NOT NULL,
    checkpoint_ns TEXT   NOT NULL DEFAULT,
    channel       TEXT   NOT NULL,
    version       TEXT   NOT NULL,
    type          TEXT   NOT NULL,
    blob          BYTEA,
    PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
);

CREATE TABLE IF NOT EXISTS checkpoint_migrations (
    v INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id     TEXT   NOT NULL,
    checkpoint_ns TEXT   NOT NULL DEFAULT '',
    checkpoint_id TEXT   NOT NULL,
    task_id       TEXT   NOT NULL,
    idx           SERIAL NOT NULL,
    channel       TEXT   NOT NULL,
    type          TEXT,
    blob          BYTEA  NOT NULL,
    task_path     TEXT,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
);

-- Agency tables
CREATE TABLE IF NOT EXISTS leads (
    lead_id           TEXT PRIMARY KEY,
    name              TEXT,
    platform          TEXT,
    status            TEXT DEFAULT 'new',
    whatsapp          TEXT,
    watcher_notes     TEXT,
    manager_contacted BOOLEAN DEFAULT FALSE,
    last_updated      TIMESTAMPTZ DEFAULT NOW(),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS watcher_conversations (
    lead_id   TEXT PRIMARY KEY,
    messages  JSONB DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS oos_temp (
    lead_id      TEXT PRIMARY KEY,
    platform     TEXT,
    post_url     TEXT,
    post_content TEXT,
    username     TEXT,
    source_type  TEXT,
    status       TEXT DEFAULT 'pending_analyzer',
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_messages (
    id            SERIAL PRIMARY KEY,
    from_agent    TEXT,
    to_agent      TEXT,
    message_type  TEXT,
    payload       JSONB,
    related_id    TEXT,
    related_type  TEXT,
    is_read       BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_errors (
    id                  SERIAL PRIMARY KEY,
    agent_name          TEXT,
    error_type          TEXT,
    error_message       TEXT,
    self_heal_attempts  INT DEFAULT 0,
    status              TEXT DEFAULT 'open',
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS projects (
    project_id    TEXT PRIMARY KEY,
    executor_type TEXT,
    status        TEXT DEFAULT 'pending_ceo',
    client        TEXT,
    deploy_note   TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- FIX 4: Startup message flag table
CREATE TABLE IF NOT EXISTS system_flags (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- FIX 2: Gmail toggle flag table
CREATE TABLE IF NOT EXISTS agent_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Researcher heartbeat tracking
CREATE TABLE IF NOT EXISTS researcher_heartbeats (
    id         SERIAL PRIMARY KEY,
    sent_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Migration: add task_path if old table exists without it
ALTER TABLE checkpoint_writes ADD COLUMN IF NOT EXISTS task_path TEXT;

-- Migration: add manager_contacted if old table exists without it
ALTER TABLE leads ADD COLUMN IF NOT EXISTS manager_contacted BOOLEAN DEFAULT FALSE;
"""

# ============================================================
# LANGGRAPH STATE SCHEMAS
# ============================================================

class ManagerState(TypedDict):
    event_type:    str
    event_payload: dict
    actions_taken: Annotated[list, operator.add]
    errors:        Annotated[list, operator.add]
    done:          bool


class AnalyzerState(TypedDict):
    oos_id:        str
    oos_data:      dict
    decision:      str
    reason:        str
    actions_taken: Annotated[list, operator.add]
    errors:        Annotated[list, operator.add]
    done:          bool


class ResearcherState(TypedDict):
    query:           str
    platform:        str
    results:         list
    summary:         str
    youtube_enabled: bool
    errors:          Annotated[list, operator.add]
    done:            bool


# ── FIX 2: Gmail on/off from DB ─────────────────────────────

def get_gmail_enabled() -> bool:
    rows = db_fetch("SELECT value FROM agent_settings WHERE key='gmail_enabled'")
    if not rows:
        return False
    return rows[0][0].lower() == "true"


def set_gmail_enabled(enabled: bool):
    db_execute("""
        INSERT INTO agent_settings (key, value, updated_at)
        VALUES ('gmail_enabled', %s, NOW())
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
    """, ("true" if enabled else "false",))


# ── FIX 4: Startup message DB flag ──────────────────────────

def get_startup_sent() -> bool:
    rows = db_fetch("SELECT value FROM system_flags WHERE key='startup_msg_sent'")
    if not rows:
        return False
    return rows[0][0].lower() == "true"


def set_startup_sent():
    db_execute("""
        INSERT INTO system_flags (key, value, updated_at)
        VALUES ('startup_msg_sent', 'true', NOW())
        ON CONFLICT (key) DO UPDATE SET value='true', updated_at=NOW()
    """)


# ── FIX 7: WhatsApp dedup helpers ───────────────────────────

def wa_get_last_processed_id(jid: str) -> str | None:
    """Return the last message ID we already replied to for this JID."""
    rows = db_fetch(
        "SELECT last_processed_msg_id FROM whatsapp_conv_state WHERE jid=%s",
        (jid,)
    )
    return rows[0][0] if rows else None


def wa_set_last_processed_id(jid: str, msg_id: str):
    """Persist the last processed message ID for this JID."""
    db_execute("""
        INSERT INTO whatsapp_conv_state (jid, last_processed_msg_id, last_updated)
        VALUES (%s, %s, NOW())
        ON CONFLICT (jid) DO UPDATE
            SET last_processed_msg_id=EXCLUDED.last_processed_msg_id,
                last_updated=NOW()
    """, (jid, msg_id))


def get_unread_agent_messages(to_agent: str) -> list[dict]:
    rows = db_fetch("""
        SELECT id, from_agent, message_type, payload, related_id, related_type
        FROM agent_messages
        WHERE to_agent=%s AND is_read=false
        ORDER BY created_at ASC
    """, (to_agent,))
    results = []
    for row in rows:
        results.append({
            "id": row[0], "from_agent": row[1],
            "message_type": row[2],
            "payload": row[3] if isinstance(row[3], dict) else json.loads(row[3] or "{}"),
            "related_id": row[4], "related_type": row[5],
        })
    return results


def mark_agent_message_read(msg_id: int):
    db_execute("UPDATE agent_messages SET is_read=true WHERE id=%s", (msg_id,))


def send_agent_message(from_agent, to_agent, message_type, payload,
                       related_id="", related_type="system"):
    db_execute("""
        INSERT INTO agent_messages
            (from_agent, to_agent, message_type, payload, related_id, related_type)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (from_agent, to_agent, message_type,
          json.dumps(payload), related_id, related_type))
    db_notify("agent_channel", to_agent)


def get_lead(lead_id: str) -> dict | None:
    rows = db_fetch(
        "SELECT lead_id, name, platform, status, watcher_notes FROM leads WHERE lead_id=%s",
        (lead_id,)
    )
    if not rows:
        return None
    r = rows[0]
    return {"lead_id": r[0], "name": r[1], "platform": r[2],
            "status": r[3], "watcher_notes": r[4]}


def get_conversation(lead_id: str) -> list:
    rows = db_fetch(
        "SELECT messages FROM watcher_conversations WHERE lead_id=%s", (lead_id,)
    )
    if not rows:
        return []
    msgs = rows[0][0]
    return msgs if isinstance(msgs, list) else json.loads(msgs or "[]")


def update_lead_status(lead_id: str, status: str):
    db_execute(
        "UPDATE leads SET status=%s, last_updated=NOW() WHERE lead_id=%s",
        (status, lead_id)
    )


def get_new_leads() -> list[dict]:
    rows = db_fetch("""
        SELECT l.lead_id, l.name, l.platform, l.watcher_notes, wc.messages
        FROM leads l
        LEFT JOIN watcher_conversations wc ON l.lead_id = wc.lead_id
        WHERE l.status = 'discovery_in_progress'
          AND l.whatsapp IS NOT NULL
          AND l.manager_contacted = false
        ORDER BY l.created_at ASC
        LIMIT 10
    """)
    results = []
    for r in rows:
        msgs = r[4] if isinstance(r[4], list) else json.loads(r[4] or "[]")
        results.append({
            "lead_id": r[0], "name": r[1], "platform": r[2],
            "watcher_notes": r[3], "conversation": msgs,
        })
    return results


def get_pending_oos() -> list[dict]:
    rows = db_fetch("""
        SELECT lead_id, platform, post_url, post_content, username, source_type
        FROM oos_temp
        WHERE status='pending_analyzer'
        ORDER BY created_at ASC
        LIMIT 20
    """)
    return [
        {"lead_id": r[0], "platform": r[1], "post_url": r[2],
         "post_content": r[3], "username": r[4], "source_type": r[5]}
        for r in rows
    ]


def update_oos_status(lead_id: str, status: str):
    db_execute("UPDATE oos_temp SET status=%s WHERE lead_id=%s", (status, lead_id))


def get_payment_sheet_data() -> list[dict]:
    try:
        import csv, io
        sheet_id = GOOGLE_SHEET_URL.split("/d/")[1].split("/")[0]
        csv_url  = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        resp     = requests.get(csv_url, timeout=15)
        resp.raise_for_status()
        reader  = csv.DictReader(io.StringIO(resp.text))
        return [{k.strip().lower().replace(" ", "_"): v.strip()
                 for k, v in row.items()} for row in reader]
    except Exception as exc:
        log.error(f"Google Sheet read error: {exc}")
        return []


def log_agent_error(agent_name, error_type, error_message):
    db_execute("""
        INSERT INTO agent_errors
            (agent_name, error_type, error_message, self_heal_attempts, status)
        VALUES (%s,%s,%s,0,'open')
    """, (agent_name, error_type, error_message))


def get_researcher_youtube_enabled() -> bool:
    rows = db_fetch("""
        SELECT payload FROM agent_messages
        WHERE to_agent='researcher' AND message_type='command'
        ORDER BY created_at DESC LIMIT 1
    """)
    if not rows:
        return False
    try:
        payload = rows[0][0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return (payload or {}).get("youtube_enabled", False)
    except Exception:
        return False


# ============================================================
# LLM UTILITIES
# ============================================================

def call_llm(messages: list, max_tokens: int = 500) -> str | None:
    for attempt in range(3):
        try:
            resp = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=messages,
                temperature=0.7,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content.strip()
        except (APITimeoutError, RateLimitError) as exc:
            wait = (2 ** attempt) + random.uniform(0, 1)
            log.warning(f"LLM retry {attempt+1}/3 in {wait:.1f}s: {exc}")
            time.sleep(wait)
        except APIError as exc:
            log.error(f"LLM API error: {exc}")
            return None
        except Exception as exc:
            log.error(f"LLM unexpected: {exc}")
            return None
    return None


# ============================================================
# EVOLUTION API — WhatsApp Utilities
# ============================================================

def _evo_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "apikey": EVOLUTION_API_KEY,
    }


def _clean_jid(number: str) -> str:
    clean = number.replace("+", "").replace("-", "").replace(" ", "")
    return clean if clean.endswith("@s.whatsapp.net") else f"{clean}@s.whatsapp.net"


def wpp_send(number: str, message: str) -> bool:
    jid = _clean_jid(number)
    url = f"{EVOLUTION_URL}/message/sendText/{EVOLUTION_INSTANCE}"
    payload = {"number": jid, "text": message}
    try:
        resp = requests.post(url, json=payload, headers=_evo_headers(), timeout=30)
        resp.raise_for_status()
        return True
    except Exception as exc:
        log.error(f"[Evolution] Send error to {number}: {exc}")
        return False


def wpp_get_new_messages(jid: str, limit: int = 30) -> list[dict]:
    """
    FIX 8: Fetch messages for a JID, return ONLY new/unread ones
    after the last processed message ID stored in DB.
    Full history is returned separately as context only.
    """
    url = f"{EVOLUTION_URL}/chat/findMessages/{EVOLUTION_INSTANCE}"
    payload = {"where": {"key": {"remoteJid": jid}}, "limit": limit}
    try:
        resp = requests.post(url, json=payload, headers=_evo_headers(), timeout=15)
        resp.raise_for_status()
        data    = resp.json()
        records = data if isinstance(data, list) else data.get("messages", {}).get("records", [])
    except Exception as exc:
        log.error(f"[Evolution] Get messages error for {jid}: {exc}")
        return new_msgs or []

    all_msgs = []
    for m in records:
        mc   = m.get("message", {})
        text = (
            mc.get("conversation") or
            mc.get("extendedTextMessage", {}).get("text") or ""
        )
        all_msgs.append({
            "body":   text,
            "jid":    jid,
            "fromMe": m.get("key", {}).get("fromMe", False),
            "id":     m.get("key", {}).get("id", ""),
            "ts":     m.get("messageTimestamp", 0),
        })

    # Sort oldest → newest
    all_msgs.sort(key=lambda x: x["ts"])

    last_id = wa_get_last_processed_id(jid)

    if last_id is None:
        # First time seeing this JID — only return the very last incoming message
        incoming = [m for m in all_msgs if not m["fromMe"] and m["body"]]
        return incoming[-1:] if incoming else []

    # Return only messages AFTER the last processed ID
    found     = False
    new_msgs  = []
    for m in all_msgs:
        if found and not m["fromMe"] and m["body"]:
            new_msgs.append(m)
        if m["id"] == last_id:
            found = True

    if not found:
        # last_id no longer in window — return only latest incoming
        incoming = [m for m in all_msgs if not m["fromMe"] and m["body"]]
        return incoming[-1:] if incoming else []

    return new_msgs or []


def wpp_get_history_context(jid: str, limit: int = 6) -> list[dict]:
    """
    FIX 8: Return last N messages as context for LLM — NOT for processing.
    """
    url = f"{EVOLUTION_URL}/chat/findMessages/{EVOLUTION_INSTANCE}"
    payload = {"where": {"key": {"remoteJid": jid}}, "limit": 30}
    try:
        resp = requests.post(url, json=payload, headers=_evo_headers(), timeout=15)
        resp.raise_for_status()
        data    = resp.json()
        records = data if isinstance(data, list) else data.get("messages", {}).get("records", [])
    except Exception as exc:
        log.error(f"[Evolution] History fetch error for {jid}: {exc}")
        return []

    all_msgs = []
    for m in records:
        mc   = m.get("message", {})
        text = (
            mc.get("conversation") or
            mc.get("extendedTextMessage", {}).get("text") or ""
        )
        if text:
            all_msgs.append({
                "role":   "me" if m.get("key", {}).get("fromMe") else "them",
                "body":   text,
                "ts":     m.get("messageTimestamp", 0),
            })

    all_msgs.sort(key=lambda x: x["ts"])
    return all_msgs[-limit:]


def wpp_get_active_jids(limit: int = 50) -> list[str]:
    """Return all JIDs that have sent us messages recently."""
    url = f"{EVOLUTION_URL}/chat/findMessages/{EVOLUTION_INSTANCE}"
    payload = {"limit": limit}
    try:
        resp = requests.post(url, json=payload, headers=_evo_headers(), timeout=15)
        resp.raise_for_status()
        data    = resp.json()
        records = data if isinstance(data, list) else data.get("messages", {}).get("records", [])
        jids = set()
        for m in records:
            jid     = m.get("key", {}).get("remoteJid", "")
            from_me = m.get("key", {}).get("fromMe", True)
            if jid and not from_me:
                jids.add(jid)
        return list(jids)
    except Exception as exc:
        log.error(f"[Evolution] Get active JIDs error: {exc}")
        return []


def wpp_status() -> bool:
    try:
        url  = f"{EVOLUTION_URL}/instance/connectionState/{EVOLUTION_INSTANCE}"
        resp = requests.get(url, headers=_evo_headers(), timeout=8)
        data = resp.json()
        state = data.get("instance", {}).get("state", "") or data.get("state", "")
        return state == "open"
    except Exception:
        return False


# ============================================================
# FIX 2 — GMAIL: rule-based filter + optional
# ============================================================

def gmail_get_unread(limit: int = 10) -> list[dict]:
    results = []
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        mail.select("inbox")
        _, data = mail.search(None, "UNSEEN")
        ids = data[0].split()[-limit:]
        for eid in ids:
            _, msg_data = mail.fetch(eid, "(RFC822)")
            msg     = email.message_from_bytes(msg_data[0][1])
            subject = msg.get("Subject", "")
            sender  = msg.get("From", "")
            body    = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        break
            else:
                body = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
            results.append({
                "id": eid.decode(), "subject": subject,
                "sender": sender, "body": body[:2000],
            })
        mail.logout()
    except Exception as exc:
        log.error(f"Gmail IMAP error: {exc}")
    return results


def _is_important_email(em: dict) -> bool:
    subject = em.get("subject", "").lower()
    sender  = em.get("sender", "").lower()
    if any(kw in subject for kw in GMAIL_IMPORTANT_SUBJECTS):
        return True
    if GMAIL_IMPORTANT_SENDERS:
        if any(s in sender for s in GMAIL_IMPORTANT_SENDERS):
            return True
    return False


def gmail_send(to: str, subject: str, body: str) -> bool:
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = GMAIL_USER
        msg["To"]      = to
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, to, msg.as_string())
        return True
    except Exception as exc:
        log.error(f"Gmail SMTP send error: {exc}")
        return False


def gmail_reply(to: str, subject: str, original_body: str) -> bool:
    reply = call_llm([{
        "role": "user",
        "content": (
            f"Email from: {to}\nSubject: {subject}\n"
            f"Their message:\n{original_body}\n\n"
            f"You are Manager of {AGENCY_NAME} AI automation agency. "
            "Generate a professional reply email. Plain text only."
        )
    }], max_tokens=400)
    if not reply:
        return False
    return gmail_send(to, f"Re: {subject}", reply)


# ============================================================
# GITHUB UTILITIES
# ============================================================

def github_push_file(repo_path: str, file_path: str,
                     content: str, commit_msg: str) -> bool:
    try:
        gh   = Github(GITHUB_TOKEN)
        repo = gh.get_repo(GITHUB_REPO)
        try:
            existing = repo.get_contents(file_path)
            repo.update_file(file_path, commit_msg, content, existing.sha)
        except Exception:
            repo.create_file(file_path, commit_msg, content)
        log.info(f"[GitHub] Pushed: {file_path}")
        return True
    except Exception as exc:
        log.error(f"GitHub push error: {exc}")
        return False


def github_get_file(file_path: str) -> str | None:
    try:
        gh      = Github(GITHUB_TOKEN)
        repo    = gh.get_repo(GITHUB_REPO)
        content = repo.get_contents(file_path)
        return content.decoded_content.decode("utf-8")
    except Exception as exc:
        log.error(f"GitHub read error: {exc}")
        return None


# ============================================================
# REDIS — Researcher cache
# ============================================================

_redis_client: redis.Redis | None = None
_redis_lock = threading.Lock()


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        with _redis_lock:
            if _redis_client is None:
                _redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


def research_cache_set(platform: str, query: str, results: list):
    try:
        key = f"research:{platform}:{hash(query) & 0xFFFFFFFF}"
        _get_redis().setex(key, RESEARCH_CACHE_TTL, json.dumps(results))
    except Exception as exc:
        log.warning(f"[Redis] Cache set failed: {exc}")


def research_cache_get(platform: str, query: str) -> list | None:
    try:
        key = f"research:{platform}:{hash(query) & 0xFFFFFFFF}"
        raw = _get_redis().get(key)
        return json.loads(raw) if raw else None
    except Exception as exc:
        log.warning(f"[Redis] Cache get failed: {exc}")
        return None


def research_collect_all_cached(platform: str) -> list:
    try:
        r    = _get_redis()
        keys = r.keys(f"research:{platform}:*")
        all_results = []
        for key in keys:
            raw = r.get(key)
            if raw:
                try:
                    all_results.extend(json.loads(raw))
                except Exception:
                    pass
        return all_results
    except Exception as exc:
        log.warning(f"[Redis] Collect failed: {exc}")
        return []


# ============================================================
# FIX 6 — SCRAPING: requests + BeautifulSoup
# ============================================================

def _get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(_SCRAPE_HEADERS)
    return session


def playwright_scrape_twitter(query: str) -> list[dict]:
    cached = research_cache_get("twitter", query)
    if cached is not None:
        return cached
    results = []
    try:
        session = _get_session()
        _human_delay()
        url  = f"https://nitter.net/search?q={requests.utils.quote(query)}&f=tweets"
        resp = session.get(url, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select(".timeline-item")[:10]:
            try:
                text_el   = item.select_one(".tweet-content")
                author_el = item.select_one(".username")
                if text_el:
                    results.append({
                        "platform": "twitter",
                        "text":   text_el.get_text(strip=True),
                        "author": author_el.get_text(strip=True) if author_el else "unknown",
                        "query":  query,
                    })
            except Exception:
                continue
            _human_delay()
    except Exception as exc:
        log.error(f"[Researcher] Twitter scrape: {exc}")
    research_cache_set("twitter", query, results)
    return results


def playwright_scrape_reddit(query: str) -> list[dict]:
    cached = research_cache_get("reddit", query)
    if cached is not None:
        return cached
    results = []
    try:
        session = _get_session()
        _human_delay()
        url  = f"https://www.reddit.com/search.json?q={requests.utils.quote(query)}&sort=new&limit=10"
        resp = session.get(url, timeout=15)
        data = resp.json()
        posts = data.get("data", {}).get("children", [])
        for post in posts[:10]:
            d = post.get("data", {})
            results.append({
                "platform": "reddit",
                "text":   d.get("title", ""),
                "author": d.get("author", "reddit_post"),
                "query":  query,
            })
            _human_delay()
    except Exception as exc:
        log.error(f"[Researcher] Reddit scrape: {exc}")
    research_cache_set("reddit", query, results)
    return results


def playwright_scrape_google(query: str) -> list[dict]:
    cached = research_cache_get("google", query)
    if cached is not None:
        return cached
    results = []
    try:
        session = _get_session()
        _human_delay()
        url  = f"https://www.google.com/search?q={requests.utils.quote(query)}&tbs=qdr:d"
        resp = session.get(url, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("div.g")[:8]:
            try:
                title_el   = item.select_one("h3")
                snippet_el = item.select_one(".VwiC3b, .s3v9rd")
                link_el    = item.select_one("a")
                results.append({
                    "platform": "google",
                    "text": (
                        (title_el.get_text(strip=True) if title_el else "") + " — " +
                        (snippet_el.get_text(strip=True) if snippet_el else "")
                    ),
                    "url":   link_el.get("href", "") if link_el else "",
                    "query": query,
                })
            except Exception:
                continue
            _human_delay()
    except Exception as exc:
        log.error(f"[Researcher] Google scrape: {exc}")
    research_cache_set("google", query, results)
    return results


def playwright_scrape_linkedin(query: str) -> list[dict]:
    cached = research_cache_get("linkedin", query)
    if cached is not None:
        return cached
    results = []
    try:
        session = _get_session()
        _human_delay()
        url  = f"https://www.linkedin.com/search/results/content/?keywords={requests.utils.quote(query)}&sortBy=date_posted"
        resp = session.get(url, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("div.search-results__list li")[:10]:
            try:
                text_el = item.select_one("p, span.break-words")
                results.append({
                    "platform": "linkedin",
                    "text":   text_el.get_text(strip=True)[:500] if text_el else "",
                    "author": "linkedin_post",
                    "query":  query,
                })
            except Exception:
                continue
            _human_delay()
    except Exception as exc:
        log.error(f"[Researcher] LinkedIn scrape: {exc}")
    research_cache_set("linkedin", query, results)
    return results


def scrape_youtube_transcript(query: str) -> list[dict]:
    results = []
    try:
        import yt_dlp, whisper
        ydl_opts = {"quiet": True, "extract_flat": True, "default_search": "ytsearch5"}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info    = ydl.extract_info(f"ytsearch5:{query}", download=False)
            entries = info.get("entries", [])[:3]
        whisper_model = whisper.load_model("small")
        for entry in entries:
            url   = entry.get("url") or f"https://youtube.com/watch?v={entry.get('id','')}"
            title = entry.get("title", "")
            try:
                audio_opts = {
                    "format": "bestaudio/best",
                    "outtmpl": f"/tmp/yt_{entry.get('id','tmp')}.%(ext)s",
                    "quiet": True,
                    "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3"}],
                }
                with yt_dlp.YoutubeDL(audio_opts) as ydl:
                    ydl.download([url])
                audio_file = f"/tmp/yt_{entry.get('id','tmp')}.mp3"
                transcript = whisper_model.transcribe(audio_file)
                results.append({
                    "platform": "youtube", "title": title, "url": url,
                    "text": transcript["text"][:2000], "query": query,
                })
                try:
                    os.remove(audio_file)
                except Exception:
                    pass
            except Exception as exc:
                log.warning(f"YouTube transcript error for {url}: {exc}")
                results.append({
                    "platform": "youtube", "title": title, "url": url,
                    "text": title, "query": query,
                })
    except ImportError:
        log.error("YouTube deps not installed: pip install yt-dlp openai-whisper")
    except Exception as exc:
        log.error(f"YouTube scrape error: {exc}")
    return results


# ============================================================
# RESEARCHER HEARTBEAT — FIX 9
# ============================================================

def researcher_send_heartbeat():
    """Called by researcher loop every N minutes to signal it's alive."""
    db_execute("INSERT INTO researcher_heartbeats (sent_at) VALUES (NOW())")
    log.info(f"[{AGENCY_NAME} Researcher] ❤ Heartbeat sent")


def check_researcher_health():
    """
    FIX 9: Manager checks if researcher sent a heartbeat recently.
    If silent longer than RESEARCHER_HB_TIMEOUT_MIN, alerts CEO.
    """
    rows = db_fetch(
        "SELECT sent_at FROM researcher_heartbeats ORDER BY sent_at DESC LIMIT 1"
    )
    if not rows:
        wpp_send(CEO_WHATSAPP,
                 f"⚠️ [{AGENCY_NAME}] Researcher has never sent a heartbeat. "
                 "It may not be running.")
        return

    last_hb: datetime = rows[0][0]
    if last_hb.tzinfo is None:
        last_hb = last_hb.replace(tzinfo=timezone.utc)

    age_minutes = (datetime.now(timezone.utc) - last_hb).total_seconds() / 60
    if age_minutes > RESEARCHER_HB_TIMEOUT_MIN:
        wpp_send(CEO_WHATSAPP,
                 f"⚠️ [{AGENCY_NAME}] Researcher silent for "
                 f"{int(age_minutes)} minutes. Please check.")
        log.warning(f"[{AGENCY_NAME} Manager] Researcher silent for {age_minutes:.0f} min")
    else:
        log.info(f"[{AGENCY_NAME} Manager] Researcher healthy — last HB {age_minutes:.0f} min ago")


# ============================================================
# MANAGER NODES
# ============================================================

def manager_listen_node(state: ManagerState) -> ManagerState:
    log.info(f"[{AGENCY_NAME} Manager] Checking events...")

    msgs = get_unread_agent_messages("manager")
    if msgs:
        msg = msgs[0]
        mark_agent_message_read(msg["id"])
        return {**state, "event_type": "agent_msg", "event_payload": msg}

    new_leads = get_new_leads()
    if new_leads:
        return {**state, "event_type": "new_lead", "event_payload": {"leads": new_leads}}

    # Check DB for unread WhatsApp messages
    unread_count = db_fetch("SELECT COUNT(*) FROM incoming_messages WHERE is_read=false")
    if unread_count and unread_count[0][0] > 0:
        return {**state, "event_type": "whatsapp_msg", "event_payload": {}}

    if get_gmail_enabled():
        emails = gmail_get_unread(limit=5)
        important = [e for e in emails if _is_important_email(e)]
        if important:
            return {**state, "event_type": "gmail",
                    "event_payload": {"emails": important}}

    return {**state, "event_type": "sheet_check", "event_payload": {}}

def _route_manager(state: ManagerState):
    t = state["event_type"]
    if t == "agent_msg":    return "handle_agent_msg"
    if t == "new_lead":     return "handle_new_lead"
    if t == "whatsapp_msg": return "handle_whatsapp"
    if t == "gmail":        return "handle_gmail"
    if t == "sheet_check":  return "handle_sheet"
    return END


def manager_handle_agent_msg_node(state: ManagerState) -> ManagerState:
    msg        = state["event_payload"]
    msg_type   = msg.get("message_type", "")
    payload    = msg.get("payload", {})
    from_agent = msg.get("from_agent", "unknown")
    actions    = []

    log.info(f"[{AGENCY_NAME} Manager] Agent message: {msg_type} from {from_agent}")

    if msg_type == "oos_decision_request":
        lead_id   = (payload or {}).get("lead_id", "")
        post_text = (payload or {}).get("post", "")
        decision  = call_llm([{
            "role": "user",
            "content": (
                f"OOS post from watcher:\n{post_text}\n\n"
                "Should we approach this as a potential partner/future client? "
                "Reply: APPROACH or SKIP. Then one sentence reason."
            )
        }], max_tokens=80)
        if decision and "APPROACH" in decision.upper():
            update_oos_status(lead_id, "approach")
            send_agent_message("manager", from_agent, "oos_approved",
                               {"lead_id": lead_id}, lead_id)
            actions.append(f"OOS approved: {lead_id}")
        else:
            update_oos_status(lead_id, "skipped")
            actions.append(f"OOS skipped: {lead_id}")

    elif msg_type == "executor_report":
        project_id  = (payload or {}).get("project_id", "")
        client_name = (payload or {}).get("client", "")
        status      = (payload or {}).get("status", "")
        deploy_note = (payload or {}).get("deploy_note", "")
        ceo_msg = call_llm([{
            "role": "user",
            "content": (
                f"You are manager of {AGENCY_NAME} AI automation agency.\n"
                f"Project complete.\nClient: {client_name}\n"
                f"Project ID: {project_id}\nStatus: {status}\n"
                f"Deploy note: {deploy_note}\n\n"
                "Write a brief WhatsApp update for the CEO."
            )
        }], max_tokens=200)
        if ceo_msg:
            wpp_send(CEO_WHATSAPP, ceo_msg)
            actions.append(f"CEO notified about project {project_id}")

    elif msg_type == "missing_info":
        project_id = (payload or {}).get("project_id", "")
        detail     = (payload or {}).get("detail", "")
        wpp_send(CEO_WHATSAPP,
                 f"⚠️ [{AGENCY_NAME}] Project {project_id} blocked.\n"
                 f"Missing info:\n{detail[:300]}\n\nPlease provide the missing details.")
        actions.append(f"CEO alerted: missing info for {project_id}")

    elif msg_type == "error_flag":
        detail = (payload or {}).get("detail", "")
        wpp_send(CEO_WHATSAPP, f"🚨 [{AGENCY_NAME}] Agent error:\n{detail[:400]}")
        actions.append("CEO alerted: agent error")

    elif msg_type == "researcher_command":
        cmd = (payload or {}).get("command", "")
        if cmd == "ondemand":
            platforms = (payload or {}).get("platforms", ["linkedin"])
            _CORE_POOL.submit(researcher_ondemand_cycle, platforms)
            actions.append(f"On-demand research started: {platforms}")
        else:
            send_agent_message("manager", "researcher", "command", payload)
            actions.append(f"Researcher command forwarded: {payload}")

    elif msg_type == "heartbeat":
        # FIX 9: Researcher heartbeat acknowledged
        log.info(f"[{AGENCY_NAME} Manager] ❤ Heartbeat from {from_agent}")
        actions.append(f"heartbeat_ack:{from_agent}")

    return {**state, "actions_taken": actions, "done": True}


def manager_handle_new_lead_node(state: ManagerState) -> ManagerState:
    leads   = state["event_payload"].get("leads", [])
    actions = []

    for lead in leads:
        lead_id  = lead["lead_id"]
        name     = lead["name"]
        platform = lead["platform"]
        notes    = lead["watcher_notes"] or ""
        conv     = lead["conversation"]

        conv_text = "\n".join(
            f"{m['role'].upper()}: {m['text']}" for m in conv[-5:]
        )
        first_msg = call_llm([{
            "role": "user",
            "content": (
                f"You are the Manager of {AGENCY_NAME}, an AI automation agency.\n"
                f"Lead name: {name}\nPlatform: {platform}\n"
                f"Context: {notes[:300]}\n"
                f"Recent conversation:\n{conv_text}\n\n"
                "Generate first WhatsApp outreach message. "
                "Professional, friendly, brief. Mention NoError naturally."
            )
        }], max_tokens=300)

        if first_msg:
            rows = db_fetch("SELECT whatsapp FROM leads WHERE lead_id=%s", (lead_id,))
            if rows and rows[0][0]:
                wa_number = rows[0][0]
                ok = wpp_send(wa_number, first_msg)
                if ok:
                    db_execute(
                        "UPDATE leads SET manager_contacted=true WHERE lead_id=%s",
                        (lead_id,)
                    )
                    update_lead_status(lead_id, "manager_in_contact")
                    actions.append(f"WhatsApp sent to {name} ({lead_id})")
                    wpp_send(CEO_WHATSAPP,
                             f"📥 [{AGENCY_NAME}] New lead contacted: {name} ({platform})\n"
                             f"Lead ID: {lead_id}")

    return {**state, "actions_taken": actions, "done": True}

def manager_handle_whatsapp_node(state: ManagerState) -> ManagerState:
    """Process unread messages from DB"""
    actions = []
    ceo_jid = _clean_jid(CEO_WHATSAPP)
    
    # Get unread messages from DB
    rows = db_fetch("""
        SELECT id, jid, message_id, body 
        FROM incoming_messages 
        WHERE is_read = false 
        ORDER BY created_at ASC 
        LIMIT 20
    """)
    
    for row in rows:
        db_id, jid, msg_id, body = row
        
        if not body or not jid:
            db_execute("UPDATE incoming_messages SET is_read=true WHERE id=%s", (db_id,))
            continue
            
        sender_number = jid.replace("@s.whatsapp.net", "").replace("@c.us", "")
        is_ceo = jid == ceo_jid
        body_lower = body.lower().strip()
        
        log.info(f"[{AGENCY_NAME} Manager] WA from {'CEO' if is_ceo else sender_number}: {body[:80]}")
        
        if is_ceo:
            if body_lower in ("gmail on", "gmail enable"):
                set_gmail_enabled(True)
                wpp_send(CEO_WHATSAPP, "✅ Gmail monitoring enabled.")
                actions.append("Gmail enabled by CEO")
                db_execute("UPDATE incoming_messages SET is_read=true WHERE id=%s", (db_id,))
                continue
                
            elif body_lower in ("gmail off", "gmail disable"):
                set_gmail_enabled(False)
                wpp_send(CEO_WHATSAPP, "🔕 Gmail monitoring disabled.")
                actions.append("Gmail disabled by CEO")
                db_execute("UPDATE incoming_messages SET is_read=true WHERE id=%s", (db_id,))
                continue
            
            history = wpp_get_history_context(ceo_jid, limit=6)
            history_text = "\n".join(
                f"{'MANAGER' if m['role'] == 'me' else 'CEO'}: {m['body']}"
                for m in history
            )
            response = call_llm([{
                "role": "user",
                "content": (
                    f"You are the Manager of {AGENCY_NAME} AI automation agency.\n"
                    f"Conversation history (context only):\n{history_text}\n\n"
                    f"CEO's NEW message: {body}\n\n"
                    "Parse this command and respond with action taken. "
                    "If project approval, extract project details."
                )
            }], max_tokens=300)
            
            if response:
                wpp_send(CEO_WHATSAPP, response)
                actions.append(f"CEO command handled: {body[:60]}")
            
            if any(kw in body_lower for kw in ["approve", "start", "go ahead"]):
                rows_proj = db_fetch("""
                    SELECT project_id, executor_type FROM projects
                    WHERE status='pending_ceo'
                    ORDER BY created_at DESC LIMIT 1
                """)
                if rows_proj:
                    project_id, exec_type = rows_proj[0]
                    db_execute(
                        "UPDATE projects SET status='approved' WHERE project_id=%s",
                        (project_id,)
                    )
                    db_notify("executor_channel", f"{exec_type}:{project_id}")
                    actions.append(f"Project {project_id} approved")
        
        else:
            rows_lead = db_fetch(
                "SELECT lead_id, name FROM leads WHERE whatsapp=%s LIMIT 1",
                (sender_number,)
            )
            
            history = wpp_get_history_context(jid, limit=6)
            history_text = "\n".join(
                f"{'MANAGER' if m['role'] == 'me' else 'CLIENT'}: {m['body']}"
                for m in history
            )
            
            if rows_lead:
                lead_id, name = rows_lead[0]
                reply = call_llm([{
                    "role": "user",
                    "content": (
                        f"You are Manager of {AGENCY_NAME} AI automation agency.\n"
                        f"Client: {name}\n"
                        f"Conversation history (context only, do NOT reply to these):\n"
                        f"{history_text}\n\n"
                        f"Client's NEW message to reply to: {body}\n\n"
                        "Generate manager reply. Be concise and professional."
                    )
                }], max_tokens=350)
                if reply:
                    wpp_send(sender_number, reply)
                    actions.append(f"Client {name} replied via WhatsApp")
            
            else:
                log.info(f"[{AGENCY_NAME} Manager] Unknown number {sender_number} — replying")
                reply = call_llm([{
                    "role": "user",
                    "content": (
                        f"You are the Manager of {AGENCY_NAME}, an AI automation agency.\n"
                        f"Someone messaged us from an unknown number.\n"
                        f"Conversation history (context only):\n{history_text}\n\n"
                        f"Their NEW message: {body}\n\n"
                        "Reply warmly, introduce NoError briefly, ask how you can help. "
                        "Keep it natural — do not mention AI or bots."
                    )
                }], max_tokens=300)
                if reply:
                    wpp_send(sender_number, reply)
                    actions.append(f"Unknown number replied: {sender_number}")
        
        # Mark as read
        db_execute("UPDATE incoming_messages SET is_read=true WHERE id=%s", (db_id,))
    
    return {**state, "actions_taken": actions, "done": True}


def manager_handle_gmail_node(state: ManagerState) -> ManagerState:
    emails  = state["event_payload"].get("emails", [])
    actions = []
    for em in emails:
        sender  = em["sender"]
        subject = em["subject"]
        body    = em["body"]
        log.info(f"[{AGENCY_NAME} Manager] Gmail from {sender}: {subject}")
        replied = gmail_reply(sender, subject, body)
        if replied:
            actions.append(f"Gmail replied to {sender}: {subject[:50]}")
        if any(kw in subject.lower() for kw in ["payment", "invoice", "urgent", "contract"]):
            wpp_send(CEO_WHATSAPP,
                     f"📧 [{AGENCY_NAME}] Important email from {sender}:\n"
                     f"Subject: {subject}\n{body[:300]}")
            actions.append(f"CEO alerted about email: {subject}")
    return {**state, "actions_taken": actions, "done": True}


def manager_handle_sheet_node(state: ManagerState) -> ManagerState:
    actions = []
    try:
        rows    = get_payment_sheet_data()
        overdue = [r for r in rows
                   if r.get("status", "").lower() in ("overdue", "pending", "unpaid")]
        if overdue:
            summary = "\n".join(
                f"• {r.get('client_name','?')} — {r.get('amount','?')} ({r.get('status','?')})"
                for r in overdue[:5]
            )
            wpp_send(CEO_WHATSAPP, f"💰 [{AGENCY_NAME}] Payment Alert:\n\n{summary}")
            actions.append(f"CEO alerted: {len(overdue)} pending payments")
    except Exception as exc:
        log.error(f"[{AGENCY_NAME} Manager] Sheet check error: {exc}")
    return {**state, "actions_taken": actions, "done": True}


# ============================================================
# ANALYZER NODES
# ============================================================

def analyzer_fetch_node(state: AnalyzerState) -> AnalyzerState:
    if state.get("oos_id"):
        rows = db_fetch("""
            SELECT lead_id, platform, post_url, post_content, username, source_type
            FROM oos_temp
            WHERE lead_id=%s AND status='pending_analyzer'
        """, (state["oos_id"],))
        if not rows:
            return {**state, "done": True}
        r    = rows[0]
        item = {"lead_id": r[0], "platform": r[1], "post_url": r[2],
                "post_content": r[3], "username": r[4], "source_type": r[5]}
    else:
        pending = get_pending_oos()
        if not pending:
            return {**state, "done": True}
        item = pending[0]

    return {**state, "oos_id": item["lead_id"], "oos_data": item}


def _route_after_fetch(state: AnalyzerState) -> Literal["analyze", "__end__"]:
    return "__end__" if state.get("done") else "analyze"


def analyzer_analyze_node(state: AnalyzerState) -> AnalyzerState:
    data     = state["oos_data"]
    post     = data.get("post_content", "")
    platform = data.get("platform", "")
    username = data.get("username", "")

    result = call_llm([{
        "role": "user",
        "content": (
            f"Agency: {AGENCY_NAME}\nPlatform: {platform}\n"
            f"User: {username}\nPost:\n{post}\n\n"
            "This post was flagged as OOS. Decide:\n"
            "APPROACH — worth contacting\nSKIP — not relevant\n\n"
            "Format:\nDECISION: APPROACH or SKIP\nREASON: one sentence"
        )
    }], max_tokens=100)

    decision = "SKIP"
    reason   = "LLM unavailable"
    if result:
        for line in result.strip().split("\n"):
            if line.startswith("DECISION:"):
                decision = "APPROACH" if "APPROACH" in line.upper() else "SKIP"
            if line.startswith("REASON:"):
                reason = line.replace("REASON:", "").strip()

    return {**state, "decision": decision, "reason": reason}


def analyzer_act_node(state: AnalyzerState) -> AnalyzerState:
    oos_id   = state["oos_id"]
    decision = state["decision"]
    reason   = state["reason"]
    actions  = []

    if decision == "APPROACH":
        update_oos_status(oos_id, "approach")
        send_agent_message("analyzer", "manager", "oos_decision_request",
                           {"lead_id": oos_id, "decision": "APPROACH", "reason": reason},
                           oos_id)
        actions.append(f"OOS {oos_id} → APPROACH")
    else:
        update_oos_status(oos_id, "skipped")
        actions.append(f"OOS {oos_id} → SKIP")

    return {**state, "actions_taken": actions, "done": True}


# ============================================================
# RESEARCHER NODES
# ============================================================

def researcher_scrape_node(state: ResearcherState) -> ResearcherState:
    query    = state["query"]
    platform = state["platform"]
    results  = []

    log.info(f"[{AGENCY_NAME} Researcher] Scraping {platform}: {query}")

    if platform == "twitter":
        results = playwright_scrape_twitter(query)
    elif platform == "reddit":
        results = playwright_scrape_reddit(query)
    elif platform == "google":
        results = playwright_scrape_google(query)
    elif platform == "linkedin":
        results = playwright_scrape_linkedin(query)
    elif platform == "youtube" and state.get("youtube_enabled"):
        results = scrape_youtube_transcript(query)

    return {**state, "results": results}


def researcher_summarize_node(state: ResearcherState) -> ResearcherState:
    return {**state, "summary": "", "done": True}


def researcher_report_node(state: ResearcherState) -> ResearcherState:
    log.info(f"[{AGENCY_NAME} Researcher] Query done: {state['query']} ({state['platform']})")
    return {**state, "done": True}


def researcher_build_final_summary():
    log.info(f"[{AGENCY_NAME} Researcher] Building final summary...")
    platforms   = ["twitter", "reddit", "google", "youtube"]
    all_results = []
    for platform in platforms:
        all_results.extend(research_collect_all_cached(platform))

    if not all_results:
        log.warning(f"[{AGENCY_NAME} Researcher] No cached results found")
        return

    combined = "\n\n".join(
        f"[{r.get('platform','?')}] {r.get('text','')[:400]}"
        for r in all_results[:40]
    )
    summary = call_llm([{
        "role": "user",
        "content": (
            f"You are the Researcher agent for {AGENCY_NAME} AI automation agency.\n"
            f"20-hour market research data.\n"
            f"Research queries: {', '.join(RESEARCH_KEYWORDS)}\n\n"
            f"Scraped content:\n{combined}\n\n"
            "Write a comprehensive market intelligence report (10-15 bullet points). "
            "Cover: trends, client pain points, competitor moves, opportunities, "
            "recommended outreach angles."
        )
    }], max_tokens=1000)

    if not summary:
        return

    send_agent_message("researcher", "manager", "research_summary",
                       {"summary": summary, "platform": "all",
                        "queries": RESEARCH_KEYWORDS, "items": len(all_results)})
    log.info(f"[{AGENCY_NAME} Researcher] Summary sent to Manager")


# ============================================================
# LANGGRAPH GRAPH BUILDERS
# ============================================================

from psycopg_pool import ConnectionPool

_conn_pool   = ConnectionPool(DATABASE_URL, max_size=5, open=False)
_conn_pool.open(wait=True)
_CHECKPOINTER = PostgresSaver(_conn_pool)
try:
    _CHECKPOINTER.setup()
except Exception as _ce:
    log.warning(f"[Checkpointer] setup() skipped (tables exist): {_ce}")


def build_manager_graph():
    g = StateGraph(ManagerState)
    g.add_node("listen",           manager_listen_node)
    g.add_node("handle_agent_msg", manager_handle_agent_msg_node)
    g.add_node("handle_new_lead",  manager_handle_new_lead_node)
    g.add_node("handle_whatsapp",  manager_handle_whatsapp_node)
    g.add_node("handle_gmail",     manager_handle_gmail_node)
    g.add_node("handle_sheet",     manager_handle_sheet_node)

    g.set_entry_point("listen")
    g.add_conditional_edges("listen", _route_manager, {
        "handle_agent_msg": "handle_agent_msg",
        "handle_new_lead":  "handle_new_lead",
        "handle_whatsapp":  "handle_whatsapp",
        "handle_gmail":     "handle_gmail",
        "handle_sheet":     "handle_sheet",
        END: END,
    })
    for node in ["handle_agent_msg", "handle_new_lead",
                 "handle_whatsapp", "handle_gmail", "handle_sheet"]:
        g.add_edge(node, END)

    return g.compile(checkpointer=_CHECKPOINTER)


def build_analyzer_graph():
    g = StateGraph(AnalyzerState)
    g.add_node("fetch",   analyzer_fetch_node)
    g.add_node("analyze", analyzer_analyze_node)
    g.add_node("act",     analyzer_act_node)

    g.set_entry_point("fetch")
    g.add_conditional_edges("fetch", _route_after_fetch,
                            {"analyze": "analyze", END: END})
    g.add_edge("analyze", "act")
    g.add_edge("act", END)
    return g.compile(checkpointer=_CHECKPOINTER)


def build_researcher_graph():
    g = StateGraph(ResearcherState)
    g.add_node("scrape",    researcher_scrape_node)
    g.add_node("summarize", researcher_summarize_node)
    g.add_node("report",    researcher_report_node)

    g.set_entry_point("scrape")
    g.add_edge("scrape",    "summarize")
    g.add_edge("summarize", "report")
    g.add_edge("report",    END)
    return g.compile(checkpointer=_CHECKPOINTER)


MANAGER_GRAPH    = build_manager_graph()
ANALYZER_GRAPH   = build_analyzer_graph()
RESEARCHER_GRAPH = build_researcher_graph()


# ============================================================
# RUN HELPERS
# ============================================================

_CORE_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="core")


def run_manager_cycle():
    config   = {"configurable": {"thread_id": "manager_main"}}
    initial: ManagerState = {
        "event_type": "", "event_payload": {},
        "actions_taken": [], "errors": [], "done": False,
    }
    try:
        MANAGER_GRAPH.invoke(initial, config)
    except Exception as exc:
        log.error(f"[{AGENCY_NAME} Manager] Cycle error: {exc}")
        log_agent_error("manager", "crash", str(exc))


def run_analyzer(oos_id: str = ""):
    thread_id = (f"analyzer_{oos_id}" if oos_id
                 else f"analyzer_scan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
    config    = {"configurable": {"thread_id": thread_id}}
    initial: AnalyzerState = {
        "oos_id": oos_id, "oos_data": {},
        "decision": "", "reason": "",
        "actions_taken": [], "errors": [], "done": False,
    }
    try:
        ANALYZER_GRAPH.invoke(initial, config)
    except Exception as exc:
        log.error(f"[{AGENCY_NAME} Analyzer] Run error: {exc}")
        log_agent_error("analyzer", "crash", str(exc))


def run_researcher(query: str, platform: str, youtube_enabled: bool = False):
    thread_id = f"researcher_{platform}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    config    = {"configurable": {"thread_id": thread_id}}
    initial: ResearcherState = {
        "query": query, "platform": platform,
        "results": [], "summary": "",
        "youtube_enabled": youtube_enabled,
        "errors": [], "done": False,
    }
    try:
        RESEARCHER_GRAPH.invoke(initial, config)
    except Exception as exc:
        log.error(f"[{AGENCY_NAME} Researcher] Run error: {exc}")
        log_agent_error("researcher", "crash", str(exc))


def researcher_ondemand_cycle(platforms: list[str] = None, keywords: list[str] = None):
    youtube_on       = get_researcher_youtube_enabled()
    target_platforms = platforms or ["linkedin", "youtube"]
    target_keywords  = keywords or RESEARCH_KEYWORDS

    futures = []
    for kw in target_keywords:
        for platform in target_platforms:
            if platform == "youtube" and not youtube_on:
                continue
            futures.append(_CORE_POOL.submit(run_researcher, kw, platform, youtube_on))
            time.sleep(random.uniform(5, 15))

    for f in as_completed(futures):
        exc = f.exception()
        if exc:
            log.error(f"[{AGENCY_NAME} Researcher] On-demand error: {exc}")

    researcher_build_final_summary()
    # FIX 9: Send heartbeat after completing a cycle
    researcher_send_heartbeat()
    log.info(f"[{AGENCY_NAME} Researcher] Daily cycle done")


# ============================================================
# RESEARCHER HEARTBEAT LOOP — FIX 9
# ============================================================

def researcher_heartbeat_loop():
    """
    Background thread: sends heartbeat every 30 minutes
    so Manager can detect if Researcher goes silent.
    """
    HEARTBEAT_INTERVAL = 30 * 60  # 30 minutes
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        try:
            researcher_send_heartbeat()
        except Exception as exc:
            log.error(f"[{AGENCY_NAME} Researcher] Heartbeat error: {exc}")


# ============================================================
# FIX 4 — STARTUP MESSAGE
# ============================================================

def send_startup_message():
    if get_startup_sent():
        log.info(f"[{AGENCY_NAME}] Startup message already sent (DB flag). Skipping.")
        return

    log.info(f"[{AGENCY_NAME}] Waiting for WhatsApp connection...")
    for _ in range(24):
        if wpp_status():
            break
        time.sleep(5)

    if wpp_status():
        startup_text = (
            f"✅ *{AGENCY_NAME} System Online*\n\n"
            "Manager, Analyzer & Researcher are active.\n"
            "Watchers are running — I'll notify you on new leads, "
            "project updates, and any issues.\n\n"
            "Commands:\n"
            "• *gmail on* / *gmail off* — Gmail monitoring toggle\n"
            "• *approve* — Approve pending project\n\n"
            "Reply anytime to give commands. 🚀"
        )
        ok = wpp_send(CEO_WHATSAPP, startup_text)
        if ok:
            set_startup_sent()
            log.info(f"[{AGENCY_NAME}] ✅ Startup message sent to CEO")
        else:
            log.warning(f"[{AGENCY_NAME}] Startup message failed to send")
    else:
        log.warning(f"[{AGENCY_NAME}] WhatsApp not connected after 120s — startup message skipped")


# ============================================================
# POSTGRESQL NOTIFY LISTENER
# ============================================================

def _make_notify_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute("LISTEN agent_channel")
        cur.execute("LISTEN oos_channel")
    return conn


def core_notify_listener():
    log.info(f"[{AGENCY_NAME}] NOTIFY listener started")
    backoff = 5
    conn    = None

    while True:
        try:
            if conn is None:
                conn    = _make_notify_conn()
                backoff = 5
                log.info(f"[{AGENCY_NAME}] NOTIFY listener connected")

            r, _, _ = select.select([conn], [], [], 60)
            if not r:
                try:
                    with conn.cursor() as _ka:
                        _ka.execute("SELECT 1")
                except Exception:
                    raise
                continue

            conn.poll()
            while conn.notifies:
                notify  = conn.notifies.pop(0)
                channel = notify.channel
                payload = notify.payload.strip()

                log.info(f"[{AGENCY_NAME}] NOTIFY on '{channel}': '{payload}'")

                if channel == "agent_channel" and payload == "manager":
                    _CORE_POOL.submit(run_manager_cycle)
                elif channel == "oos_channel":
                    _CORE_POOL.submit(run_analyzer, payload)

        except Exception as exc:
            log.error(f"[{AGENCY_NAME}] NOTIFY error: {exc} — reconnecting in {backoff}s")
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)


# ============================================================
# ENV VALIDATION
# ============================================================

def _validate_env():
    required = [
        "OPENAI_API_KEY", "DATABASE_URL", "CEO_WHATSAPP_NUMBER",
        "GMAIL_USER", "GMAIL_APP_PASSWORD", "GOOGLE_SHEET_URL",
        "GITHUB_TOKEN", "GITHUB_REPO", "EVOLUTION_URL",
        "EVOLUTION_API_KEY", "EVOLUTION_INSTANCE",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            f"[{AGENCY_NAME}] Missing env variables: {', '.join(missing)}"
        )
    log.info(f"[{AGENCY_NAME}] All env variables present ✓")


# ============================================================
# MAIN
# ============================================================

def periodic_health_check():
    """
    Background thread that checks database health every 5 minutes
    and alerts CEO if connection becomes unhealthy.
    """
    while True:
        time.sleep(300)  # 5 minutes
        try:
            if health_check():
                log.debug("[Health] ✅ Database connection healthy")
            else:
                log.warning("[Health] ⚠️ Database connection unhealthy - attempting recovery")
                # The health_check() function will automatically try to reconnect
                # Alert CEO about the issue
                try:
                    wpp_send(CEO_WHATSAPP, 
                        f"⚠️ [{AGENCY_NAME}] Database connection health check failed. "
                        "System is attempting automatic recovery.")
                except Exception:
                    pass  # Don't crash if WhatsApp is also down
        except Exception as e:
            log.error(f"[Health] Health check error: {e}")

from flask import Flask
app_http = Flask(__name__)

@app_http.route("/", methods=["GET"])
def health():
    return "ok", 200

@app_http.route("/webhook/whatsapp", methods=["POST"])
def whatsapp_webhook():
    from flask import request
    try:
        data = request.get_json()
        event = data.get("event", "")
        
        if event == "messages.upsert":
            msg_data = data.get("data", {})
            key = msg_data.get("key", {})
            message = msg_data.get("message", {})
            
            jid = key.get("remoteJid", "")
            msg_id = key.get("id", "")
            from_me = key.get("fromMe", False)
            timestamp = msg_data.get("messageTimestamp", 0)
            
            body = (
                message.get("conversation") or
                message.get("extendedTextMessage", {}).get("text") or
                ""
            )
            
            if jid and msg_id and body and not from_me:
                db_execute("""
                    INSERT INTO incoming_messages 
                    (jid, message_id, body, from_me, timestamp, is_read)
                    VALUES (%s, %s, %s, %s, %s, false)
                    ON CONFLICT (message_id) DO NOTHING
                """, (jid, msg_id, body, from_me, timestamp))
                
                db_notify("agent_channel", "manager")
                
        return "ok", 200
    except Exception as e:
        log.error(f"Webhook error: {e}")
        return "error", 500
 
def main():
    log.info("══════════════════════════════════════")
    log.info(f"  {AGENCY_NAME} — Manager + Analyzer + Researcher")
    log.info("══════════════════════════════════════")
 
    # Validate environment variables
    _validate_env()
    
    # ✅ FIX: Wait for database to be ready (handles recovery mode)
    log.info("[Startup] Waiting for database to be ready...")
    if not wait_for_db(retries=30, delay=3):
        log.error("❌ Database never became ready after 90 seconds. Exiting.")
        log.error("This usually means:")
        log.error("  1. Database is still in recovery/failover")
        log.error("  2. DATABASE_URL is incorrect")
        log.error("  3. Database service is down")
        return
    
    log.info("✅ Database is ready and accepting connections")
    
    # Setup database schema
    log.info("Database connection pool is active.")
    
    # ✅ FIX: Start database health monitoring
    threading.Thread(
        target=periodic_health_check, 
        daemon=True,
        name="db-health-monitor"
    ).start()
    log.info("✅ Database health monitor started")
 
    # Send startup message to CEO
    threading.Thread(
        target=send_startup_message, 
        daemon=True,
        name="startup-msg"
    ).start()
 
    # Start PostgreSQL NOTIFY listener
    threading.Thread(
        target=core_notify_listener, 
        daemon=True,
        name="notify-listener"
    ).start()
 
    # Start HTTP webhook server
    threading.Thread(
        target=lambda: app_http.run(host="0.0.0.0", port=8080),
        daemon=True, name="webhook-server"
    ).start()
  
    # Start researcher heartbeat background thread
    threading.Thread(
        target=researcher_heartbeat_loop, 
        daemon=True,
        name="researcher-hb"
    ).start()
 
    # Schedule periodic tasks
    schedule.every(30).minutes.do(
        lambda: _CORE_POOL.submit(check_researcher_health)
    )
 
    schedule.every().day.at("08:00").do(
        lambda: _CORE_POOL.submit(researcher_ondemand_cycle)
    )
 
    log.info(f"[{AGENCY_NAME}] All systems running ✅")
    log.info(f"[{AGENCY_NAME}] Gmail: {'ON' if get_gmail_enabled() else 'OFF (send gmail on to enable)'}")
    log.info(f"[{AGENCY_NAME}] Database: Connected and healthy ✅")
 
    # Main loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        log.info("Shutting down gracefully...")
        close_pool()
        log.info("✅ Shutdown complete")
 
 
if __name__ == "__main__":
    main()
