"""
watchers_executors.py
=====================
Agency — FILE 1
Contains: 4 Watchers + 4 Repliers + 4 Executors

Updates in this version:
  - Selenium REMOVED → Playwright + Browserless.io (remote Chrome pool)
  - playwright-stealth on every context (anti-bot fingerprint)
  - Tor + Privoxy proxy on all browser sessions (IP rotation)
  - Executors: real tool-equipped agentic loop
      LLM decides steps → agent executes real tools:
      browser_action, run_code, github_push, api_call, write_file, read_url
  - LangGraph checkpoints via PostgreSQL (preserved)
  - Connection pool, bounded thread pools, LLM retry (preserved)
"""

import os
import json
import time
import select
import random
import logging
import threading
import subprocess
import tempfile
from datetime import datetime
from typing import TypedDict, Annotated, Literal
from concurrent.futures import ThreadPoolExecutor, as_completed
import operator

import psycopg2
import psycopg2.extensions
import psycopg2.pool
import requests
import schedule

from openai import OpenAI, APIError, APITimeoutError, RateLimitError
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver

from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

# ============================================================
# CONFIG & SECRETS
# ============================================================

OPENAI_MODEL    = os.environ.get("OPENAI_MODEL_ID",
                   "ft:gpt-4o-mini-2024-07-18:personal:final-brain-1:DREfTesR")
DATABASE_URL    = os.environ["DATABASE_URL"]
DISCORD_TOKEN   = os.environ.get("DISCORD_TOKEN", "")
DISCORD_API     = "https://discord.com/api/v10"
GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO     = os.environ.get("GITHUB_REPO", "")

WATCHER_KEYWORDS = [
    "AI automation expert", "Chatbot developer needed",
    "Lead Gen expert", "social media marketing expert",
    "looking for automation", "need chatbot",
    "hire AI developer", "n8n developer", "make.com",
]

LINKEDIN_JOB_KEYWORDS = [
    "AI automation", "chatbot developer",
    "lead generation automation", "n8n developer",
    "make.com developer", "AI agent developer",
]

REDDIT_SUBREDDITS = [
    "entrepreneur", "SaaS", "smallbusiness", "agency",
    "automation", "webdev", "ecommerce", "freelance",
    "marketing", "startups", "digitalnomad",
]

DISCORD_SERVERS  = json.loads(os.environ.get("DISCORD_SERVERS", "[]"))
DISCORD_CHANNELS = json.loads(os.environ.get("DISCORD_CHANNELS", "[]"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
)
log = logging.getLogger("agency")
openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])


# ============================================================
# CONNECTION POOL
# ============================================================

_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2, maxconn=20, dsn=DATABASE_URL
                )
    return _pool


def get_conn():
    return _get_pool().getconn()


def put_conn(conn):
    try:
        _get_pool().putconn(conn)
    except Exception:
        pass


# ============================================================
# LANGGRAPH STATE SCHEMAS
# ============================================================

class WatcherState(TypedDict):
    platform:        str
    keyword:         str
    posts_found:     list
    posts_processed: Annotated[list, operator.add]
    errors:          Annotated[list, operator.add]
    cycle_done:      bool
    retry_count:     int


class ReplierState(TypedDict):
    platform:     str
    inbox_items:  list
    replies_sent: Annotated[list, operator.add]
    errors:       Annotated[list, operator.add]
    cycle_done:   bool


class ExecutorState(TypedDict):
    project_id:    str
    executor_type: str
    project_data:  dict
    tool_log:      Annotated[list, operator.add]
    test_passed:   bool
    test_attempts: int
    deployed:      bool
    errors:        Annotated[list, operator.add]
    done:          bool


# ============================================================
# DATABASE UTILITIES
# ============================================================

def is_already_processed(lead_id: str) -> bool:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM leads WHERE lead_id=%s", (lead_id,))
        return cur.fetchone() is not None
    finally:
        cur.close(); put_conn(conn)


def save_lead(lead_id, username, platform, source, post_content, status):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO leads (lead_id, name, platform, source, status, watcher_notes)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (lead_id) DO NOTHING
        """, (lead_id, username, platform, source, status, post_content[:500]))
        conn.commit()
    except Exception as exc:
        conn.rollback(); log.error(f"save_lead: {exc}")
    finally:
        cur.close(); put_conn(conn)


def save_conversation(lead_id, platform, username, role, text):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT messages FROM watcher_conversations WHERE lead_id=%s", (lead_id,)
        )
        row = cur.fetchone()
        new_msg = {"role": role, "text": text,
                   "timestamp": datetime.utcnow().isoformat()}
        if row:
            msgs = row[0] if isinstance(row[0], list) else json.loads(row[0])
            msgs.append(new_msg)
            cur.execute(
                "UPDATE watcher_conversations SET messages=%s, last_message_at=NOW(),"
                " current_status=%s WHERE lead_id=%s",
                (json.dumps(msgs), "in_conversation", lead_id),
            )
        else:
            cur.execute("""
                INSERT INTO watcher_conversations
                    (lead_id, platform, username, messages, current_status, last_message_at)
                VALUES (%s,%s,%s,%s,'approached',NOW())
            """, (lead_id, platform, username, json.dumps([new_msg])))
        conn.commit()
    except Exception as exc:
        conn.rollback(); log.error(f"save_conversation: {exc}")
    finally:
        cur.close(); put_conn(conn)


def save_oos_temp(lead_id, platform, post_url, post_content, username, source_type):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO oos_temp
                (lead_id, platform, post_url, post_content, username, source_type, status)
            VALUES (%s,%s,%s,%s,%s,%s,'pending_analyzer')
            ON CONFLICT (lead_id) DO NOTHING
        """, (lead_id, platform, post_url, post_content, username, source_type))
        conn.commit()
    except Exception as exc:
        conn.rollback(); log.error(f"save_oos_temp: {exc}")
    finally:
        cur.close(); put_conn(conn)


def update_lead_status(lead_id, status):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE leads SET status=%s, last_updated=NOW() WHERE lead_id=%s",
            (status, lead_id),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback(); log.error(f"update_lead_status: {exc}")
    finally:
        cur.close(); put_conn(conn)


def get_conversation(lead_id: str) -> dict | None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT messages, username, platform FROM watcher_conversations WHERE lead_id=%s",
            (lead_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        msgs = row[0] if isinstance(row[0], list) else json.loads(row[0])
        return {"messages": msgs, "username": row[1], "platform": row[2]}
    finally:
        cur.close(); put_conn(conn)


def get_lead_id_by_username(username: str, platform: str) -> str | None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT lead_id FROM leads WHERE name=%s AND platform=%s LIMIT 1",
            (username, platform),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close(); put_conn(conn)


def already_replied_to_latest(lead_id: str) -> bool:
    conv = get_conversation(lead_id)
    if not conv or not conv["messages"]:
        return False
    return conv["messages"][-1]["role"] == "watcher"


def save_incoming_message(lead_id, platform, username, text):
    save_conversation(lead_id, platform, username, "lead", text)


def save_our_reply(lead_id, platform, username, text):
    save_conversation(lead_id, platform, username, "watcher", text)


def get_watcher_meta(key: str) -> str | None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT meta_value FROM watcher_meta WHERE meta_key=%s", (key,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close(); put_conn(conn)


def set_watcher_meta(key: str, value: str):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO watcher_meta (meta_key, meta_value, updated_at)
            VALUES (%s,%s,NOW())
            ON CONFLICT (meta_key) DO UPDATE SET meta_value=%s, updated_at=NOW()
        """, (key, value, value))
        conn.commit()
    except Exception as exc:
        conn.rollback(); log.error(f"set_watcher_meta: {exc}")
    finally:
        cur.close(); put_conn(conn)


def notify_manager(from_agent, message_type, payload, related_id, related_type="lead"):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_messages
                (from_agent, to_agent, message_type, payload, related_id, related_type)
            VALUES (%s,'manager',%s,%s,%s,%s)
        """, (from_agent, message_type, json.dumps(payload), related_id, related_type))
        conn.commit()
        cur.execute("SELECT pg_notify('agent_channel', 'manager')")
        conn.commit()
        # Fire oos_channel with lead_id so Analyzer wakes with correct oos_id
        if message_type == "oos_decision_request":
            cur.execute("SELECT pg_notify('oos_channel', %s)", (related_id,))
            conn.commit()
    except Exception as exc:
        conn.rollback(); log.error(f"notify_manager: {exc}")
    finally:
        cur.close(); put_conn(conn)


def get_pending_project(executor_type: str, project_id: str = "") -> dict | None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        if project_id:
            cur.execute("""
                SELECT project_id, client_name, client_whatsapp, service_type,
                       scope_summary, tools_needed, executor_type
                FROM projects WHERE project_id=%s AND status='approved'
            """, (project_id,))
        else:
            cur.execute("""
                SELECT project_id, client_name, client_whatsapp, service_type,
                       scope_summary, tools_needed, executor_type
                FROM projects WHERE status='approved' AND executor_type=%s
                ORDER BY created_at ASC LIMIT 1
            """, (executor_type,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "project_id": row[0], "client_name": row[1],
            "client_whatsapp": row[2], "service_type": row[3],
            "scope_summary": row[4], "tools_needed": row[5],
            "executor_type": row[6],
        }
    finally:
        cur.close(); put_conn(conn)


def update_project_status(project_id, status):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE projects SET status=%s, last_updated=NOW() WHERE project_id=%s",
            (status, project_id),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback(); log.error(f"update_project_status: {exc}")
    finally:
        cur.close(); put_conn(conn)


def log_agent_error(agent_name, error_type, error_message, self_heal_attempts=0):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_errors
                (agent_name, error_type, error_message, self_heal_attempts, status)
            VALUES (%s,%s,%s,%s,'open')
        """, (agent_name, error_type, error_message, self_heal_attempts))
        conn.commit()
    except Exception as exc:
        conn.rollback(); log.error(f"log_agent_error: {exc}")
    finally:
        cur.close(); put_conn(conn)


def save_tool_log(project_id: str, tool: str, result: str, success: bool):
    """Full audit trail of every real action executor takes."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_errors
                (agent_name, error_type, error_message, self_heal_attempts, status)
            VALUES (%s,%s,%s,0,%s)
        """, (
            f"executor:{project_id}", tool,
            result[:1000],
            "resolved" if success else "open",
        ))
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            cur.close()
        except Exception:
            pass
        put_conn(conn)


# ============================================================
# LLM WITH RETRY
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
            log.error(f"LLM API error: {exc}"); return None
        except Exception as exc:
            log.error(f"LLM unexpected: {exc}"); return None
    log.error("LLM failed after 3 attempts")
    return None


# ============================================================
# LLM PROMPT HELPERS
# ============================================================

def qualify_content(text: str, keyword: str, platform: str,
                    content_type: str = "post") -> str:
    result = call_llm([{
        "role": "user",
        "content": (
            f"Platform: {platform}\nContent type: {content_type}\n"
            f"Keyword: {keyword}\nContent:\n{text}\n\n"
            "Classify:\nENGAGE — directly needs our services\n"
            "OOS — out of scope but interesting (partnership, future potential)\n"
            "SKIP — completely irrelevant\n\nReply ONE word: ENGAGE, OOS, or SKIP"
        )
    }], max_tokens=5)
    if not result:
        return "SKIP"
    r = result.strip().upper()
    if "ENGAGE" in r: return "ENGAGE"
    if "OOS" in r:    return "OOS"
    return "SKIP"


def generate_first_dm(text, keyword, username, platform) -> str | None:
    return call_llm([{"role": "user", "content": (
        f"Platform: {platform}\nLead: {username}\n"
        f"Keyword: {keyword}\nPost:\n{text}\n\nGenerate first outreach DM."
    )}], max_tokens=300)


def generate_first_comment(text, keyword, platform) -> str | None:
    return call_llm([{"role": "user", "content": (
        f"Platform: {platform}\nKeyword: {keyword}\nPost:\n{text}\n\n"
        "Generate a helpful public comment. Natural, not spammy."
    )}], max_tokens=250)


def generate_reply(conv_history, new_message, username, platform, channel) -> str | None:
    history_text = "\n".join(f"{m['role'].upper()}: {m['text']}" for m in conv_history)
    return call_llm([{"role": "user", "content": (
        f"Platform: {platform}\nChannel: {channel}\nLead: {username}\n\n"
        f"Conversation:\n{history_text}\n\nNew message:\n{new_message}\n\nGenerate reply."
    )}], max_tokens=400)


def _make_lead_id(*parts: str) -> str:
    return "_".join(str(p).lower().replace(" ", "_") for p in parts)[:120]


def random_delay(min_sec=8, max_sec=22):
    time.sleep(random.uniform(min_sec, max_sec))


def _human_delay(min_ms=800, max_ms=2500):
    time.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


# ============================================================
# PLAYWRIGHT + BROWSERLESS + TOR + STEALTH
# Every browser session:
#   1. Connects to Browserless remote Chrome pool
#   2. Routes through Tor+Privoxy SOCKS5 (IP rotation)
#   3. playwright-stealth patches (anti-detection)
# ============================================================

def _new_stealth_context(playwright, cookies_raw: str = ""):
    """Connect to Browserless, apply Tor proxy + stealth."""
    browser = playwright.chromium.connect_over_cdp(
        f"{BROWSERLESS_URL}?stealth=true&--proxy-server={TOR_PROXY}"
    )
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
        locale="en-US",
        timezone_id="America/New_York",
    )
    stealth_sync(context)
    if cookies_raw:
        try:
            cookies = json.loads(cookies_raw)
            if cookies:
                context.add_cookies(cookies)
        except Exception as exc:
            log.warning(f"Cookie load: {exc}")
    return browser, context


# ============================================================
# DISCORD HELPERS (API — no browser)
# ============================================================

def discord_headers() -> dict:
    token = DISCORD_TOKEN
    if token and not token.startswith("Bot "):
        token = f"Bot {token}"
    return {"Authorization": token, "Content-Type": "application/json"}


def discord_get(endpoint, params=None):
    try:
        r = requests.get(f"{DISCORD_API}{endpoint}",
                         headers=discord_headers(), params=params, timeout=10)
        r.raise_for_status(); return r.json()
    except Exception as exc:
        log.warning(f"Discord GET {endpoint}: {exc}"); return None


def discord_post(endpoint, payload):
    try:
        r = requests.post(f"{DISCORD_API}{endpoint}",
                          headers=discord_headers(), json=payload, timeout=10)
        r.raise_for_status(); return r.json()
    except Exception as exc:
        log.warning(f"Discord POST {endpoint}: {exc}"); return None


def discord_send_dm(user_id, text) -> bool:
    ch = discord_post("/users/@me/channels", {"recipient_id": user_id})
    if not ch: return False
    return discord_post(f"/channels/{ch['id']}/messages", {"content": text}) is not None


def discord_send_channel(channel_id, text, reply_to=None) -> bool:
    payload = {"content": text}
    if reply_to:
        payload["message_reference"] = {"message_id": reply_to}
    return discord_post(f"/channels/{channel_id}/messages", payload) is not None


# ============================================================
# ══════════════════════════════════════════════════════════
#  WATCHER NODES
# ══════════════════════════════════════════════════════════
# ============================================================

# ── Facebook ────────────────────────────────────────────────

def fb_scrape_node(state: WatcherState) -> WatcherState:
    log.info(f"[FB-Watcher] Scraping: {state['keyword']}")
    posts = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw, os.environ.get("FB_COOKIES", ""))
            page = ctx.new_page()
            url = f"https://www.facebook.com/search/posts?q={state['keyword'].replace(' ','%20')}"
            page.goto(url, timeout=25000)
            page.wait_for_timeout(5000)
            seen: set = set()
            for el in page.query_selector_all("[data-ad-comet-preview='message']")[:10]:
                try:
                    text = el.inner_text().strip()
                    if not text: continue
                    try:
                        username = el.evaluate(
                            "el => el.closest('[data-pagelet]')?.querySelector('a[role=link] span')?.innerText || ''"
                        ).strip()
                    except Exception:
                        username = "unknown"
                    if not username or username in seen: continue
                    try:
                        post_url = el.evaluate(
                            "el => el.closest('[data-pagelet]')?.querySelector('a[href*=\"/posts/\"]')?.href || ''"
                        )
                    except Exception:
                        post_url = url
                    seen.add(username)
                    posts.append({"text": text, "username": username,
                                  "url": post_url or url, "keyword": state["keyword"],
                                  "platform": "facebook"})
                except Exception:
                    continue
            browser.close()
    except Exception as exc:
        log.error(f"[FB-Watcher] Scrape: {exc}")
        log_agent_error("fb_watcher", "scrape_error", str(exc))
        return {**state, "errors": [str(exc)]}
    return {**state, "posts_found": posts}


def fb_process_node(state: WatcherState) -> WatcherState:
    processed = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw, os.environ.get("FB_COOKIES", ""))
            page = ctx.new_page()
            for post in state["posts_found"]:
                username = post["username"]; post_text = post["text"]
                keyword  = post["keyword"];  post_url  = post["url"]
                lead_id  = _make_lead_id("fb", username)
                if is_already_processed(lead_id): continue
                q = qualify_content(post_text, keyword, "Facebook")
                if q == "SKIP": continue
                if q == "OOS":
                    save_oos_temp(lead_id, "facebook", post_url, post_text, username, "post")
                    notify_manager("facebook_watcher", "oos_decision_request",
                                   {"lead_id": lead_id, "post": post_text[:300]}, lead_id)
                    continue
                first_msg = generate_first_dm(post_text, keyword, username, "Facebook")
                if not first_msg: continue
                save_lead(lead_id, username, "facebook", "keyword_search", post_text, "approached")
                dm_ok = False
                try:
                    page.goto(f"https://www.facebook.com/search/people?q={username.replace(' ','%20')}", timeout=20000)
                    page.wait_for_timeout(4000); _human_delay()
                    page.wait_for_selector("text=Message", timeout=8000).click()
                    _human_delay()
                    page.wait_for_selector("[contenteditable='true']", timeout=8000).type(
                        first_msg, delay=random.randint(60, 140)
                    )
                    _human_delay(); page.keyboard.press("Enter")
                    page.wait_for_timeout(2000); dm_ok = True
                except Exception:
                    dm_ok = False
                random_delay(8, 18)
                if dm_ok:
                    save_conversation(lead_id, "facebook", username, "watcher", first_msg)
                    update_lead_status(lead_id, "discovery_in_progress")
                else:
                    comment = generate_first_comment(post_text, keyword, "Facebook")
                    if comment:
                        try:
                            page.goto(post_url, timeout=20000); page.wait_for_timeout(4000)
                            cb = page.wait_for_selector("[aria-label='Write a comment']", timeout=10000)
                            cb.click(); _human_delay()
                            cb.type(comment, delay=random.randint(60, 130)); _human_delay()
                            page.keyboard.press("Enter"); page.wait_for_timeout(3000)
                            save_conversation(lead_id, "facebook", username, "watcher", comment)
                            update_lead_status(lead_id, "discovery_in_progress")
                        except Exception:
                            update_lead_status(lead_id, "contact_failed")
                processed.append(lead_id); random_delay()
            browser.close()
    except Exception as exc:
        log.error(f"[FB-Watcher] Process: {exc}")
        log_agent_error("fb_watcher", "process_error", str(exc))
        return {**state, "errors": [str(exc)], "cycle_done": True}
    return {**state, "posts_processed": processed, "cycle_done": True}


# ── Reddit ──────────────────────────────────────────────────

def reddit_scrape_node(state: WatcherState) -> WatcherState:
    log.info(f"[Reddit-Watcher] Scraping: {state['keyword']}")
    posts = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw, os.environ.get("REDDIT_COOKIES", ""))
            page = ctx.new_page()
            for sub in REDDIT_SUBREDDITS:
                url = f"https://www.reddit.com/r/{sub}/search/?q={state['keyword'].replace(' ','+')}&sort=new&restrict_sr=1"
                page.goto(url, timeout=20000); page.wait_for_timeout(4000)
                for el in page.query_selector_all("div[data-testid='post-container']")[:5]:
                    try:
                        title = ""; body = ""
                        try: title = el.query_selector("h3").inner_text().strip()
                        except Exception: pass
                        try: body = el.query_selector("[data-click-id='body']").inner_text().strip()
                        except Exception: pass
                        text = f"{title} {body}".strip()
                        if not text: continue
                        try: username = el.query_selector("a[href*='/user/']").inner_text().strip().replace("u/","")
                        except Exception: username = "unknown"
                        try: post_url = el.query_selector("a[data-click-id='body']").get_attribute("href")
                        except Exception: post_url = url
                        if username and username != "unknown":
                            posts.append({"text": text, "username": username,
                                          "url": post_url, "subreddit": sub,
                                          "keyword": state["keyword"], "platform": "reddit"})
                    except Exception: continue
                random_delay(12, 25)
            browser.close()
    except Exception as exc:
        log.error(f"[Reddit-Watcher] Scrape: {exc}")
        log_agent_error("reddit_watcher", "scrape_error", str(exc))
        return {**state, "errors": [str(exc)]}
    seen: set = set()
    unique = []
    for p in posts:
        key = f"{p['username']}|{p['subreddit']}"
        if key not in seen:
            seen.add(key); unique.append(p)
    return {**state, "posts_found": unique}


def reddit_process_node(state: WatcherState) -> WatcherState:
    processed = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw, os.environ.get("REDDIT_COOKIES", ""))
            page = ctx.new_page()
            for post in state["posts_found"]:
                username = post["username"]; post_text = post["text"]
                keyword  = post["keyword"];  post_url  = post["url"]
                sub      = post["subreddit"]
                lead_id  = _make_lead_id("reddit", username, sub)
                if is_already_processed(lead_id): continue
                q = qualify_content(post_text, keyword, "Reddit")
                if q == "SKIP": continue
                if q == "OOS":
                    save_oos_temp(lead_id, "reddit", post_url, post_text, username, "post")
                    notify_manager("reddit_watcher", "oos_decision_request",
                                   {"lead_id": lead_id, "post": post_text[:300]}, lead_id)
                    continue
                save_lead(lead_id, username, "reddit", "subreddit_search", post_text, "approached")
                dm_text = generate_first_dm(post_text, keyword, username, "Reddit")
                if not dm_text: continue
                dm_ok = False
                try:
                    page.goto(f"https://www.reddit.com/message/compose/?to={username}", timeout=20000)
                    page.wait_for_timeout(4000)
                    page.wait_for_selector("#subject", timeout=10000).fill("Quick question about your post")
                    _human_delay()
                    page.query_selector("#message").type(dm_text, delay=random.randint(60, 130))
                    _human_delay(); page.query_selector("button[type='submit']").click()
                    page.wait_for_timeout(3000); dm_ok = True
                except Exception:
                    dm_ok = False
                random_delay(10, 22)
                if dm_ok:
                    save_conversation(lead_id, "reddit", username, "watcher", dm_text)
                    update_lead_status(lead_id, "discovery_in_progress")
                else:
                    comment = generate_first_comment(post_text, keyword, "Reddit")
                    if comment:
                        try:
                            page.goto(post_url, timeout=20000); page.wait_for_timeout(5000)
                            box = page.wait_for_selector("[placeholder='What are your thoughts?']", timeout=12000)
                            box.click(); _human_delay()
                            box.type(comment, delay=random.randint(60, 130)); _human_delay()
                            try: page.query_selector("button.save").click()
                            except Exception:
                                page.keyboard.press("Tab"); _human_delay(300, 600)
                                page.keyboard.press("Enter")
                            page.wait_for_timeout(3000)
                            save_conversation(lead_id, "reddit", username, "watcher", comment)
                            update_lead_status(lead_id, "discovery_in_progress")
                        except Exception:
                            update_lead_status(lead_id, "contact_failed")
                processed.append(lead_id); random_delay()
            browser.close()
    except Exception as exc:
        log.error(f"[Reddit-Watcher] Process: {exc}")
        log_agent_error("reddit_watcher", "process_error", str(exc))
        return {**state, "errors": [str(exc)], "cycle_done": True}
    return {**state, "posts_processed": processed, "cycle_done": True}


# ── LinkedIn ────────────────────────────────────────────────

def linkedin_scrape_node(state: WatcherState) -> WatcherState:
    log.info(f"[LinkedIn-Watcher] Scraping: {state['keyword']}")
    posts = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw, os.environ.get("LINKEDIN_COOKIES", ""))
            page = ctx.new_page()
            seen: set = set()
            url = f"https://www.linkedin.com/search/results/content/?keywords={state['keyword'].replace(' ','%20')}&sortBy=date_posted"
            page.goto(url, timeout=25000); page.wait_for_timeout(5000)
            for el in page.query_selector_all(".feed-shared-update-v2")[:5]:
                try:
                    try: text = el.query_selector(".feed-shared-update-v2__description span[dir='ltr']").inner_text().strip()
                    except Exception: text = el.inner_text().strip()
                    if not text: continue
                    try: author = el.query_selector(".app-aware-link .t-bold span[aria-hidden='true']").inner_text().strip()
                    except Exception: author = "unknown"
                    if not author or author == "unknown" or author in seen: continue
                    try: post_url = el.query_selector("a[href*='/posts/'],a[href*='/feed/update/']").get_attribute("href")
                    except Exception: post_url = url
                    seen.add(author)
                    posts.append({"type":"post","text":text,"username":author,
                                  "url":post_url,"keyword":state["keyword"],"platform":"linkedin"})
                except Exception: continue
            for jkw in LINKEDIN_JOB_KEYWORDS:
                jurl = f"https://www.linkedin.com/jobs/search/?keywords={jkw.replace(' ','%20')}&sortBy=DD"
                page.goto(jurl, timeout=25000); page.wait_for_timeout(5000)
                for jel in page.query_selector_all(".jobs-search__results-list li")[:3]:
                    try:
                        title   = jel.query_selector(".base-search-card__title").inner_text().strip()
                        company = jel.query_selector(".base-search-card__subtitle").inner_text().strip()
                        if not title or company in seen: continue
                        try: jpost_url = jel.query_selector("a.base-card__full-link").get_attribute("href")
                        except Exception: jpost_url = jurl
                        seen.add(company)
                        posts.append({"type":"job","text":f"{title} at {company}",
                                      "username":company,"title":title,"url":jpost_url,
                                      "keyword":jkw,"platform":"linkedin"})
                    except Exception: continue
                random_delay(8, 15)
            browser.close()
    except Exception as exc:
        log.error(f"[LinkedIn-Watcher] Scrape: {exc}")
        log_agent_error("linkedin_watcher", "scrape_error", str(exc))
        return {**state, "errors": [str(exc)]}
    return {**state, "posts_found": posts}


def linkedin_process_node(state: WatcherState) -> WatcherState:
    processed = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw, os.environ.get("LINKEDIN_COOKIES", ""))
            page = ctx.new_page()
            for post in state["posts_found"]:
                username = post["username"]; post_text = post["text"]
                keyword  = post["keyword"];  post_url  = post["url"]
                ptype    = post.get("type", "post")
                lead_id  = _make_lead_id("li_post" if ptype=="post" else "li_job", username)
                if is_already_processed(lead_id): continue
                q = qualify_content(post_text, keyword, "LinkedIn", ptype)
                if q == "SKIP": continue
                if q == "OOS":
                    save_oos_temp(lead_id, "linkedin", post_url, post_text, username, ptype)
                    notify_manager("linkedin_watcher", "oos_decision_request",
                                   {"lead_id": lead_id, "post": post_text[:300]}, lead_id)
                    continue
                save_lead(lead_id, username, "linkedin", f"{ptype}_search", post_text, "approached")
                if ptype == "post":
                    comment = generate_first_comment(post_text, keyword, "LinkedIn")
                    if comment:
                        try:
                            page.goto(post_url, timeout=20000); page.wait_for_timeout(5000)
                            page.wait_for_selector("button[aria-label*='Comment'],button:has-text('Comment')", timeout=10000).click()
                            _human_delay()
                            page.wait_for_selector(".ql-editor[contenteditable='true']", timeout=10000).type(comment, delay=random.randint(60,130))
                            _human_delay()
                            page.query_selector("button.comments-comment-box__submit-button").click()
                            page.wait_for_timeout(3000)
                            save_conversation(lead_id, "linkedin", username, "watcher", comment)
                            update_lead_status(lead_id, "discovery_in_progress")
                        except Exception: update_lead_status(lead_id, "contact_failed")
                elif ptype == "job":
                    note = generate_first_dm(post_text, keyword, username, "LinkedIn Job")
                    if note:
                        try:
                            page.goto(post_url, timeout=20000); page.wait_for_timeout(4000)
                            page.wait_for_selector("button[aria-label*='Connect']", timeout=8000).click(); _human_delay()
                            try:
                                page.wait_for_selector("button[aria-label*='Add a note']", timeout=5000).click(); _human_delay()
                                page.wait_for_selector("textarea[name='message'],#custom-message", timeout=8000).type(note[:300], delay=random.randint(60,120)); _human_delay()
                            except Exception: pass
                            page.wait_for_selector("button[aria-label*='Send']", timeout=8000).click()
                            page.wait_for_timeout(3000)
                            save_conversation(lead_id, "linkedin", username, "watcher", note)
                            update_lead_status(lead_id, "discovery_in_progress")
                        except Exception: update_lead_status(lead_id, "contact_failed")
                processed.append(lead_id); random_delay()
            browser.close()
    except Exception as exc:
        log.error(f"[LinkedIn-Watcher] Process: {exc}")
        log_agent_error("linkedin_watcher", "process_error", str(exc))
        return {**state, "errors": [str(exc)], "cycle_done": True}
    return {**state, "posts_processed": processed, "cycle_done": True}


# ── Discord (API) ────────────────────────────────────────────

def discord_scrape_node(state: WatcherState) -> WatcherState:
    log.info(f"[Discord-Watcher] Checking: {state['keyword']}")
    posts = []
    for server_id in DISCORD_SERVERS:
        channels = discord_get(f"/guilds/{server_id}/channels") or []
        for ch in [c for c in channels if c.get("type")==0
                   and (not DISCORD_CHANNELS or c["id"] in DISCORD_CHANNELS)][:5]:
            msgs    = discord_get(f"/channels/{ch['id']}/messages", {"limit": 50}) or []
            last_id = get_watcher_meta(f"discord_last_{ch['id']}")
            for msg in reversed(msgs):
                if last_id and msg["id"] <= last_id: continue
                content = msg.get("content","").strip()
                author  = msg.get("author",{})
                if not content or author.get("bot"): continue
                if any(kw.lower() in content.lower() for kw in WATCHER_KEYWORDS):
                    posts.append({"text":content,"username":author.get("username","unknown"),
                                  "user_id":author.get("id",""),"channel_id":ch["id"],
                                  "message_id":msg["id"],"server_id":server_id,
                                  "keyword":state["keyword"],"platform":"discord"})
            if msgs: set_watcher_meta(f"discord_last_{ch['id']}", msgs[0]["id"])
            time.sleep(1)
    return {**state, "posts_found": posts}


def discord_process_node(state: WatcherState) -> WatcherState:
    processed = []
    for post in state["posts_found"]:
        username = post["username"]; user_id = post["user_id"]
        post_text = post["text"];   keyword = post["keyword"]
        channel_id = post["channel_id"]; message_id = post["message_id"]
        lead_id = _make_lead_id("discord", username, post["server_id"])
        if is_already_processed(lead_id): continue
        q = qualify_content(post_text, keyword, "Discord")
        if q == "SKIP": continue
        if q == "OOS":
            save_oos_temp(lead_id, "discord", f"channel:{channel_id}", post_text, username, "message")
            notify_manager("discord_watcher", "oos_decision_request",
                           {"lead_id": lead_id, "post": post_text[:300]}, lead_id)
            continue
        first_msg = generate_first_dm(post_text, keyword, username, "Discord")
        if not first_msg: continue
        save_lead(lead_id, username, "discord", "channel_search", post_text, "approached")
        dm_ok = discord_send_dm(user_id, first_msg) if user_id else False
        if not dm_ok:
            comment = generate_first_comment(post_text, keyword, "Discord")
            if comment: dm_ok = discord_send_channel(channel_id, comment, reply_to=message_id)
        if dm_ok:
            save_conversation(lead_id, "discord", username, "watcher", first_msg)
            update_lead_status(lead_id, "discovery_in_progress")
        else:
            update_lead_status(lead_id, "contact_failed")
        processed.append(lead_id)
        time.sleep(random.uniform(3, 8))
    return {**state, "posts_processed": processed, "cycle_done": True}


# ============================================================
# ══════════════════════════════════════════════════════════
#  REPLIER NODES
# ══════════════════════════════════════════════════════════
# ============================================================

def fb_replier_node(state: ReplierState) -> ReplierState:
    log.info("[FB-Replier] Checking inbox")
    sent = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw, os.environ.get("FB_COOKIES",""))
            page = ctx.new_page()
            page.goto("https://www.facebook.com/messages/", timeout=25000)
            page.wait_for_timeout(5000)
            for thread in page.query_selector_all("[role='row']")[:10]:
                try:
                    name_el = thread.query_selector("span[dir='auto']")
                    if not name_el: continue
                    username = name_el.inner_text().strip()
                    lead_id  = get_lead_id_by_username(username, "facebook")
                    if not lead_id or already_replied_to_latest(lead_id): continue
                    conv = get_conversation(lead_id)
                    if not conv: continue
                    thread.click(); page.wait_for_timeout(3000)
                    msg_els = page.query_selector_all("[data-scope='messages_table'] [dir='auto']")
                    if not msg_els: continue
                    latest_text = msg_els[-1].inner_text().strip()
                    if not latest_text: continue
                    reply = generate_reply(conv["messages"], latest_text, username, "Facebook", "DM")
                    if not reply: continue
                    box = page.wait_for_selector("[contenteditable='true'][role='textbox']", timeout=8000)
                    box.click(); _human_delay()
                    box.type(reply, delay=random.randint(60, 130)); _human_delay()
                    page.keyboard.press("Enter"); page.wait_for_timeout(2000)
                    save_incoming_message(lead_id, "facebook", username, latest_text)
                    save_our_reply(lead_id, "facebook", username, reply)
                    update_lead_status(lead_id, "in_conversation")
                    sent.append(lead_id); random_delay(5, 12)
                except Exception: continue
            browser.close()
    except Exception as exc:
        log.error(f"[FB-Replier]: {exc}")
        log_agent_error("fb_replier", "replier_error", str(exc))
        return {**state, "errors": [str(exc)], "cycle_done": True}
    return {**state, "replies_sent": sent, "cycle_done": True}


def reddit_replier_node(state: ReplierState) -> ReplierState:
    log.info("[Reddit-Replier] Checking inbox")
    sent = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw, os.environ.get("REDDIT_COOKIES",""))
            page = ctx.new_page()
            page.goto("https://www.reddit.com/message/inbox/", timeout=20000)
            page.wait_for_timeout(4000)
            for msg_el in page.query_selector_all(".thing.message")[:15]:
                try:
                    author_el = msg_el.query_selector(".author")
                    if not author_el: continue
                    username = author_el.inner_text().strip()
                    lead_id  = get_lead_id_by_username(username, "reddit")
                    if not lead_id or already_replied_to_latest(lead_id): continue
                    conv = get_conversation(lead_id)
                    if not conv: continue
                    body_el = msg_el.query_selector(".md")
                    if not body_el: continue
                    latest_text = body_el.inner_text().strip()
                    reply = generate_reply(conv["messages"], latest_text, username, "Reddit", "DM")
                    if not reply: continue
                    reply_btn = msg_el.query_selector("a.reply-button,button.reply")
                    if not reply_btn: continue
                    reply_btn.click(); _human_delay()
                    msg_el.wait_for_selector("textarea", timeout=8000).type(reply, delay=random.randint(60,130))
                    _human_delay(); msg_el.query_selector("button[type='submit']").click()
                    page.wait_for_timeout(3000)
                    save_incoming_message(lead_id, "reddit", username, latest_text)
                    save_our_reply(lead_id, "reddit", username, reply)
                    update_lead_status(lead_id, "in_conversation")
                    sent.append(lead_id); random_delay(5, 12)
                except Exception: continue
            browser.close()
    except Exception as exc:
        log.error(f"[Reddit-Replier]: {exc}")
        log_agent_error("reddit_replier", "replier_error", str(exc))
        return {**state, "errors": [str(exc)], "cycle_done": True}
    return {**state, "replies_sent": sent, "cycle_done": True}


def linkedin_replier_node(state: ReplierState) -> ReplierState:
    log.info("[LinkedIn-Replier] Checking notifications")
    sent = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw, os.environ.get("LINKEDIN_COOKIES",""))
            page = ctx.new_page()
            page.goto("https://www.linkedin.com/notifications/", timeout=25000)
            page.wait_for_timeout(5000)
            for notif in page.query_selector_all(".nt-card-list__item")[:15]:
                try:
                    text = notif.inner_text().strip()
                    if "commented" not in text.lower() and "replied" not in text.lower(): continue
                    try: author = notif.query_selector(".nt-card__text--bold,.actor-name").inner_text().strip()
                    except Exception: continue
                    lead_id = get_lead_id_by_username(author, "linkedin")
                    if not lead_id or already_replied_to_latest(lead_id): continue
                    conv = get_conversation(lead_id)
                    if not conv: continue
                    link = notif.query_selector("a")
                    if not link: continue
                    notif_url = link.get_attribute("href")
                    page.goto(f"https://www.linkedin.com{notif_url}", timeout=20000)
                    page.wait_for_timeout(4000)
                    reply_text_found = ""
                    for cel in page.query_selector_all(".comments-comment-item"):
                        try:
                            cel_author = cel.query_selector(".comments-post-meta__name-text").inner_text().strip()
                            if cel_author.lower() == author.lower():
                                reply_text_found = cel.query_selector(".comments-comment-item__main-content").inner_text().strip()
                        except Exception: continue
                    if not reply_text_found: continue
                    reply = generate_reply(conv["messages"], reply_text_found, author, "LinkedIn", "Comment")
                    if not reply: continue
                    reply_btns = page.query_selector_all("button[aria-label*='Reply']")
                    if not reply_btns: continue
                    reply_btns[-1].click(); _human_delay()
                    page.wait_for_selector(".ql-editor[contenteditable='true']", timeout=8000).type(reply, delay=random.randint(60,130))
                    _human_delay(); page.query_selector("button.comments-comment-box__submit-button").click()
                    page.wait_for_timeout(3000)
                    save_incoming_message(lead_id, "linkedin", author, reply_text_found)
                    save_our_reply(lead_id, "linkedin", author, reply)
                    update_lead_status(lead_id, "in_conversation")
                    sent.append(lead_id); random_delay(5, 12)
                except Exception: continue
            browser.close()
    except Exception as exc:
        log.error(f"[LinkedIn-Replier]: {exc}")
        log_agent_error("linkedin_replier", "replier_error", str(exc))
        return {**state, "errors": [str(exc)], "cycle_done": True}
    return {**state, "replies_sent": sent, "cycle_done": True}


def discord_replier_node(state: ReplierState) -> ReplierState:
    log.info("[Discord-Replier] Checking DMs and mentions")
    sent = []
    try:
        my_info = discord_get("/users/@me")
        my_id   = my_info["id"] if my_info else None
        for ch in [c for c in (discord_get("/users/@me/channels") or []) if c.get("type")==1]:
            try:
                recipients = ch.get("recipients", [])
                if not recipients: continue
                username = recipients[0].get("username","unknown")
                user_id  = recipients[0].get("id","")
                lead_id  = get_lead_id_by_username(username, "discord")
                if not lead_id or already_replied_to_latest(lead_id): continue
                conv = get_conversation(lead_id)
                if not conv: continue
                lead_msgs = [m for m in (discord_get(f"/channels/{ch['id']}/messages",{"limit":10}) or [])
                             if m.get("author",{}).get("id") != my_id and m.get("content","").strip()]
                if not lead_msgs: continue
                reply = generate_reply(conv["messages"], lead_msgs[0]["content"].strip(), username, "Discord", "DM")
                if not reply: continue
                if discord_send_dm(user_id, reply):
                    save_incoming_message(lead_id, "discord", username, lead_msgs[0]["content"])
                    save_our_reply(lead_id, "discord", username, reply)
                    update_lead_status(lead_id, "in_conversation")
                    sent.append(lead_id)
                time.sleep(random.uniform(4, 10))
            except Exception: continue
        for mention in (discord_get("/users/@me/mentions",{"limit":25}) or []):
            try:
                author  = mention.get("author",{})
                if author.get("id") == my_id: continue
                username = author.get("username","unknown")
                text     = mention.get("content","").strip()
                msg_id   = mention.get("id",""); ch_id = mention.get("channel_id","")
                user_id  = author.get("id","")
                if not text or not username: continue
                lead_id = get_lead_id_by_username(username, "discord")
                if not lead_id or already_replied_to_latest(lead_id): continue
                conv = get_conversation(lead_id)
                if not conv: continue
                reply = generate_reply(conv["messages"], text, username, "Discord", "Channel")
                if not reply: continue
                dm_ok = discord_send_dm(user_id, reply) if user_id else False
                if not dm_ok and ch_id: dm_ok = discord_send_channel(ch_id, reply, reply_to=msg_id)
                if dm_ok:
                    save_incoming_message(lead_id, "discord", username, text)
                    save_our_reply(lead_id, "discord", username, reply)
                    update_lead_status(lead_id, "in_conversation"); sent.append(lead_id)
                time.sleep(random.uniform(4, 10))
            except Exception: continue
    except Exception as exc:
        log.error(f"[Discord-Replier]: {exc}")
        log_agent_error("discord_replier", "replier_error", str(exc))
        return {**state, "errors": [str(exc)], "cycle_done": True}
    return {**state, "replies_sent": sent, "cycle_done": True}


# ============================================================
# ══════════════════════════════════════════════════════════
#  EXECUTOR — Real Tool-Equipped Agentic Loop
#
#  LLM decides WHAT to do step by step.
#  Agent executes REAL tools:
#    browser_action → Playwright on Browserless
#    run_code       → subprocess (python/node/bash)
#    github_push    → PyGithub → Northflank auto-deploy
#    api_call       → HTTP requests to any platform API
#    write_file     → create files for deliverables
#    read_url       → fetch any URL content
# ══════════════════════════════════════════════════════════
# ============================================================

TOOL_SCHEMAS = """
Available tools — call ONE per step, respond in JSON only:

1. browser_action   args: url, instruction
2. run_code         args: code, lang (python|node|bash)
3. github_push      args: file_path, content, commit_msg
4. api_call         args: method, url, headers, body
5. write_file       args: file_path, content
6. read_url         args: url

When finished:  {"tool":"done","result":"summary","test_command":"bash cmd to verify"}
When blocked:   {"tool":"blocked","reason":"what is missing"}
"""


def _tool_browser_action(project_id, url, instruction, **_) -> dict:
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_stealth_context(pw)
            page = ctx.new_page()
            page.goto(url, timeout=30000); page.wait_for_timeout(3000)
            page_text = page.inner_text("body")[:3000]
            action_plan = call_llm([{"role":"user","content":(
                f"Task: {instruction}\nURL: {url}\nPage:\n{page_text}\n\n"
                "Write Playwright Python actions (page.* only, no imports). "
                "Variables available: page, time, random"
            )}], max_tokens=500)
            result = f"Loaded {url}"
            if action_plan:
                try:
                    _safe_globals = {
                        "page": page,
                        "time": time,
                        "random": random,
                        "__builtins__": {
                            "print": print,
                            "range": range,
                            "len": len,
                            "str": str,
                            "int": int,
                            "list": list,
                            "dict": dict,
                            "True": True,
                            "False": False,
                            "None": None,
                        },
                    }
                    exec(action_plan, _safe_globals)
                    result = f"Executed: {action_plan[:200]}"
                except Exception as e:
                    result = f"Action error: {e}"
            shot = f"/tmp/exec_{project_id}_{int(time.time())}.png"
            page.screenshot(path=shot); browser.close()
        save_tool_log(project_id, "browser_action", result, True)
        return {"success": True, "result": result, "screenshot": shot}
    except Exception as exc:
        save_tool_log(project_id, "browser_action", str(exc), False)
        return {"success": False, "result": str(exc)}


def _tool_run_code(project_id, code, lang="python", **_) -> dict:
    try:
        suffix = {"python":".py","node":".js","bash":".sh"}.get(lang,".py")
        with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as f:
            f.write(code); fpath = f.name
        cmd = {"python":["python3",fpath],"node":["node",fpath],"bash":["bash",fpath]}.get(lang,["python3",fpath])
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        ok  = r.returncode == 0
        out = r.stdout[:2000] if ok else r.stderr[:2000]
        save_tool_log(project_id, f"run_code:{lang}", out, ok)
        return {"success": ok, "stdout": r.stdout[:2000], "stderr": r.stderr[:1000]}
    except subprocess.TimeoutExpired:
        save_tool_log(project_id, f"run_code:{lang}", "Timeout 60s", False)
        return {"success": False, "stdout": "", "stderr": "Timeout 60s"}
    except Exception as exc:
        save_tool_log(project_id, f"run_code:{lang}", str(exc), False)
        return {"success": False, "stdout": "", "stderr": str(exc)}


def _tool_github_push(project_id, file_path, content, commit_msg, **_) -> dict:
    try:
        from github import Github
        repo = Github(GITHUB_TOKEN).get_repo(GITHUB_REPO)
        try:
            ex = repo.get_contents(file_path)
            repo.update_file(file_path, commit_msg, content, ex.sha)
        except Exception:
            repo.create_file(file_path, commit_msg, content)
        result = f"Pushed {file_path} → {GITHUB_REPO} (Northflank deploys automatically)"
        save_tool_log(project_id, "github_push", result, True)
        return {"success": True, "result": result}
    except Exception as exc:
        save_tool_log(project_id, "github_push", str(exc), False)
        return {"success": False, "result": str(exc)}


def _tool_api_call(project_id, method, url, headers=None, body=None, **_) -> dict:
    try:
        r = requests.request(method.upper(), url, headers=headers or {}, json=body, timeout=30)
        ok = r.status_code < 400
        save_tool_log(project_id, f"api_call:{method}:{url[:60]}", f"{r.status_code}", ok)
        return {"success": ok, "status": r.status_code, "body": r.text[:2000]}
    except Exception as exc:
        save_tool_log(project_id, "api_call", str(exc), False)
        return {"success": False, "status": 0, "body": str(exc)}


def _tool_write_file(project_id, file_path, content, **_) -> dict:
    try:
        os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
        with open(file_path, "w") as f:
            f.write(content)
        result = f"Written {file_path} ({len(content)} chars)"
        save_tool_log(project_id, "write_file", result, True)
        return {"success": True, "result": result, "path": file_path}
    except Exception as exc:
        save_tool_log(project_id, "write_file", str(exc), False)
        return {"success": False, "result": str(exc)}


def _tool_read_url(project_id, url, **_) -> dict:
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent":"Mozilla/5.0"})
        content = r.text[:5000]
        save_tool_log(project_id, f"read_url:{url[:60]}", f"{len(content)} chars", True)
        return {"success": True, "content": content, "status": r.status_code}
    except Exception as exc:
        save_tool_log(project_id, "read_url", str(exc), False)
        return {"success": False, "content": str(exc)}


EXECUTOR_TOOLS = {
    "browser_action": _tool_browser_action,
    "run_code":       _tool_run_code,
    "github_push":    _tool_github_push,
    "api_call":       _tool_api_call,
    "write_file":     _tool_write_file,
    "read_url":       _tool_read_url,
}


# ── Executor Graph Nodes ────────────────────────────────────

def executor_receive_node(state: ExecutorState) -> ExecutorState:
    pid     = state.get("project_id", "")
    project = get_pending_project(state["executor_type"], project_id=pid)
    if not project:
        log.info(f"[{state['executor_type']}-Executor] No pending projects")
        return {**state, "done": True}
    update_project_status(project["project_id"], "in_progress")
    return {**state, "project_id": project["project_id"],
            "project_data": project, "tool_log": ["Project received"]}


def _route_after_receive(state: ExecutorState) -> Literal["plan", END]:
    return END if state.get("done") else "plan"


def executor_plan_node(state: ExecutorState) -> ExecutorState:
    if state.get("done"): return state
    plan = call_llm([{"role":"user","content":(
        f"You are the {state['executor_type']} executor agent.\n"
        f"Project:\n{json.dumps(state['project_data'],indent=2)}\n\n"
        f"{TOOL_SCHEMAS}\n\n"
        "Create a numbered step-by-step plan. Each step = one tool call. Be specific."
    )}], max_tokens=800)
    if not plan:
        return {**state, "errors": ["Plan generation failed"], "done": True}
    return {**state, "tool_log": [f"PLAN:\n{plan}"]}


def _route_after_plan(state: ExecutorState) -> Literal["execute", END]:
    return END if state.get("done") else "execute"


def executor_execute_node(state: ExecutorState) -> ExecutorState:
    """
    Real agentic tool loop:
    LLM picks tool → agent executes → result fed back → repeat (max 15 steps)
    """
    if state.get("done"): return state
    project    = state["project_data"]
    project_id = state["project_id"]
    exec_type  = state["executor_type"]
    tool_log   = list(state.get("tool_log", []))

    messages = [
        {"role": "system", "content": (
            f"You are the {exec_type} executor. Complete the project using tools. "
            f"Always respond in JSON.\n\n{TOOL_SCHEMAS}"
        )},
        {"role": "user", "content": (
            f"Project:\n{json.dumps(project,indent=2)}\n\n"
            f"Log so far:\n{chr(10).join(tool_log[-5:])}\n\n"
            "Next tool call? JSON only."
        )}
    ]

    for step in range(15):
        response = call_llm(messages, max_tokens=500)
        if not response:
            tool_log.append(f"Step {step+1}: LLM failed"); break

        try:
            clean = response.strip()
            if "```" in clean:
                clean = clean.split("```")[1].replace("json","").strip()
            call = json.loads(clean)
        except Exception:
            messages.append({"role":"assistant","content":response})
            messages.append({"role":"user","content":"Invalid JSON. Respond in JSON only."})
            continue

        tool_name = call.get("tool","")
        args      = call.get("args", {})

        if tool_name == "done":
            test_cmd = call.get("test_command","")
            tool_log.append(f"✅ DONE: {call.get('result','Completed')}")
            if test_cmd: tool_log.append(f"Test command: {test_cmd}")
            return {**state, "tool_log": tool_log, "test_passed": False}  # go to test node

        if tool_name == "blocked":
            reason = call.get("reason","Unknown")
            tool_log.append(f"🚫 BLOCKED: {reason}")
            notify_manager(f"{exec_type}_executor", "missing_info",
                           {"project_id": project_id, "detail": reason}, project_id, "project")
            update_project_status(project_id, "blocked")
            return {**state, "tool_log": tool_log, "errors": [f"Blocked: {reason}"], "done": True}

        if tool_name not in EXECUTOR_TOOLS:
            messages.append({"role":"assistant","content":response})
            messages.append({"role":"user","content":f"Unknown tool. Use: {list(EXECUTOR_TOOLS.keys())}"})
            continue

        log.info(f"[{exec_type}-Executor] Step {step+1}: {tool_name}")
        result = EXECUTOR_TOOLS[tool_name](project_id=project_id, **args)
        entry  = f"Step {step+1} [{tool_name}]: {'✅' if result.get('success') else '❌'} {str(result)[:300]}"
        tool_log.append(entry)
        log.info(f"[{exec_type}-Executor] {entry}")

        messages.append({"role":"assistant","content":response})
        messages.append({"role":"user","content":(
            f"Tool result:\n{json.dumps(result)}\n\nNext step? JSON only."
        )})

    tool_log.append("⚠️ Max steps reached")
    return {**state, "tool_log": tool_log}


def _route_after_execute(state: ExecutorState) -> Literal["test", END]:
    return END if (state.get("done") or state.get("errors")) else "test"


def executor_test_node(state: ExecutorState) -> ExecutorState:
    if state.get("done") or state.get("errors"): return state
    tool_log = list(state.get("tool_log", []))
    test_cmd = ""
    for entry in reversed(tool_log):
        if "Test command:" in entry:
            test_cmd = entry.replace("Test command:","").strip(); break

    if test_cmd:
        r = _tool_run_code(state["project_id"], test_cmd, "bash")
        passed = r["success"]
        tool_log.append(f"TEST: {'✅ PASSED' if passed else '❌ FAILED'} — {(r['stdout'] or r['stderr'])[:300]}")
    else:
        verify = call_llm([{"role":"user","content":(
            f"Executor: {state['executor_type']}\n"
            f"Project: {json.dumps(state['project_data'],indent=2)}\n"
            f"Log:\n{chr(10).join(tool_log[-8:])}\n\n"
            "Was the project completed successfully? Reply: TEST_PASSED or TEST_FAILED + one sentence."
        )}], max_tokens=100)
        passed = bool(verify and "TEST_PASSED" in verify.upper())
        tool_log.append(f"TEST: {'✅ PASSED' if passed else '❌ FAILED'} — {(verify or '')[:200]}")

    return {**state, "test_passed": passed,
            "test_attempts": state.get("test_attempts",0)+1, "tool_log": tool_log}


def _route_after_test(state: ExecutorState) -> Literal["deploy","execute",END]:
    if state.get("test_passed"): return "deploy"
    attempts = state.get("test_attempts", 1)
    if attempts < 3:
        log.warning(f"[{state['executor_type']}-Executor] Test failed — retry {attempts}/3")
        return "execute"
    project_id = state["project_id"]; exec_type = state["executor_type"]
    notify_manager(f"{exec_type}_executor","error_flag",
                   {"project_id":project_id,"detail":"3 test failures — manual review"},
                   project_id,"project")
    update_project_status(project_id,"blocked")
    log_agent_error(f"{exec_type}_executor","test_failure","3 consecutive failures",3)
    return END


def executor_deploy_node(state: ExecutorState) -> ExecutorState:
    if state.get("done"): return state
    project_id = state["project_id"]; project = state["project_data"]
    exec_type  = state["executor_type"]; tool_log = list(state.get("tool_log",[]))

    # Final deploy action if needed
    deploy_response = call_llm([{"role":"user","content":(
        f"Executor: {exec_type}\nProject: {json.dumps(project,indent=2)}\n"
        f"Log:\n{chr(10).join(tool_log[-5:])}\n\n{TOOL_SCHEMAS}\n\n"
        "Any final deployment step? If yes return tool JSON. If no: "
        '{"tool":"done","result":"Already deployed"}'
    )}], max_tokens=300)

    if deploy_response:
        try:
            clean = deploy_response.strip()
            if "```" in clean: clean = clean.split("```")[1].replace("json","").strip()
            dc = json.loads(clean)
            if dc.get("tool") not in ("done", None) and dc["tool"] in EXECUTOR_TOOLS:
                r = EXECUTOR_TOOLS[dc["tool"]](project_id=project_id, **dc.get("args",{}))
                tool_log.append(f"DEPLOY [{dc['tool']}]: {str(r)[:300]}")
        except Exception:
            pass

    update_project_status(project_id, "complete")
    notify_manager(f"{exec_type}_executor","executor_report",{
        "project_id":project_id,"client":project.get("client_name"),
        "status":"complete","deploy_note":"\n".join(tool_log[-3:])
    }, project_id,"project")
    log.info(f"[{exec_type}-Executor] {project_id} complete ✓")
    return {**state, "deployed":True, "tool_log":tool_log, "done":True}


# ============================================================
# LANGGRAPH GRAPH BUILDERS
# ============================================================

_CHECKPOINTER = PostgresSaver.from_conn_string(DATABASE_URL)
try:
    _CHECKPOINTER.setup()   # Creates checkpoint tables on first run (idempotent)
except Exception as _ce:
    log.warning(f"[Checkpointer] setup() skipped (may already exist): {_ce}")


def build_watcher_graph(scrape_fn, process_fn, name):
    g = StateGraph(WatcherState)
    g.add_node("scrape", scrape_fn); g.add_node("process", process_fn)
    g.set_entry_point("scrape"); g.add_edge("scrape","process"); g.add_edge("process",END)
    return g.compile(checkpointer=_CHECKPOINTER)


def build_replier_graph(replier_fn, name):
    g = StateGraph(ReplierState)
    g.add_node("reply", replier_fn)
    g.set_entry_point("reply"); g.add_edge("reply", END)
    return g.compile(checkpointer=_CHECKPOINTER)


def build_executor_graph(executor_type):
    g = StateGraph(ExecutorState)
    g.add_node("receive", executor_receive_node)
    g.add_node("plan",    executor_plan_node)
    g.add_node("execute", executor_execute_node)
    g.add_node("test",    executor_test_node)
    g.add_node("deploy",  executor_deploy_node)
    g.set_entry_point("receive")
    g.add_conditional_edges("receive", _route_after_receive, {"plan":"plan", END:END})
    g.add_conditional_edges("plan",    _route_after_plan,    {"execute":"execute", END:END})
    g.add_conditional_edges("execute", _route_after_execute, {"test":"test", END:END})
    g.add_conditional_edges("test",    _route_after_test,    {"deploy":"deploy","execute":"execute",END:END})
    g.add_edge("deploy", END)
    return g.compile(checkpointer=_CHECKPOINTER)


GRAPHS = {
    "fb_watcher":           build_watcher_graph(fb_scrape_node, fb_process_node, "fb"),
    "reddit_watcher":       build_watcher_graph(reddit_scrape_node, reddit_process_node, "reddit"),
    "linkedin_watcher":     build_watcher_graph(linkedin_scrape_node, linkedin_process_node, "linkedin"),
    "discord_watcher":      build_watcher_graph(discord_scrape_node, discord_process_node, "discord"),
    "fb_replier":           build_replier_graph(fb_replier_node, "fb"),
    "reddit_replier":       build_replier_graph(reddit_replier_node, "reddit"),
    "linkedin_replier":     build_replier_graph(linkedin_replier_node, "linkedin"),
    "discord_replier":      build_replier_graph(discord_replier_node, "discord"),
    "workflow_executor":    build_executor_graph("workflow"),
    "leadgen_executor":     build_executor_graph("leadgen"),
    "chatbot_executor":     build_executor_graph("chatbot"),
    "socialmedia_executor": build_executor_graph("socialmedia"),
}


# ============================================================
# RUN HELPERS
# ============================================================

def run_watcher(graph_key, keyword):
    thread_id = f"{graph_key}_{keyword.replace(' ','_')}"
    try:
        GRAPHS[graph_key].invoke({
            "platform": graph_key.split("_")[0], "keyword": keyword,
            "posts_found": [], "posts_processed": [],
            "errors": [], "cycle_done": False, "retry_count": 0,
        }, {"configurable": {"thread_id": thread_id}})
    except Exception as exc:
        log.error(f"[{graph_key}] {exc}"); log_agent_error(graph_key, "crash", str(exc))


def run_replier(graph_key):
    thread_id = f"{graph_key}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}"
    try:
        GRAPHS[graph_key].invoke({
            "platform": graph_key.split("_")[0], "inbox_items": [],
            "replies_sent": [], "errors": [], "cycle_done": False,
        }, {"configurable": {"thread_id": thread_id}})
    except Exception as exc:
        log.error(f"[{graph_key}] {exc}"); log_agent_error(graph_key, "crash", str(exc))


def run_executor(executor_type, project_id=""):
    graph_key = f"{executor_type}_executor"
    thread_id = f"{graph_key}_{project_id}" if project_id else f"{graph_key}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    try:
        GRAPHS[graph_key].invoke({
            "project_id": project_id, "executor_type": executor_type,
            "project_data": {}, "tool_log": [],
            "test_passed": False, "test_attempts": 0,
            "deployed": False, "errors": [], "done": False,
        }, {"configurable": {"thread_id": thread_id}})
    except Exception as exc:
        log.error(f"[{graph_key}] {exc}"); log_agent_error(graph_key, "crash", str(exc))


# ============================================================
# CRON — Bounded thread pools
# ============================================================

_WATCHER_POOL  = ThreadPoolExecutor(max_workers=8, thread_name_prefix="watcher")
_REPLIER_POOL  = ThreadPoolExecutor(max_workers=4, thread_name_prefix="replier")
_EXECUTOR_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="executor")


def watcher_cycle():
    log.info("━━━━ Watcher Cycle Start ━━━━")
    futures = []
    for kw in WATCHER_KEYWORDS:
        for w in ["fb_watcher","reddit_watcher","linkedin_watcher","discord_watcher"]:
            futures.append(_WATCHER_POOL.submit(run_watcher, w, kw))
        time.sleep(random.uniform(30, 60))
    for f in as_completed(futures):
        exc = f.exception()
        if exc: log.error(f"Watcher error: {exc}")
    log.info("━━━━ Watcher Cycle Done ━━━━")


def replier_cycle():
    log.info("━━━━ Replier Cycle Start ━━━━")
    futures = [_REPLIER_POOL.submit(run_replier, r)
               for r in ["fb_replier","reddit_replier","linkedin_replier","discord_replier"]]
    for f in as_completed(futures):
        exc = f.exception()
        if exc: log.error(f"Replier error: {exc}")
    log.info("━━━━ Replier Cycle Done ━━━━")


# ============================================================
# NOTIFY LISTENER — Executors (select.select, auto-reconnect)
# ============================================================

def _make_notify_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute("LISTEN executor_channel")
    return conn


def executor_notify_listener():
    log.info("Executor NOTIFY listener started")
    backoff = 5; conn = None
    while True:
        try:
            if conn is None:
                conn = _make_notify_conn(); backoff = 5
                log.info("NOTIFY listener connected")
            r, _, _ = select.select([conn], [], [], 60)
            if not r:
                try:
                    with conn.cursor() as _ka:
                        _ka.execute("SELECT 1")   # keepalive ping
                except Exception:
                    raise
                continue
            conn.poll()
            while conn.notifies:
                payload = conn.notifies.pop(0).payload.strip()
                log.info(f"NOTIFY: '{payload}'")
                exec_type, project_id = (payload.split(":",1) + [""])[:2]
                if exec_type.strip() in {"workflow","leadgen","chatbot","socialmedia"}:
                    _EXECUTOR_POOL.submit(run_executor, exec_type.strip(), project_id.strip())
        except Exception as exc:
            log.error(f"NOTIFY error: {exc} — reconnect in {backoff}s")
            try:
                if conn: conn.close()
            except Exception: pass
            conn = None; time.sleep(backoff); backoff = min(backoff*2, 120)


# ============================================================
# MAIN
# ============================================================

def _validate_env():
    """Fail fast on startup if critical env vars are missing."""
    required = ["OPENAI_API_KEY", "DATABASE_URL"]
    optional_warn = ["DISCORD_TOKEN", "GITHUB_TOKEN", "GITHUB_REPO",
                     "BROWSERLESS_URL", "TOR_PROXY_URL"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            f"[Startup] Missing required environment variables: {', '.join(missing)}"
        )
    for k in optional_warn:
        if not os.environ.get(k):
            log.warning(f"[Startup] Optional env var not set: {k}")
    log.info("[Startup] Environment validation passed ✓")


def main():
    log.info("══════════════════════════════════════")
    log.info("  Agency — Watchers + Repliers + Executors")
    log.info("══════════════════════════════════════")

    _validate_env()
    threading.Thread(target=executor_notify_listener, daemon=True, name="notify-listener").start()
    schedule.every(3).hours.do(watcher_cycle)
    schedule.every(30).minutes.do(replier_cycle)
    watcher_cycle(); replier_cycle()
    while True:
        schedule.run_pending(); time.sleep(30)


if __name__ == "__main__":
    main()
