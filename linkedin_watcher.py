import os
import json
import time
import logging
import psycopg2
from openai import OpenAI

# ============================================================
# CONFIG
# ============================================================

OPENAI_MODEL = "ft:gpt-4o-mini-2024-07-18:personal:final-brain-1:DREfTesR"

SYSTEM_PROMPT = (
    "You are the Analyzer Agent for the agency (the agency contact number). "
    "You receive project briefs from Manager. Your job: analyze the brief, "
    "check website if provided, identify missing info, build a complete project summary, "
    "and hand back to Manager for verification. For workflow projects, identify modules "
    "and what will be needed. If anything is unclear, ask Manager. Never contact client "
    "directly. Self-log only for: recurring issues (2+ times), blockers, improvement "
    "suggestions. Not for normal operations. "
    "Services we offer: AI agents, automation, chatbots, workflows, lead gen, social media/content."
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Analyzer] %(message)s"
)
log = logging.getLogger(__name__)

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])


# ============================================================
# DATABASE
# ============================================================

def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def fetch_pending_messages():
    """Fetch all unprocessed messages addressed to analyzer."""
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT id, from_agent, message_type, payload, related_id
        FROM agent_messages
        WHERE to_agent  = 'analyzer'
          AND processed = FALSE
        ORDER BY created_at ASC
    """)
    rows = cur.fetchall()
    cur.close()
    db.close()
    return rows


def mark_processed(message_id: int):
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "UPDATE agent_messages SET processed = TRUE, processed_at = NOW() WHERE id = %s",
        (message_id,)
    )
    db.commit()
    cur.close()
    db.close()


def get_conversation_history(related_id: str):
    """Load existing conversation turns for multi-turn briefs (e.g. missing info → update)."""
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "SELECT messages FROM analyzer_conversations WHERE related_id = %s",
        (related_id,)
    )
    row = cur.fetchone()
    cur.close()
    db.close()
    return json.loads(row[0]) if row else []


def save_conversation_history(related_id: str, messages: list):
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO analyzer_conversations (related_id, messages, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (related_id) DO UPDATE
            SET messages   = EXCLUDED.messages,
                updated_at = NOW()
    """, (related_id, json.dumps(messages)))
    db.commit()
    cur.close()
    db.close()


def save_project(project_id: str, brief_raw: str, status: str):
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO projects
            (project_id, brief_raw, status, created_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (project_id) DO UPDATE
            SET status     = EXCLUDED.status,
                updated_at = NOW()
    """, (project_id, brief_raw[:2000], status))
    db.commit()
    cur.close()
    db.close()


def save_project_summary(project_id: str, summary_text: str, executor: str):
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO project_summaries
            (project_id, summary_text, executor, created_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (project_id) DO UPDATE
            SET summary_text = EXCLUDED.summary_text,
                executor     = EXCLUDED.executor,
                updated_at   = NOW()
    """, (project_id, summary_text, executor))
    db.commit()
    cur.close()
    db.close()


def save_missing_info(project_id: str, questions_text: str):
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO missing_info_requests
            (project_id, questions, status, created_at)
        VALUES (%s, %s, 'pending', NOW())
        ON CONFLICT (project_id) DO UPDATE
            SET questions  = EXCLUDED.questions,
                status     = 'pending',
                updated_at = NOW()
    """, (project_id, questions_text))
    db.commit()
    cur.close()
    db.close()


def save_oos_result(check_id: str, signal: str, reason: str,
                    executor: str = None, estimated_time: str = None):
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO oos_checks
            (check_id, signal, reason, executor, estimated_time, checked_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (check_id) DO NOTHING
    """, (check_id, signal, reason, executor, estimated_time))
    db.commit()
    cur.close()
    db.close()


def save_self_log(category: str, description: str, suggestion: str):
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO analyzer_self_logs
            (category, description, suggestion, logged_at)
        VALUES (%s, %s, %s, NOW())
    """, (category, description, suggestion))
    db.commit()
    cur.close()
    db.close()


def notify_manager(message_type: str, payload: dict, related_id: str):
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO agent_messages
            (from_agent, to_agent, message_type, payload,
             related_id, related_type, processed, created_at)
        VALUES ('analyzer', 'manager', %s, %s, %s, 'project', FALSE, NOW())
    """, (message_type, json.dumps(payload), related_id))
    db.commit()
    cur.close()
    db.close()


def notify_watcher(check_id: str, signal: str, reason: str,
                   executor: str = None, estimated_time: str = None):
    payload = {
        "check_id":       check_id,
        "signal":         signal,
        "reason":         reason,
        "executor":       executor,
        "estimated_time": estimated_time,
    }
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO agent_messages
            (from_agent, to_agent, message_type, payload,
             related_id, related_type, processed, created_at)
        VALUES ('analyzer', 'watcher', 'oos_signal', %s, %s, 'oos_check', FALSE, NOW())
    """, (json.dumps(payload), check_id))
    db.commit()
    cur.close()
    db.close()


# ============================================================
# OPENAI — CALL FINE-TUNED MODEL
# ============================================================

def call_analyzer(messages: list) -> str:
    """Send full conversation history to fine-tuned model, return response text."""
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        temperature=0.3,
        max_tokens=1500,
    )
    return response.choices[0].message.content.strip()


# ============================================================
# RESPONSE PARSER
# ============================================================

def parse_response(response_text: str) -> dict:
    """
    Extract structured fields from model response.
    Model always ends with STATUS / EXECUTOR / STORE / NEXT lines.
    """
    result = {
        "status":     None,
        "executor":   None,
        "project_id": None,
        "check_id":   None,
        "signal":     None,
        "raw":        response_text,
    }

    for line in response_text.splitlines():
        line = line.strip()

        if line.startswith("STATUS:"):
            result["status"] = line.replace("STATUS:", "").strip().lower()

        elif line.startswith("EXECUTOR:"):
            result["executor"] = line.replace("EXECUTOR:", "").strip().lower()

        elif line.startswith("STORE:"):
            store_part = line.replace("STORE:", "").strip()
            for segment in store_part.split(","):
                segment = segment.strip()
                if "=" in segment:
                    key, _, val = segment.partition("=")
                    key = key.strip().lower()
                    val = val.strip()
                    if key == "project_id":
                        result["project_id"] = val
                    elif key == "check_id":
                        result["check_id"] = val
                    elif key == "signal":
                        result["signal"] = val.upper()
                    elif key == "estimated_time":
                        result["estimated_time"] = val

    return result


# ============================================================
# ROUTING — act on model response
# ============================================================

def route_result(parsed: dict, brief_text: str, related_id: str):
    status     = parsed.get("status") or ""
    project_id = parsed.get("project_id") or related_id
    executor   = parsed.get("executor") or "unknown"
    summary    = parsed["raw"]

    # ── Summary ready ─────────────────────────────────────────
    if "summary_ready" in status:
        save_project(project_id, brief_text, "summary_ready")
        save_project_summary(project_id, summary, executor)
        notify_manager(
            message_type="summary_ready",
            payload={"project_id": project_id, "executor": executor, "summary": summary},
            related_id=project_id,
        )
        log.info(f"Summary ready — project_id={project_id} executor={executor}")

    # ── Missing info ──────────────────────────────────────────
    elif "missing_info" in status:
        save_project(project_id, brief_text, "missing_info")
        save_missing_info(project_id, summary)
        notify_manager(
            message_type="missing_info",
            payload={"project_id": project_id, "questions": summary},
            related_id=project_id,
        )
        log.info(f"Missing info flagged — project_id={project_id}")

    # ── Budget flag ───────────────────────────────────────────
    elif "budget_flag" in status:
        save_project(project_id, brief_text, "budget_flag")
        notify_manager(
            message_type="budget_flag",
            payload={"project_id": project_id, "details": summary},
            related_id=project_id,
        )
        log.info(f"Budget flag — project_id={project_id}")

    # ── Website error ─────────────────────────────────────────
    elif "website_error" in status:
        save_project(project_id, brief_text, "website_error")
        notify_manager(
            message_type="website_error",
            payload={"project_id": project_id, "details": summary},
            related_id=project_id,
        )
        log.info(f"Website error flagged — project_id={project_id}")

    # ── Queued (executor busy) ────────────────────────────────
    elif "queued" in status:
        save_project(project_id, brief_text, "queued")
        notify_manager(
            message_type="project_queued",
            payload={"project_id": project_id, "executor": executor, "details": summary},
            related_id=project_id,
        )
        log.info(f"Project queued — project_id={project_id}")

    # ── OOS check result ──────────────────────────────────────
    elif any(x in status for x in ("oos", "signal", "green", "red")):
        check_id       = parsed.get("check_id") or related_id
        signal         = parsed.get("signal") or ("GREEN" if "green" in status else "RED")
        estimated_time = parsed.get("estimated_time")

        save_oos_result(check_id, signal, summary, executor, estimated_time)
        notify_watcher(check_id, signal, summary, executor, estimated_time)
        log.info(f"OOS check done — check_id={check_id} signal={signal}")

    # ── Self-log / job review ─────────────────────────────────
    elif any(x in status for x in ("self_log", "report", "review")):
        save_self_log(category="job_review", description=summary, suggestion="")
        notify_manager(
            message_type="self_log_report",
            payload={"report": summary},
            related_id=related_id,
        )
        log.info("Self-log report saved and sent to Manager")

    # ── Fallback — save raw and notify ───────────────────────
    else:
        log.warning(f"Unknown status '{status}' — forwarding raw to Manager")
        notify_manager(
            message_type="analyzer_response",
            payload={"details": summary, "raw_status": status},
            related_id=related_id,
        )


# ============================================================
# PROCESS ONE MESSAGE
# ============================================================

def process_message(msg_id: int, from_agent: str, message_type: str,
                    payload: dict, related_id: str):

    log.info(f"Processing id={msg_id} type={message_type} related={related_id}")

    # Build user turn text from payload
    brief_text = (
        payload.get("brief")
        or payload.get("message")
        or payload.get("update")
        or json.dumps(payload)
    )

    # Load existing conversation (multi-turn support)
    history = get_conversation_history(related_id)

    if not history:
        # First turn — inject system prompt
        history = [{"role": "system", "content": SYSTEM_PROMPT}]

    # Add new user message
    history.append({"role": "user", "content": brief_text})

    # Call fine-tuned model
    response_text = call_analyzer(history)
    log.info(f"Response received ({len(response_text)} chars)")

    # Save updated history for next turn
    history.append({"role": "assistant", "content": response_text})
    save_conversation_history(related_id, history)

    # Parse + route
    parsed = parse_response(response_text)
    route_result(parsed, brief_text, related_id)

    # Mark done
    mark_processed(msg_id)
    log.info(f"id={msg_id} done — status={parsed.get('status')}")


# ============================================================
# MAIN LOOP
# ============================================================

def run():
    log.info("Analyzer Agent starting")

    while True:
        try:
            messages = fetch_pending_messages()

            if not messages:
                log.info("No pending messages — sleeping 30s")
                time.sleep(30)
                continue

            log.info(f"{len(messages)} message(s) found")

            for row in messages:
                msg_id, from_agent, message_type, payload_raw, related_id = row

                try:
                    payload = (
                        payload_raw
                        if isinstance(payload_raw, dict)
                        else json.loads(payload_raw)
                    )
                    process_message(msg_id, from_agent, message_type, payload, related_id)

                except Exception as e:
                    log.error(f"Error on id={msg_id}: {e}")
                    mark_processed(msg_id)  # avoid infinite retry
                    continue

                time.sleep(2)

        except Exception as e:
            log.error(f"Loop error: {e}")
            time.sleep(15)


if __name__ == "__main__":
    run()
