# ═══════════════════════════════════════════════════════
# linkedin_agent.py
# NoError Agency — LinkedIn Agent
# Watcher + Sender + Notifier — Sab Ek File
# ═══════════════════════════════════════════════════════

import os
import re
import time
import random
import asyncio
import sqlite3
from datetime import datetime, timedelta
import requests
from playwright.async_api import async_playwright

# ─────────────────────────────
# ENV VARIABLES
# ─────────────────────────────
LI_AT         = os.environ["LI_AT"]
LI_JSESSIONID = os.environ["LI_JSESSIONID"]
OPENAI_KEY    = os.environ["OPENAI_KEY"]
DB_FILE       = "agency.db"

MAX_MESSAGES_PER_RUN = 15
WATCHER_INTERVAL     = 7200   # 2 ghante (seconds)
NOTIFIER_INTERVAL    = 1800   # 30 minute (seconds)

SYSTEM_PROMPT = (
    "You are Bilal, a professional sales agent for NoError — a digital agency that works with clients globally. "
    "You are a human sales representative. "
    "If asked if you are AI say: I am part of the NoError sales team, for detailed questions I can connect you with our lead consultant directly. "
    "HARD RULES: "
    "1. NEVER share price in first or second message. "
    "2. Keep first DM under 2 sentences only. "
    "3. NEVER use markdown, bold, bullets, or any formatting. Plain text only. "
    "4. NEVER mention price unless client asks multiple times. "
    "5. When client asks for proof, use case studies naturally. "
    "TONE: Main Client = casual friendly. GoodClient = professional. Opportunity = formal corporate zero emojis."
)

QUERIES = [
    "AI Automation Expert",
    "Social Media Marketing Manager",
    "Chatbot Developer",
    "Custom Flow Workflow Builder"
]

# ═══════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════
def get_connection():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def is_seen(url):
    conn = get_connection()
    row  = conn.execute("SELECT id FROM seen_urls WHERE url = ?", (url,)).fetchone()
    conn.close()
    return row is not None

def mark_seen(url):
    conn = get_connection()
    conn.execute("INSERT OR IGNORE INTO seen_urls (url) VALUES (?)", (url,))
    conn.commit()
    conn.close()

def save_lead(data):
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO leads
        (title, description, location, job_condition,
         client_type, profile_url, apply_url, job_time, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
    """, (
        data.get("title", ""),
        data.get("description", ""),
        data.get("location", ""),
        data.get("job_condition", ""),
        data.get("client_type", "Main Client"),
        data.get("profile_url", ""),
        data.get("apply_url", ""),
        data.get("job_time", "")
    ))
    conn.commit()
    conn.close()

def get_pending_leads():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM leads WHERE status = 'pending' LIMIT 20").fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_followup_leads():
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM leads
        WHERE status = 'sent'
        AND created_at <= datetime('now', '-3 days')
    """).fetchall()
    conn.close()
    return [dict(row) for row in rows]

def update_lead_status(profile_url, status):
    conn = get_connection()
    conn.execute("UPDATE leads SET status = ? WHERE profile_url = ?", (status, profile_url))
    conn.commit()
    conn.close()

def is_already_contacted(profile_url):
    conn = get_connection()
    row  = conn.execute(
        "SELECT id FROM conversations WHERE profile_url = ?", (profile_url,)
    ).fetchone()
    conn.close()
    return row is not None

def save_conversation(data):
    conn = get_connection()
    conn.execute("""
        INSERT INTO conversations
        (profile_url, company_name, role, message, signal)
        VALUES (?, ?, ?, ?, ?)
    """, (
        data.get("profile_url", ""),
        data.get("company_name", ""),
        data.get("role", "bilal"),
        data.get("message", ""),
        data.get("signal", None)
    ))
    conn.commit()
    conn.close()

def get_conversation_history(profile_url):
    conn = get_connection()
    rows = conn.execute("""
        SELECT role, message FROM conversations
        WHERE profile_url = ?
        ORDER BY created_at ASC
    """, (profile_url,)).fetchall()
    conn.close()
    return [dict(row) for row in rows]

def update_conversation_signal(profile_url, signal):
    conn = get_connection()
    conn.execute(
        "UPDATE conversations SET signal = ? WHERE profile_url = ?",
        (signal, profile_url)
    )
    conn.commit()
    conn.close()

def save_memory(data):
    conn = get_connection()
    conn.execute("""
        INSERT INTO memory
        (client_type, signal, what_failed, better_response, emotion_tone)
        VALUES (?, ?, ?, ?, ?)
    """, (
        data.get("client_type", "Main Client"),
        data.get("signal", ""),
        data.get("what_failed", ""),
        data.get("better_response", ""),
        data.get("emotion_tone", "")
    ))
    conn.commit()
    conn.close()

def save_context(data):
    conn = get_connection()
    conn.execute("""
        INSERT INTO context
        (situation, bilal_response, client_reaction, lesson, what_worked)
        VALUES (?, ?, ?, ?, ?)
    """, (
        data.get("situation", ""),
        data.get("bilal_response", ""),
        data.get("client_reaction", ""),
        data.get("lesson", ""),
        data.get("what_worked", "")
    ))
    conn.commit()
    conn.close()

# ═══════════════════════════════════════════════════════
# BRAIN
# ═══════════════════════════════════════════════════════
def build_brain():
    brain = ""
    try:
        conn         = get_connection()
        memory_rows  = conn.execute("SELECT * FROM memory  ORDER BY created_at DESC LIMIT 30").fetchall()
        context_rows = conn.execute("SELECT * FROM context ORDER BY created_at DESC LIMIT 30").fetchall()
        case_rows    = conn.execute("SELECT * FROM case_studies").fetchall()
        conn.close()

        if memory_rows:
            brain += "\n\nPAST LESSONS:\n"
            for r in memory_rows:
                if r["what_failed"] and r["what_failed"].lower() != "nothing failed":
                    brain += f"- [{r['client_type']}][{r['signal']}] Failed: {r['what_failed']} | Better: {r['better_response']}\n"

        if context_rows:
            brain += "\n\nPAST CONVERSATIONS:\n"
            for r in context_rows:
                brain += (
                    f"- Situation: {r['situation']}\n"
                    f"  Bilal said: {r['bilal_response']}\n"
                    f"  Client reacted: {r['client_reaction']}\n"
                    f"  Lesson: {r['lesson']} | Worked: {r['what_worked']}\n"
                )

        if case_rows:
            brain += "\n\nCASE STUDIES:\n"
            for r in case_rows:
                brain += (
                    f"- Client: {r['client_name']} | Service: {r['service']}\n"
                    f"  Problem: {r['problem']}\n"
                    f"  Results: {r['results']}\n"
                    f"  Review: {r['review']}\n"
                )

        print(f"Brain ready — Memory: {len(memory_rows)} | Context: {len(context_rows)} | Cases: {len(case_rows)}")

    except Exception as e:
        print(f"Brain error: {e}")

    return brain

# ═══════════════════════════════════════════════════════
# AI
# ═══════════════════════════════════════════════════════
def call_openai(payload, retries=3):
    for attempt in range(retries):
        try:
            res = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_KEY}"},
                json=payload,
                timeout=30
            )
            if res.status_code == 429:
                wait = 2 ** attempt
                print(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            return res
        except Exception as e:
            print(f"OpenAI error: {e}")
    return None

def get_client_type(description):
    desc = description.lower()
    if any(x in desc for x in ["enterprise", "fortune", "global leader", "multinational"]):
        return "Opportunity"
    if any(x in desc for x in ["startup", "growing", "series a", "saas", "scale up"]):
        return "GoodClient"
    return "Main Client"

def qualify_job(title, description):
    try:
        res = call_openai({
            "model":       "gpt-4o-mini",
            "messages":    [
                {"role": "system", "content": (
                    "You are a lead qualification expert. "
                    "RELEVANT: company looking to outsource digital/tech/marketing work. "
                    "NOT RELEVANT: employee hiring only. "
                    "Reply ONLY: relevant: yes OR relevant: no"
                )},
                {"role": "user", "content": f"Title: {title}\nDescription: {description[:300]}"}
            ],
            "temperature": 0.1,
            "max_tokens":  10
        })
        answer = res.json()["choices"][0]["message"]["content"].strip().lower()
        return "relevant: yes" in answer
    except Exception as e:
        print(f"Qualify error: {e}")
        return True

def generate_hook(lead, brain):
    try:
        client_type = lead.get("client_type", "Main Client")
        temp        = 0.3 if client_type == "Opportunity" else 0.5 if client_type == "GoodClient" else 0.7

        res  = call_openai({
            "model":    "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT + brain},
                {"role": "user",   "content": (
                    f"Title: {lead.get('title', '')}\n"
                    f"Description: {lead.get('description', '')[:300]}\n"
                    f"Client Type: {client_type}\n"
                    f"Write LinkedIn DM. Max 190 chars. Pain point first. Soft CTA. No price. Plain text only."
                )}
            ],
            "temperature": temp,
            "max_tokens":  100
        })
        note = res.json()["choices"][0]["message"]["content"].strip()
        note = note.replace("**", "").replace("*", "").replace("#", "").replace("\n", " ")
        if len(note) > 190:
            note = note[:187] + "..."
        return note
    except Exception as e:
        print(f"Hook error: {e}")
        return "Saw you need help scaling — we have done this for similar businesses. Worth a quick chat?"

def generate_dm(history, brain):
    try:
        history_text    = ""
        last_client_msg = ""
        for msg in history:
            role          = "Bilal" if msg["role"] == "bilal" else "Client"
            history_text += f"{role}: {msg['message']}\n"
        for msg in reversed(history):
            if msg["role"] == "client":
                last_client_msg = msg["message"]
                break

        res   = call_openai({
            "model":    "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT + brain},
                {"role": "user",   "content": (
                    f"Conversation:\n{history_text}\n\n"
                    f"Client just said: {last_client_msg}\n\n"
                    f"Max 3 sentences. Human tone. No price. Plain text only. Output ONLY the reply."
                )}
            ],
            "temperature": 0.5,
            "max_tokens":  150
        })
        reply = res.json()["choices"][0]["message"]["content"].strip()
        reply = reply.replace("**", "").replace("*", "").replace("#", "").replace("\n", " ")
        return reply
    except Exception as e:
        print(f"DM error: {e}")
        return "Thanks for your message! Could you tell me more about what you need?"

def determine_signal(message):
    msg = message.lower()
    if any(x in msg for x in ["yes", "interested", "lets talk", "sounds good",
                                "how much", "tell me more", "great", "sure"]):
        return "Green"
    if any(x in msg for x in ["no thanks", "not interested", "already hired", "not looking"]):
        return "Red"
    if any(x in msg for x in ["contract", "legal", "nda", "registration"]):
        return "Yellow"
    return None

# ═══════════════════════════════════════════════════════
# BROWSER HELPERS
# ═══════════════════════════════════════════════════════
def get_browser_args():
    return {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "viewport":   {"width": 1366, "height": 768}
    }

def get_cookies():
    return [
        {"name": "li_at",      "value": LI_AT,                "domain": ".linkedin.com",     "path": "/"},
        {"name": "JSESSIONID", "value": f'"{LI_JSESSIONID}"', "domain": ".www.linkedin.com", "path": "/"},
    ]

async def human_type(element, text):
    for char in text:
        await element.type(char, delay=random.uniform(80, 160))
        if random.random() < 0.08:
            await asyncio.sleep(random.uniform(0.2, 0.6))

# ═══════════════════════════════════════════════════════
# WATCHER — Nai jobs dhundo
# ═══════════════════════════════════════════════════════
def get_session():
    s = requests.Session()
    s.headers.update({
        "accept":                         "application/vnd.linkedin.normalized+json+2.1",
        "csrf-token":                     LI_JSESSIONID,
        "user-agent":                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/109.0.0.0 Safari/537.36",
        "x-li-lang":                      "en_US",
        "x-restli-protocol-version":      "2.0.0",
        "x-li-deco-include-micro-schema": "true",
        "cookie":                         f'JSESSIONID="{LI_JSESSIONID}"; li_at={LI_AT}'
    })
    return s

def search_jobs(query, s):
    try:
        kw  = query.replace(" ", "%20")
        url = (
            "https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards"
            "?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollectionLite-88"
            "&count=5&q=jobSearch"
            f"&query=(origin:JOBS_HOME_SEARCH_BUTTON,keywords:{kw},locationUnion:(geoId:92000000),spellCorrectionEnabled:true)"
            "&servedEventEnabled=false&start=0&f_TPR=r259200"
        )
        res   = s.get(url)
        data  = res.json()
        cards = data.get("data", {}).get("metadata", {}).get("jobCardPrefetchQueries", [])
        ids   = []
        for card in cards:
            for key in card.get("prefetchJobPostingCard", {}).keys():
                match = re.search(r'\((\d+),', key)
                if match:
                    ids.append(match.group(1))
        print(f"Search '{query}': {len(ids)} jobs")
        return ids
    except Exception as e:
        print(f"Search error: {e}")
        return []

def get_job_data(job_id, s):
    try:
        time.sleep(random.uniform(2, 4))
        res = s.get(
            f"https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}",
            headers={
                "accept":                    "application/vnd.linkedin.normalized+json+2.1",
                "csrf-token":                LI_JSESSIONID,
                "user-agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/109.0.0.0 Safari/537.36",
                "x-restli-protocol-version": "2.0.0",
                "cookie":                    f'JSESSIONID="{LI_JSESSIONID}"; li_at={LI_AT}'
            }
        )
        if res.status_code != 200:
            return None

        raw   = res.json()
        data  = raw.get("data", {})
        title = data.get("title", "")
        if not title:
            return None

        listed_at = data.get("listedAt", "")
        job_time  = ""
        if listed_at:
            posted = datetime.fromtimestamp(int(listed_at) / 1000)
            diff   = datetime.now() - posted
            if diff > timedelta(hours=72):
                print(f"Too old — skip!")
                return None
            job_time = posted.strftime("%H:%M")

        apply    = data.get("applyMethod", {})
        atype    = apply.get("$type", "")
        external = apply.get("companyApplyUrl", "") if "OffsiteApply"       in atype else ""
        easy     = apply.get("easyApplyUrl",    "") if "ComplexOnsiteApply" in atype else ""

        location = data.get("formattedLocation", "")
        if data.get("workRemoteAllowed") and "remote" not in location.lower():
            location = f"Remote ({location})" if location else "Remote"

        emp           = data.get("employmentStatus", "")
        job_condition = ""
        if emp:
            type_map = {
                "FULL_TIME": "Full Time", "PART_TIME": "Part Time",
                "CONTRACT":  "Contract",  "TEMPORARY": "Temporary",
                "INTERNSHIP":"Internship","OTHER":     "Other"
            }
            job_condition = type_map.get(emp.split(":")[-1], "")

        return {
            "title":         title,
            "description":   data.get("description", {}).get("text", "")[:300],
            "location":      location,
            "job_condition": job_condition,
            "job_time":      job_time,
            "profile_url":   data.get("jobPostingUrl", f"https://www.linkedin.com/jobs/view/{job_id}/"),
            "apply_url":     external if external else easy
        }
    except Exception as e:
        print(f"Job data error: {e}")
        return None

def run_watcher():
    print(f"\n{'='*50}\nWatcher Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*50}")
    s = get_session()
    for query in QUERIES:
        time.sleep(random.uniform(3, 6))
        for job_id in search_jobs(query, s):
            url = f"https://www.linkedin.com/jobs/view/{job_id}/"
            if is_seen(url):
                continue
            data = get_job_data(job_id, s)
            if not data:
                continue
            if not qualify_job(data["title"], data["description"]):
                mark_seen(url)
                continue
            data["client_type"] = get_client_type(data["description"])
            save_lead(data)
            mark_seen(url)
            time.sleep(random.uniform(1, 3))
    print(f"Watcher Done: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ═══════════════════════════════════════════════════════
# SENDER — Messages bhejo
# ═══════════════════════════════════════════════════════
async def send_message_to_company(page, job_url, note):
    try:
        await page.goto(job_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(4, 6))
        await page.mouse.move(random.randint(200, 600), random.randint(200, 500))

        company_url = await page.evaluate("""
            () => {
                const links   = Array.from(document.querySelectorAll('a'));
                const company = links.find(l =>
                    l.href.includes('/company/') && l.innerText.trim().length > 0
                );
                if (!company) return null;
                let url = company.href.split('?')[0];
                ['/life','/about','/jobs','/people','/posts'].forEach(s => {
                    if (url.includes(s)) url = url.substring(0, url.indexOf(s));
                });
                while (url.endsWith('/')) url = url.slice(0, -1);
                return url + '/';
            }
        """)

        if not company_url:
            return False, None, None

        company_name = company_url.split('/company/')[1].replace('/', '').strip()
        await page.goto(company_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(4, 6))

        clicked = await page.evaluate("""
            () => {
                const btn = Array.from(document.querySelectorAll('button'))
                    .find(b => b.innerText.trim() === 'Message' || b.innerText.trim() === 'Send message');
                if (btn) { btn.click(); return true; }
                return false;
            }
        """)

        if not clicked:
            return False, company_name, company_url

        await asyncio.sleep(random.uniform(4, 6))

        msg_box = (
            await page.query_selector('textarea.artdeco-text-input--input') or
            await page.query_selector('textarea')
        )
        if not msg_box:
            return False, company_name, company_url

        await msg_box.click()
        await asyncio.sleep(random.uniform(1, 2))
        await human_type(msg_box, note)
        await asyncio.sleep(random.uniform(2, 3))

        sent = await page.evaluate("""
            () => {
                const btn = Array.from(document.querySelectorAll('button'))
                    .find(b => (b.innerText.trim() === 'Send message' || b.innerText.trim() === 'Send') && !b.disabled);
                if (btn) { btn.click(); return true; }
                return false;
            }
        """)

        await asyncio.sleep(2)
        if sent:
            print(f"Message sent: {company_name}")
        return sent, company_name, company_url

    except Exception as e:
        print(f"Send error: {e}")
        return False, None, None

async def send_cold_followup(page, profile_url, company_name):
    msg = "Hey, just checking if you had a chance to review my message — worth a 5 min chat?"
    try:
        await page.goto(profile_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(5, 8))

        clicked = await page.evaluate("""
            () => {
                const btn = Array.from(document.querySelectorAll('button'))
                    .find(b => b.innerText.trim() === 'Message' || b.innerText.trim() === 'Send message');
                if (btn) { btn.click(); return true; }
                return false;
            }
        """)
        if not clicked:
            return False

        await asyncio.sleep(random.uniform(3, 5))
        msg_box = await page.query_selector('textarea') or await page.query_selector('textarea.artdeco-text-input--input')
        if not msg_box:
            return False

        await msg_box.click()
        await human_type(msg_box, msg)
        await asyncio.sleep(random.uniform(2, 3))

        sent = await page.evaluate("""
            () => {
                const btn = Array.from(document.querySelectorAll('button'))
                    .find(b => (b.innerText.trim() === 'Send' || b.innerText.trim() === 'Send message') && !b.disabled);
                if (btn) { btn.click(); return true; }
                return false;
            }
        """)
        await asyncio.sleep(2)
        return sent
    except Exception as e:
        print(f"Followup error: {e}")
        return False

async def run_sender(page, brain):
    print(f"\n{'='*50}\nSender Start\n{'='*50}")
    leads      = get_pending_leads()
    sent_count = 0

    for lead in leads:
        if sent_count >= MAX_MESSAGES_PER_RUN:
            break

        job_url = lead.get("profile_url", "")
        if not job_url:
            continue
        if is_already_contacted(job_url):
            update_lead_status(job_url, "sent")
            continue

        note    = generate_hook(lead, brain)
        success, company_name, company_url = await send_message_to_company(page, job_url, note)

        if success:
            sent_count += 1
            save_conversation({
                "profile_url":  company_url or job_url,
                "company_name": company_name or "",
                "role":         "bilal",
                "message":      note
            })
            update_lead_status(job_url, "sent")
            await asyncio.sleep(random.uniform(15, 30))
        else:
            await asyncio.sleep(random.uniform(5, 10))

    # Cold followups
    print("\n--- Cold Followups ---")
    for lead in get_followup_leads():
        url  = lead.get("profile_url", "")
        name = lead.get("title", "")
        if url:
            success = await send_cold_followup(page, url, name)
            if success:
                update_lead_status(url, "followup_sent")
            await asyncio.sleep(random.uniform(15, 30))

# ═══════════════════════════════════════════════════════
# NOTIFIER — Replies check karo
# ═══════════════════════════════════════════════════════
async def process_reply(page, conv_url, brain):
    try:
        await page.goto(conv_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(5, 8))
        await page.mouse.move(random.randint(200, 600), random.randint(200, 500))
        await asyncio.sleep(random.uniform(1, 3))

        sender_profile_url = await page.evaluate("""
            () => {
                const link = document.querySelector(
                    '.msg-thread__link-to-profile, a[href*="/in/"], a[href*="/company/"]'
                );
                return link ? link.href.split('?')[0] : null;
            }
        """)

        sender_name = await page.evaluate("""
            () => {
                const el = document.querySelector(
                    '.msg-entity-lockup__entity-title, .msg-thread__participant-name'
                );
                return el ? el.innerText.trim() : null;
            }
        """)

        page_messages = await page.evaluate("""
            () => {
                return Array.from(document.querySelectorAll('.msg-s-message-list__event'))
                    .map(m => ({
                        role:    m.querySelector('.msg-s-message-group__name') ? 'client' : 'bilal',
                        message: m.querySelector('.msg-s-event-listitem__body')?.innerText.trim() || ''
                    }))
                    .filter(m => m.message);
            }
        """)

        if not page_messages:
            return

        last_client_msg = ""
        for msg in reversed(page_messages):
            if msg["role"] == "client":
                last_client_msg = msg["message"]
                break

        if not last_client_msg:
            return

        print(f"Reply from {sender_name}: {last_client_msg[:60]}")

        signal  = determine_signal(last_client_msg)
        history = get_conversation_history(sender_profile_url) or page_messages
        reply   = generate_dm(history, brain)

        await asyncio.sleep(random.uniform(3, 6))
        msg_box = (
            await page.query_selector('div.msg-form__contenteditable') or
            await page.query_selector('div[role="textbox"]') or
            await page.query_selector('div[contenteditable="true"]')
        )

        if msg_box:
            await msg_box.click()
            await asyncio.sleep(random.uniform(1, 2))
            await human_type(msg_box, reply)
            await asyncio.sleep(random.uniform(2, 4))
            await page.evaluate("""
                () => {
                    const btn = Array.from(document.querySelectorAll('button'))
                        .find(b => b.innerText.trim() === 'Send' || b.getAttribute('type') === 'submit');
                    if (btn) btn.click();
                }
            """)
            await asyncio.sleep(2)
            print(f"Reply sent!")

            save_conversation({"profile_url": sender_profile_url, "company_name": sender_name, "role": "client",  "message": last_client_msg, "signal": signal})
            save_conversation({"profile_url": sender_profile_url, "company_name": sender_name, "role": "bilal",   "message": reply})

            if signal == "Green":
                update_lead_status(sender_profile_url, "warm")
                save_context({
                    "situation":       f"Client replied positively — {sender_name}",
                    "bilal_response":  reply,
                    "client_reaction": last_client_msg,
                    "lesson":          "Positive response",
                    "what_worked":     "Client engaged"
                })
            elif signal == "Red":
                update_lead_status(sender_profile_url, "missed")
                save_memory({
                    "client_type":     "Main Client",
                    "signal":          "Red",
                    "what_failed":     last_client_msg[:100],
                    "better_response": "Review conversation before rejection",
                    "emotion_tone":    "Cold"
                })
            elif signal == "Yellow":
                update_lead_status(sender_profile_url, "warm")
                update_conversation_signal(sender_profile_url, "Yellow")
                print(f"YELLOW ALERT: {last_client_msg[:100]}")

    except Exception as e:
        print(f"Process reply error: {e}")

async def run_notifier(page, brain):
    print(f"\n{'='*50}\nNotifier Start\n{'='*50}")
    try:
        await page.goto("https://www.linkedin.com/messaging/", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(5, 8))

        conversations = await page.evaluate("""
            () => {
                return Array.from(document.querySelectorAll('.msg-conversation-listitem'))
                    .filter(c => c.querySelector('.msg-conversation-listitem__unread-count'))
                    .map(c => c.querySelector('a')?.href)
                    .filter(Boolean);
            }
        """)

        print(f"Unread: {len(conversations)}")
        for conv_url in conversations:
            await process_reply(page, conv_url, brain)
            await asyncio.sleep(random.uniform(20, 40))

    except Exception as e:
        print(f"Notifier error: {e}")

# ═══════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════
async def main():
    print(f"\n{'='*50}\nLinkedIn Agent Starting\n{'='*50}")

    last_watcher_run = 0  # Pehli baar turant chalega

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(**get_browser_args())
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        await context.add_cookies(get_cookies())
        page = await context.new_page()

        await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(3, 5))

        while True:
            now   = time.time()
            brain = build_brain()

            # Watcher + Sender — har 2 ghante
            if now - last_watcher_run >= WATCHER_INTERVAL:
                print("\nWatcher + Sender time!")
                run_watcher()
                await run_sender(page, brain)
                last_watcher_run = time.time()

            # Notifier — har 30 minute
            print("\nNotifier time!")
            await run_notifier(page, brain)

            print(f"\nSleeping 30 minutes...")
            await asyncio.sleep(NOTIFIER_INTERVAL)

asyncio.run(main())
