import os
import random
import asyncio
import requests
import httpx
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

LI_AT               = os.environ["LI_AT"]
LI_JSESSIONID       = os.environ["LI_JSESSIONID"]
OPENAI_KEY          = os.environ["OPENAI_KEY"]
WATCHER_SHEET_URL   = os.environ["WATCHER_SHEET_URL"]
RESPONDER_SHEET_URL = os.environ["RESPONDER_SHEET_URL"]
MEMORY_URL          = os.environ["MEMORY_URL"]
CONTEXT_URL         = os.environ["CONTEXT_URL"]

MAX_CONNECTIONS_PER_RUN = 15
MAX_MEMORY_ROWS         = 120
MAX_CONTEXT_ROWS        = 120

SYSTEM_PROMPT = (
    "You are Bilal, a professional sales agent for NoError — a digital agency that works with clients globally. "
    "You are a human sales representative. "
    "If asked if you are AI say: I am part of the NoError sales team, for detailed questions I can connect you with our lead consultant directly. "
    "HARD RULES: "
    "1. NEVER share price in first or second message. "
    "2. Keep first DM under 2 sentences only. "
    "3. NEVER use markdown, bold, bullets, or any formatting. Plain text only. "
    "4. NEVER mention price unless client asks multiple times. "
    "TONE: Main Client = casual friendly. GoodClient = professional. Opportunity = formal corporate zero emojis."
)


def build_brain():
    brain = ""
    try:
        res  = requests.get(MEMORY_URL)
        res2 = requests.get(CONTEXT_URL)

        memory_rows  = res.json().get("rows", [])
        context_rows = res2.json().get("rows", [])

        rows_to_use = min(30, len(memory_rows), len(context_rows))

        brain += "\n\nPAST LESSONS (learn from these):\n"
        for row in memory_rows[:rows_to_use]:
            signal = row.get("signal", "")
            failed = row.get("what_failed", "")
            worked = row.get("better_response", "")
            ct     = row.get("client_type", "")
            tone   = row.get("emotion_tone", "")
            if failed and failed.lower() != "nothing failed":
                brain += f"- [{ct}][{signal}][{tone}] Failed: {failed} | Better: {worked}\n"

        brain += "\n\nPAST CONVERSATIONS (learn patterns):\n"
        for row in context_rows[:rows_to_use]:
            sit    = row.get("situation", "")
            resp   = row.get("bilal_response", "")
            react  = row.get("client_reaction", "")
            lesson = row.get("lesson", "")
            worked = row.get("what_worked", "")
            brain += f"- Situation: {sit}\n  Bilal said: {resp}\n  Client reacted: {react}\n  Lesson: {lesson} | Worked: {worked}\n"

        print(f"Brain built — Memory: {len(memory_rows)} rows | Context: {len(context_rows)} rows | Used: {rows_to_use} pairs")
    except Exception as e:
        print(f"Brain build error: {e}")
    return brain


def fetch_leads():
    try:
        res   = requests.get(WATCHER_SHEET_URL, params={"filter_col": "M", "filter_val": "In Pending"})
        leads = res.json().get("rows", [])
        print(f"Leads found: {len(leads)}")
        return leads
    except Exception as e:
        print(f"Fetch leads error: {e}")
        return []


def get_temperature(client_type):
    if client_type == "Opportunity":
        return 0.3
    elif client_type == "GoodClient":
        return 0.5
    return 0.7


def reclassify_client(title, description):
    desc = (title + " " + description).lower()
    if any(x in desc for x in ["enterprise", "fortune", "global leader", "publicly traded", "10,000", "multinational", "corporation"]):
        return "Opportunity"
    if any(x in desc for x in ["startup", "growing", "series a", "series b", "saas", "scale up", "mid-size", "scaleup"]):
        return "GoodClient"
    return "Main Client"


def call_openai(payload, retries=3):
    for attempt in range(retries):
        try:
            res = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_KEY}"},
                json=payload
            )
            if res.status_code == 429:
                wait = 2 ** attempt
                print(f"OpenAI rate limited — waiting {wait}s")
                import time
                time.sleep(wait)
                continue
            return res
        except Exception as e:
            print(f"OpenAI call error: {e}")
    return None


def generate_hook(lead, brain):
    try:
        client_type = lead.get("client_type", "Main Client")
        title       = lead.get("title", "")
        description = lead.get("description", "")
        temp        = get_temperature(client_type)

        prompt = (
            f"Lead Info:\n"
            f"Job Title: {title}\n"
            f"Description: {description[:300]}\n"
            f"Client Type: {client_type}\n\n"
            f"Write a LinkedIn message note for this lead. "
            f"Max 190 characters. "
            f"Start with a pain point from their job description. "
            f"End with a soft CTA. "
            f"Do NOT mention price. "
            f"No markdown, no bold, no bullets. Plain text only. "
            f"Tone: {'casual and friendly' if client_type == 'Main Client' else 'professional' if client_type == 'GoodClient' else 'formal corporate no emojis'}. "
            f"Output ONLY the message text, nothing else."
        )

        res = call_openai({
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT + brain},
                {"role": "user",   "content": prompt}
            ],
            "temperature": temp,
            "max_tokens": 100
        })

        if not res:
            raise Exception("OpenAI returned None")

        note = res.json()["choices"][0]["message"]["content"].strip()
        if len(note) > 190:
            note = note[:187] + "..."
        print(f"Hook generated: {note}")
        return note
    except Exception as e:
        print(f"Hook error: {e}")
        return "Saw you need automation help — we build AI solutions for businesses like yours. Worth a quick chat?"


def generate_dm(conversation_history, brain):
    try:
        history_text = ""
        for msg in conversation_history:
            role = "Bilal" if msg["role"] == "bilal" else "Client"
            history_text += f"{role}: {msg['text']}\n"

        last_client_msg = ""
        for msg in reversed(conversation_history):
            if msg["role"] == "client":
                last_client_msg = msg["text"]
                break

        prompt = (
            f"Full conversation so far:\n{history_text}\n\n"
            f"Client just said: {last_client_msg}\n\n"
            f"Read the full conversation carefully and understand:\n"
            f"- Who is this person based on what they said\n"
            f"- What do they need\n"
            f"- What tone fits them\n\n"
            f"Use your past lessons and conversations to guide your reply.\n"
            f"Max 3 sentences. Natural and human. No price. No markdown. Plain text only.\n"
            f"If client asks price — say: Depends on your exact needs, can we discuss scope first.\n"
            f"If client asks team size — say: We are a focused specialist team, results matter more than headcount.\n"
            f"Output ONLY the reply text."
        )

        res = call_openai({
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT + brain},
                {"role": "user",   "content": prompt}
            ],
            "temperature": 0.5,
            "max_tokens": 150
        })

        if not res:
            raise Exception("OpenAI returned None")

        reply = res.json()["choices"][0]["message"]["content"].strip()
        print(f"DM generated: {reply[:80]}")
        return reply
    except Exception as e:
        print(f"DM generate error: {e}")
        return "Thanks for your message! Could you tell me more about what you need?"


def determine_signal(client_message):
    msg = client_message.lower()
    if any(x in msg for x in ["yes", "interested", "lets talk", "sounds good", "when", "how much", "tell me more", "great", "perfect"]):
        return "Green"
    if any(x in msg for x in ["no thanks", "not interested", "already hired", "found someone", "not looking"]):
        return "Red"
    if any(x in msg for x in ["contract", "legal", "nda", "company registration"]):
        return "Yellow"
    return None


async def save_to_responder(data):
    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(RESPONDER_SHEET_URL, json=data)
            print(f"Responder saved: {res.text[:50]}")
    except Exception as e:
        print(f"Responder save error: {e}")


async def update_watcher(profile_url, lead_status=None, lead_type=None, client_type=None, if_alert=None):
    try:
        payload = {"action": "update", "profile_url": profile_url}
        if lead_status: payload["lead_status"] = lead_status
        if lead_type:   payload["lead_type"]   = lead_type
        if client_type: payload["client_type"] = client_type
        if if_alert:    payload["if_alert"]    = if_alert
        async with httpx.AsyncClient() as client:
            res = await client.post(WATCHER_SHEET_URL, json=payload)
            print(f"Watcher updated: {res.text[:50]}")
    except Exception as e:
        print(f"Watcher update error: {e}")


async def save_to_memory(data):
    try:
        res = requests.get(MEMORY_URL)
        current_rows = res.json().get("rows", [])
        if len(current_rows) >= MAX_MEMORY_ROWS:
            print("Memory sheet full — skipping save")
            return
        async with httpx.AsyncClient() as client:
            await client.post(MEMORY_URL, json=data)
    except Exception as e:
        print(f"Memory save error: {e}")


async def save_to_context(data):
    try:
        res = requests.get(CONTEXT_URL)
        current_rows = res.json().get("rows", [])
        if len(current_rows) >= MAX_CONTEXT_ROWS:
            print("Context sheet full — skipping save")
            return
        async with httpx.AsyncClient() as client:
            await client.post(CONTEXT_URL, json=data)
    except Exception as e:
        print(f"Context save error: {e}")


def get_browser_context_args():
    return {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "viewport": {"width": 1366, "height": 768}
    }


def get_li_cookies():
    return [
        {"name": "li_at",      "value": LI_AT,                  "domain": ".linkedin.com",      "path": "/"},
        {"name": "JSESSIONID", "value": f'"{LI_JSESSIONID}"',   "domain": ".www.linkedin.com",  "path": "/"},
    ]


async def send_message_to_company(profile_url, note, brain):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(**get_browser_context_args())
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        await context.add_cookies(get_li_cookies())
        page = await context.new_page()

        try:
            await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(3, 5))

            await page.goto(profile_url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(4, 6))

            company_name = await page.evaluate("""
                () => {
                    const el = document.querySelector('.job-details-jobs-unified-top-card__company-name a, .jobs-unified-top-card__company-name a');
                    return el ? el.innerText.trim() : null;
                }
            """)

            company_url = await page.evaluate("""
                () => {
                    const el = document.querySelector('.job-details-jobs-unified-top-card__company-name a, .jobs-unified-top-card__company-name a');
                    return el ? el.href.split('?')[0] : null;
                }
            """)

            if not company_url:
                print(f"Company URL not found for job: {profile_url}")
                return False, None, None

            print(f"Company found: {company_name} — {company_url}")

            await page.goto(company_url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(3, 5))

            clicked = await page.evaluate("""
                () => {
                    const buttons = Array.from(document.querySelectorAll('button'));
                    const btn = buttons.find(b => b.innerText.trim() === 'Message');
                    if (btn) { btn.click(); return true; }
                    return false;
                }
            """)

            if not clicked:
                print(f"Message button not found for company: {company_url}")
                return False, company_name, company_url

            await asyncio.sleep(random.uniform(2, 3))

            msg_box = await page.query_selector('div.msg-form__contenteditable')
            if not msg_box:
                msg_box = await page.query_selector('div[role="textbox"]')
            if not msg_box:
                msg_box = await page.query_selector('div[contenteditable="true"]')

            if msg_box:
                await msg_box.click()
                await asyncio.sleep(random.uniform(1, 2))

                for char in note:
                    await msg_box.type(char, delay=random.uniform(60, 130))
                    if random.random() < 0.08:
                        await asyncio.sleep(random.uniform(0.2, 0.5))

                await asyncio.sleep(random.uniform(1, 2))

                await page.evaluate("""
                    () => {
                        const buttons = Array.from(document.querySelectorAll('button'));
                        const btn = buttons.find(b => b.innerText.trim() === 'Send' || b.getAttribute('type') === 'submit');
                        if (btn) btn.click();
                    }
                """)
                await asyncio.sleep(2)
                print(f"Message sent to company: {company_name}")
                return True, company_name, company_url

            return False, company_name, company_url

        except Exception as e:
            print(f"Send message error: {e}")
            return False, None, None
        finally:
            await browser.close()


async def send_connection_note(profile_url, note):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(**get_browser_context_args())
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        await context.add_cookies(get_li_cookies())
        page = await context.new_page()

        try:
            await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(3, 5))
            await page.goto(profile_url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(4, 6))

            clicked = await page.evaluate("""
                () => {
                    const buttons = Array.from(document.querySelectorAll('button'));
                    const btn = buttons.find(b => b.innerText.trim() === 'Connect');
                    if (btn) { btn.click(); return true; }
                    return false;
                }
            """)

            if not clicked:
                print(f"Connect button not found for {profile_url}")
                return False

            await asyncio.sleep(random.uniform(2, 3))

            await page.evaluate("""
                () => {
                    const buttons = Array.from(document.querySelectorAll('button'));
                    const btn = buttons.find(b => b.innerText.includes('Add a note'));
                    if (btn) btn.click();
                }
            """)
            await asyncio.sleep(random.uniform(1, 2))

            note_box = await page.query_selector('textarea[name="message"]')
            if note_box:
                for char in note:
                    await note_box.type(char, delay=random.uniform(60, 130))
                    if random.random() < 0.08:
                        await asyncio.sleep(random.uniform(0.2, 0.5))

                await asyncio.sleep(random.uniform(1, 2))
                await page.evaluate("""
                    () => {
                        const buttons = Array.from(document.querySelectorAll('button'));
                        const btn = buttons.find(b => b.innerText.trim() === 'Send');
                        if (btn) btn.click();
                    }
                """)
                await asyncio.sleep(2)
                print(f"Connection + note sent to {profile_url}")
                return True

            return False

        except Exception as e:
            print(f"Connection note error: {e}")
            return False
        finally:
            await browser.close()


async def check_messages():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(**get_browser_context_args())
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        await context.add_cookies(get_li_cookies())
        page = await context.new_page()

        try:
            await page.goto("https://www.linkedin.com/messaging/", wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(4, 6))

            conversations = await page.evaluate("""
                () => {
                    const convs = Array.from(document.querySelectorAll('.msg-conversation-listitem'));
                    const unread = convs.filter(c => c.querySelector('.msg-conversation-listitem__unread-count'));
                    return unread.map(c => {
                        const link = c.querySelector('a');
                        return link ? link.href : null;
                    }).filter(Boolean);
                }
            """)

            print(f"Unread conversations: {len(conversations)}")
            return conversations

        except Exception as e:
            print(f"Check messages error: {e}")
            return []
        finally:
            await browser.close()


async def get_responder_record(profile_url, company_name):
    try:
        res = requests.get(RESPONDER_SHEET_URL)
        rows = res.json().get("rows", [])

        for row in rows:
            if profile_url and row.get("profile_url", "").strip() == profile_url.strip():
                return row

        if company_name:
            for row in rows:
                if company_name.lower() in row.get("company_name", "").lower():
                    return row

    except Exception as e:
        print(f"Responder lookup error: {e}")
    return None


async def read_and_reply(conv_url, brain):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(**get_browser_context_args())
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        await context.add_cookies(get_li_cookies())
        page = await context.new_page()

        try:
            await page.goto(conv_url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(3, 5))

            sender_profile_url = await page.evaluate("""
                () => {
                    const link = document.querySelector('.msg-thread__link-to-profile, a[href*="/in/"], a[href*="/company/"]');
                    return link ? link.href.split('?')[0] : null;
                }
            """)

            sender_name = await page.evaluate("""
                () => {
                    const el = document.querySelector('.msg-entity-lockup__entity-title, .msg-thread__participant-name');
                    return el ? el.innerText.trim() : null;
                }
            """)

            print(f"Sender: {sender_name} — {sender_profile_url}")

            responder_record = await get_responder_record(sender_profile_url, sender_name)

            messages = await page.evaluate("""
                () => {
                    const msgs = Array.from(document.querySelectorAll('.msg-s-message-list__event'));
                    return msgs.map(m => {
                        const sender = m.querySelector('.msg-s-message-group__name');
                        const text   = m.querySelector('.msg-s-event-listitem__body');
                        return {
                            role: sender ? 'client' : 'bilal',
                            text: text ? text.innerText.trim() : ''
                        };
                    }).filter(m => m.text);
                }
            """)

            if not messages:
                return

            last_client_msg = ""
            for msg in reversed(messages):
                if msg["role"] == "client":
                    last_client_msg = msg["text"]
                    break

            if not last_client_msg:
                return

            print(f"Last client message: {last_client_msg[:80]}")

            signal = determine_signal(last_client_msg)

            reply = generate_dm(messages, brain)

            msg_box = await page.query_selector('div.msg-form__contenteditable')
            if not msg_box:
                msg_box = await page.query_selector('div[role="textbox"]')
            if not msg_box:
                msg_box = await page.query_selector('div[contenteditable="true"]')

            if msg_box:
                await msg_box.click()
                await asyncio.sleep(random.uniform(1, 2))

                for char in reply:
                    await msg_box.type(char, delay=random.uniform(60, 130))
                    if random.random() < 0.08:
                        await asyncio.sleep(random.uniform(0.2, 0.4))

                await asyncio.sleep(random.uniform(1, 2))

                await page.evaluate("""
                    () => {
                        const buttons = Array.from(document.querySelectorAll('button'));
                        const btn = buttons.find(b => b.innerText.trim() === 'Send' || b.getAttribute('type') === 'submit');
                        if (btn) btn.click();
                    }
                """)
                await asyncio.sleep(2)
                print(f"Reply sent!")

            profile_to_update = sender_profile_url or (responder_record.get("profile_url", "") if responder_record else "")
            title_to_save     = responder_record.get("title", sender_name or "Unknown") if responder_record else (sender_name or "Unknown")
            client_type       = responder_record.get("client_type", "Main Client") if responder_record else "Main Client"
            company_name      = responder_record.get("company_name", sender_name or "") if responder_record else (sender_name or "")

            if signal == "Yellow":
                alert_msg = f"Client said: {last_client_msg[:150]}"
                await update_watcher(profile_to_update, if_alert=alert_msg)
                await save_to_responder({
                    "timestamp":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "title":        title_to_save,
                    "company_name": company_name,
                    "client_type":  client_type,
                    "lead_status":  "Alert",
                    "lead_rank":    "Alert",
                    "if_alert":     alert_msg,
                    "profile_url":  profile_to_update,
                    "note":         "",
                    "note_time":    ""
                })

            elif signal == "Green":
                await update_watcher(profile_to_update, lead_status="Warm", lead_type="Warm")
                await save_to_responder({
                    "timestamp":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "title":        title_to_save,
                    "company_name": company_name,
                    "client_type":  client_type,
                    "lead_status":  "Warm",
                    "lead_rank":    "Warm",
                    "if_alert":     "",
                    "profile_url":  profile_to_update,
                    "note":         reply,
                    "note_time":    datetime.now().strftime("%H:%M:%S")
                })
                await save_to_context({
                    "situation":        f"{client_type} — {title_to_save} — client replied positively",
                    "bilal_response":   reply,
                    "client_reaction":  last_client_msg,
                    "lesson":           "Positive response — note what worked",
                    "what_worked":      "Client engaged positively"
                })

            elif signal == "Red":
                await update_watcher(profile_to_update, lead_status="Missed", lead_type="Cold")
                await save_to_memory({
                    "what_failed":      f"Client said: {last_client_msg[:100]}",
                    "service_interest": title_to_save,
                    "client_type":      client_type,
                    "date_time":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "signal":           "Red",
                    "emotion_tone":     "Cold",
                    "followup_need":    "No",
                    "better_response":  "Review what was said before this rejection"
                })

        except Exception as e:
            print(f"Read reply error: {e}")
        finally:
            await browser.close()


async def send_cold_dm(profile_url, company_name, brain):
    cold_message = "Hey, just checking if you had a chance to review my message — worth a 5 min chat?"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(**get_browser_context_args())
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        await context.add_cookies(get_li_cookies())
        page = await context.new_page()

        try:
            await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(3, 5))
            await page.goto(profile_url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(4, 6))

            clicked = await page.evaluate("""
                () => {
                    const buttons = Array.from(document.querySelectorAll('button'));
                    const btn = buttons.find(b => b.innerText.trim() === 'Message');
                    if (btn) { btn.click(); return true; }
                    return false;
                }
            """)

            if not clicked:
                return False

            await asyncio.sleep(random.uniform(2, 3))

            msg_box = await page.query_selector('div.msg-form__contenteditable')
            if not msg_box:
                msg_box = await page.query_selector('div[role="textbox"]')
            if not msg_box:
                msg_box = await page.query_selector('div[contenteditable="true"]')

            if msg_box:
                await msg_box.click()
                await asyncio.sleep(1)
                for char in cold_message:
                    await msg_box.type(char, delay=random.uniform(60, 130))
                await asyncio.sleep(random.uniform(1, 2))
                await page.evaluate("""
                    () => {
                        const buttons = Array.from(document.querySelectorAll('button'));
                        const btn = buttons.find(b => b.innerText.trim() === 'Send' || b.getAttribute('type') === 'submit');
                        if (btn) btn.click();
                    }
                """)
                await asyncio.sleep(2)
                print(f"Cold DM sent to: {company_name}")
                return True

            return False

        except Exception as e:
            print(f"Cold DM error: {e}")
            return False
        finally:
            await browser.close()


async def main():
    print(f"\n{'='*50}")
    print(f"Bilal Starting: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    brain = build_brain()
    leads = fetch_leads()

    connections_sent = 0

    for lead in leads:
        if connections_sent >= MAX_CONNECTIONS_PER_RUN:
            print(f"Max connections reached ({MAX_CONNECTIONS_PER_RUN}) — stopping!")
            break

        job_url     = lead.get("profile_url", "")
        title       = lead.get("title", "")
        description = lead.get("description", "")

        if not job_url:
            continue

        print(f"\n--- Processing: {title} ---")

        client_type      = reclassify_client(title, description)
        lead["client_type"] = client_type
        print(f"Client Type: {client_type}")

        note = generate_hook(lead, brain)

        success, company_name, company_url = await send_message_to_company(job_url, note, brain)

        if success:
            connections_sent += 1
            now = datetime.now()

            await save_to_responder({
                "timestamp":    now.strftime("%Y-%m-%d %H:%M:%S"),
                "title":        title,
                "company_name": company_name or "",
                "note":         note,
                "client_type":  client_type,
                "note_time":    now.strftime("%H:%M:%S"),
                "lead_status":  "Message Sent",
                "lead_rank":    "",
                "if_alert":     "",
                "profile_url":  company_url or job_url
            })

            await update_watcher(
                job_url,
                lead_status="Message Sent",
                lead_type="Warm",
                client_type=client_type
            )

            print(f"Done: {title}")
            await asyncio.sleep(random.uniform(8, 20))
        else:
            print(f"Failed: {title}")

    print(f"\n--- Checking Cold Follow-ups ---")
    try:
        res  = requests.get(RESPONDER_SHEET_URL)
        rows = res.json().get("rows", [])

        for row in rows:
            if row.get("lead_status") == "Message Sent":
                sent_time    = row.get("timestamp", "")
                profile_url  = row.get("profile_url", "")
                company_name = row.get("company_name", "")
                try:
                    sent_dt      = datetime.strptime(sent_time, "%Y-%m-%d %H:%M:%S")
                    days_passed  = (datetime.now() - sent_dt).days
                    if days_passed >= 3 and profile_url:
                        success = await send_cold_dm(profile_url, company_name, brain)
                        if success:
                            await update_watcher(profile_url, lead_status="Cold DM Sent")
                        await asyncio.sleep(random.uniform(10, 25))
                except Exception as e:
                    print(f"Follow-up timing error: {e}")
    except Exception as e:
        print(f"Cold follow-up fetch error: {e}")

    print(f"\n--- Checking Messages ---")
    conversations = await check_messages()

    for conv_url in conversations:
        await read_and_reply(conv_url, brain)
        await asyncio.sleep(random.uniform(15, 35))

    print(f"\n{'='*50}")
    print(f"Bilal Done: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Connections sent: {connections_sent}")
    print(f"{'='*50}")


asyncio.run(main())
