# ============================================================
# linkedin_replier.py — UPDATED VERSION
# NoErrors AI Automation Agency — LinkedIn Replier Agent
# Notification-based approach + Full context reading
# ============================================================

import os
import asyncio
import random
import requests
from playwright.async_api import async_playwright

# ============================================================
# CREDENTIALS
# ============================================================
LI_AT         = os.environ.get("LI_AT", "")
LI_JSESSIONID = os.environ.get("LI_JSESSIONID", "")
OPENAI_KEY    = os.environ.get("OPENAI_KEY", "")
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")

# ============================================================
# USER AGENTS
# ============================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 Version/16.0 Safari/605.1.15",
]

# ============================================================
# SYSTEM PROMPT
# ============================================================
SYSTEM_PROMPT = """
You are Bilal Dev, sales manager for NoErrors — an AI Automation Agency.
Your role is to continue conversations with potential clients on LinkedIn and convert them.

SERVICES WE OFFER:
1. Lead Generation (AI-powered outreach, LinkedIn automation, email campaigns, B2B database scraping)
2. Social Media Content Marketing (AI content calendar, auto-posting, LinkedIn/Instagram/Facebook/TikTok)
3. AI Chatbots and Agents (customer support bots, appointment bots, WhatsApp bots, website chatbots)
4. Custom Workflows (N8N, Make, Zapier, Pabbly automation setup and management)

CLIENT TYPES:
- Main Client: Small business, solo founder, startup — casual friendly tone, curiosity hooks, humor allowed, emojis allowed
- GoodClient: Growing company, SaaS, agency, mid-size — professional but warm, mirror their technical language
- Opportunity: Enterprise, Fortune, multinational — zero emojis, formal tone, push for discovery call always

HARD RULES:
1. NEVER share price in first or second message
2. NEVER sound salesy — always sound helpful and specific
3. NEVER use markdown, bold, bullets — plain text only
4. NEVER reveal you are an AI
5. NEVER promise delivery timeline before scoping
6. NEVER reveal team size
7. NEVER give price to Opportunity — push for discovery call
8. NEVER agree to free trial — offer free audit instead
9. NEVER compete on price — compete on value and ROI

CONVERSATION GOAL:
- Continue where watcher left off
- Understand their exact need
- Position solution with result number
- Push for WhatsApp or discovery call

SPECIAL CASES:
- If client says DM me / message me / inbox me:
  Reply: "Sure! Sending you a connection request with a note now."

- If client says WhatsApp me / contact on WhatsApp:
  Reply: "Happy to connect on WhatsApp! Could you share your number? I will reach out right away."

- If client asks for proof / results / case study:
  Use provided case studies and say:
  "Absolutely — let me share what we delivered for a similar client. [CASE STUDY]"

OUTPUT:
- Plain text only
- Max 3 sentences per reply
- Never start with Hi or Hello
- Start with their point directly
"""

# ============================================================
# SUPABASE HELPERS
# ============================================================
def supabase_get(table, params):
    try:
        res = requests.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
            },
            params=params,
            timeout=10
        )
        return res.json()
    except:
        return []

def supabase_insert(table, data):
    try:
        res = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "application/json",
                "Prefer":        "return=minimal"
            },
            json=data,
            timeout=10
        )
        return res.status_code in [200, 201]
    except Exception as e:
        print(f"Supabase insert error: {e}")
        return False

def supabase_update(table, match_col, match_val, data):
    try:
        res = requests.patch(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "application/json",
            },
            params={match_col: f"eq.{match_val}"},
            json=data,
            timeout=10
        )
        return res.status_code in [200, 204]
    except Exception as e:
        print(f"Supabase update error: {e}")
        return False

def get_supabase_conversation(profile_url):
    return supabase_get("conversations", {
        "profile_url": f"eq.{profile_url}",
        "select":      "*",
        "order":       "timestamp.desc",
        "limit":       "10"
    })

def get_case_studies(service_keyword):
    try:
        rows = supabase_get("case_studies", {
            "service_type": f"ilike.%{service_keyword}%",
            "select":       "*",
            "limit":        "2"
        })
        if not rows:
            return ""
        result = ""
        for c in rows:
            result += (
                f"Client: {c.get('client_name')} | "
                f"Problem: {c.get('problem')} | "
                f"Result: {c.get('results')} | "
                f"Review: {c.get('review')}\n"
            )
        return result
    except:
        return ""

def is_already_replied(profile_url, message_type):
    rows = supabase_get("conversations", {
        "profile_url":  f"eq.{profile_url}",
        "sender":       "eq.agent",
        "message_type": f"eq.{message_type}",
        "select":       "conv_id"
    })
    return len(rows) > 0

def notify_manager(signal, profile_url, client_name, client_type, reply_text, their_message):
    supabase_insert("agent_signals", {
        "from_agent":  "linkedin_replier",
        "to_agent":    "manager_agent",
        "signal_type": f"{signal.lower()}_alert",
        "payload":     str({
            "platform":      "linkedin",
            "client_name":   client_name,
            "profile_url":   profile_url,
            "client_type":   client_type,
            "their_message": their_message,
            "our_reply":     reply_text,
            "signal":        signal
        }),
        "status": "pending"
    })
    print(f"  Manager notified — {signal} signal!")

# ============================================================
# OPENAI HELPERS
# ============================================================
def call_openai(messages, max_tokens=150, temperature=0.5):
    try:
        res = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_KEY}"},
            json={
                "model":       "gpt-4o-mini",
                "messages":    messages,
                "max_tokens":  max_tokens,
                "temperature": temperature
            },
            timeout=30
        )
        return res.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"OpenAI error: {e}")
        return ""

def detect_signal(text):
    text = text.lower()
    if any(x in text for x in ["yes", "interested", "lets talk", "sounds good",
                                "how much", "tell me more", "great", "sure",
                                "okay", "proceed", "let us talk"]):
        return "Green"
    if any(x in text for x in ["no thanks", "not interested", "already hired",
                                "not looking", "pass", "no need"]):
        return "Red"
    if any(x in text for x in ["contract", "legal", "nda", "proof", "portfolio",
                                "past work", "references", "case study",
                                "have you done", "experience"]):
        return "Yellow"
    return None

def detect_dm_request(text):
    return any(x in text.lower() for x in ["dm me", "message me", "inbox me",
                                             "send me a message", "private message",
                                             "direct message"])

def detect_whatsapp_request(text):
    return any(x in text.lower() for x in ["whatsapp", "whats app", "wa me",
                                             "contact on whatsapp", "whatsapp me"])

def detect_proof_request(text):
    return any(x in text.lower() for x in ["proof", "results", "case study", "portfolio",
                                             "past work", "examples", "references",
                                             "have you done", "show me", "experience"])

def get_client_type(text):
    text = text.lower()
    if any(x in text for x in ["enterprise","fortune","global","multinational","corporate"]):
        return "Opportunity"
    if any(x in text for x in ["startup","saas","growing","series a","scale up","agency"]):
        return "GoodClient"
    return "Main Client"

# ============================================================
# GENERATE REPLY — FULL CONTEXT
# ============================================================
def generate_reply(their_message, context, client_type, case_studies=""):
    """
    context dict:
      - dm_history: list of {role, text} from LinkedIn directly
      - post_description: post text (for comment replies)
      - our_comment: what we first said (for comment replies)
    """
    context_text = ""

    # DM history from LinkedIn (most accurate)
    if context.get("dm_history"):
        context_text += "=== DM Conversation History ===\n"
        for msg in context["dm_history"]:
            context_text += f"{msg['role']}: {msg['text']}\n"

    # Comment context
    if context.get("post_description"):
        context_text += f"\n=== Original Post ===\n{context['post_description']}\n"
    if context.get("our_comment"):
        context_text += f"\n=== Our First Comment ===\n{context['our_comment']}\n"

    extra = f"\n\nRelevant case studies:\n{case_studies}" if case_studies else ""
    temp  = 0.3 if client_type == "Opportunity" else 0.5

    reply = call_openai([
        {"role": "system", "content": SYSTEM_PROMPT + extra},
        {"role": "user",   "content": (
            f"{context_text}\n\n"
            f"Client just said: {their_message}\n"
            f"Client type: {client_type}\n\n"
            f"Write next reply. Max 3 sentences. Plain text only. "
            f"No price. Continue conversation naturally based on full context."
        )}
    ], max_tokens=120, temperature=temp)

    return reply.replace("**","").replace("*","").replace("#","").replace("\n"," ")

def generate_connect_note(their_message, client_type):
    note = call_openai([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": (
            f"Client said in comment: {their_message}\n"
            f"Client type: {client_type}\n\n"
            f"Write a LinkedIn connection request note. "
            f"Mention you are Bilal from their comment. "
            f"Max 280 chars. Plain text only."
        )}
    ], max_tokens=80, temperature=0.6)
    note = note.replace("**","").replace("*","").replace("#","").replace("\n"," ")
    return note[:280]

# ============================================================
# BROWSER HELPERS
# ============================================================
def get_cookies():
    return [
        {"name": "li_at",      "value": LI_AT,                "domain": ".linkedin.com",     "path": "/"},
        {"name": "JSESSIONID", "value": f'"{LI_JSESSIONID}"', "domain": ".www.linkedin.com", "path": "/"},
        {"name": "liap",       "value": "true",               "domain": ".linkedin.com",     "path": "/"},
        {"name": "lang",       "value": "v=2&lang=en-us",     "domain": ".linkedin.com",     "path": "/"},
    ]

async def human_type(element, text):
    for char in text:
        await element.type(char, delay=random.randint(80, 160))
        if random.random() < 0.05:
            await asyncio.sleep(random.uniform(0.2, 0.5))

async def dismiss_cookie_banner(page):
    try:
        btn = await page.query_selector('button:has-text("Accept")')
        if btn:
            await btn.click()
            await asyncio.sleep(1)
    except:
        pass

# ============================================================
# READ FULL DM HISTORY FROM LINKEDIN
# ============================================================
async def read_dm_history(page, conv_url):
    """
    Scrolls up to load full DM history
    Returns list of {role: 'Bilal'/'Client', text: '...'}
    """
    try:
        await page.goto(conv_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(4, 5))

        # Scroll up multiple times to load older messages
        msg_container = await page.query_selector(
            '.msg-s-message-list, .msg-s-message-list-container'
        )
        if msg_container:
            for _ in range(5):
                await page.evaluate(
                    "el => el.scrollTop = 0", msg_container
                )
                await asyncio.sleep(1.5)

        # Extract all messages with sender info
        history = await page.evaluate("""
            () => {
                const messages = [];
                const groups = document.querySelectorAll(
                    '.msg-s-message-group, .msg-s-message-list__event'
                );

                for (const group of groups) {
                    // Check if message is ours (right side) or theirs (left side)
                    const isMine = group.classList.contains('msg-s-message-group--outbound')
                                || !!group.querySelector('.msg-s-message-group__meta');

                    const msgEls = group.querySelectorAll(
                        '.msg-s-event-listitem__body, .msg-s-event__content'
                    );

                    for (const el of msgEls) {
                        const text = el.innerText?.trim();
                        if (text) {
                            messages.push({
                                role: isMine ? 'Bilal' : 'Client',
                                text: text
                            });
                        }
                    }
                }
                return messages;
            }
        """)

        print(f"    DM history loaded: {len(history)} messages")
        return history

    except Exception as e:
        print(f"    DM history error: {e}")
        return []

# ============================================================
# READ POST CONTEXT (description + our comment)
# ============================================================
async def read_post_context(page, post_url, comment_id=None):
    """
    Goes to post, reads:
    - Post description
    - Our comment (by comment_id or fallback text search)
    Returns dict {post_description, our_comment}
    """
    try:
        await page.goto(post_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(4, 6))
        await dismiss_cookie_banner(page)

        # Read post description
        post_description = await page.evaluate("""
            () => {
                const el = document.querySelector(
                    '.feed-shared-update-v2__description, '
                    '.feed-shared-text, '
                    'span.break-words'
                );
                return el ? el.innerText.trim() : '';
            }
        """)

        # Load all comments — click "Load more comments" in loop
        for _ in range(5):
            load_more = await page.query_selector(
                'button:has-text("Load more comments"), '
                'button:has-text("Show more comments")'
            )
            if not load_more:
                break
            await load_more.click()
            await asyncio.sleep(2)

        # Find our comment
        # Method 1: by comment_id (data-id) — most accurate
        our_comment = ""
        if comment_id:
            our_comment = await page.evaluate(f"""
                () => {{
                    const el = document.querySelector('[data-id="{comment_id}"]');
                    if (el) {{
                        const textEl = el.querySelector(
                            '.comments-comment__main-content, '
                            '.comments-comment-item__main-content'
                        );
                        return textEl ? textEl.innerText.trim() : '';
                    }}
                    return '';
                }}
            """)

        # Method 2: fallback — search by "Bilal" or "NoError"
        if not our_comment:
            our_comment = await page.evaluate("""
                () => {
                    const items = document.querySelectorAll('.comments-comment-item');
                    for (const item of items) {
                        if (item.innerText.includes('Bilal') ||
                            item.innerText.includes('NoError')) {
                            const textEl = item.querySelector(
                                '.comments-comment__main-content, '
                                '.comments-comment-item__main-content'
                            );
                            return textEl ? textEl.innerText.trim() : '';
                        }
                    }
                    return '';
                }
            """)

        print(f"    Post description: {post_description[:60]}...")
        print(f"    Our comment: {our_comment[:60]}...")
        return {
            "post_description": post_description,
            "our_comment":      our_comment
        }

    except Exception as e:
        print(f"    Post context error: {e}")
        return {"post_description": "", "our_comment": ""}

# ============================================================
# SEND CONNECT WITH NOTE
# ============================================================
async def send_connect_with_note(page, profile_url, note_text):
    try:
        await page.goto(profile_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(4, 6))

        connect_btn = await page.query_selector(
            'button:has-text("Connect"), button[aria-label*="Connect"]'
        )
        if not connect_btn:
            dots = await page.query_selector('button[aria-label*="More"]')
            if dots:
                await dots.click()
                await asyncio.sleep(2)
                connect_btn = await page.query_selector('li:has-text("Connect")')

        if not connect_btn:
            return False

        await connect_btn.click()
        await asyncio.sleep(random.uniform(2, 3))

        add_note_btn = await page.query_selector(
            'button:has-text("Add a note"), button[aria-label*="note"]'
        )
        if add_note_btn:
            await add_note_btn.click()
            await asyncio.sleep(2)

        note_box = await page.query_selector('textarea[name="message"], textarea')
        if note_box:
            await note_box.click()
            await asyncio.sleep(1)
            await human_type(note_box, note_text)
            await asyncio.sleep(random.uniform(2, 3))

        send_btn = await page.query_selector(
            'button:has-text("Send"), button[aria-label*="Send invitation"]'
        )
        if send_btn:
            await send_btn.click()
            await asyncio.sleep(2)
            print(f"  Connect + note sent!")
            return True

        return False

    except Exception as e:
        print(f"  Connect error: {e}")
        return False

# ============================================================
# REPLY TO COMMENT ON POST
# ============================================================
async def reply_to_comment(page, post_url, reply_text, comment_id=None):
    try:
        await page.goto(post_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(5, 7))
        await dismiss_cookie_banner(page)

        # Load all comments first
        for _ in range(5):
            load_more = await page.query_selector(
                'button:has-text("Load more comments"), '
                'button:has-text("Show more comments")'
            )
            if not load_more:
                break
            await load_more.click()
            await asyncio.sleep(2)

        # Find our comment
        our_comment_el = None

        # Method 1: by comment_id
        if comment_id:
            our_comment_el = await page.query_selector(f'[data-id="{comment_id}"]')

        # Method 2: fallback text search
        if not our_comment_el:
            items = await page.query_selector_all('.comments-comment-item')
            for item in items:
                text = await item.inner_text()
                if 'Bilal' in text or 'NoError' in text:
                    our_comment_el = item
                    break

        if not our_comment_el:
            print(f"  Our comment not found!")
            return False

        # Click Reply
        reply_btn = await our_comment_el.query_selector(
            'button:has-text("Reply"), button[aria-label*="Reply"]'
        )
        if not reply_btn:
            return False

        await reply_btn.click()
        await asyncio.sleep(random.uniform(2, 3))

        reply_box = await page.query_selector(
            'div.ql-editor[contenteditable="true"], div[contenteditable="true"]'
        )
        if not reply_box:
            return False

        await page.evaluate("el => { el.click(); el.focus(); }", reply_box)
        await asyncio.sleep(1)
        await human_type(reply_box, reply_text)
        await asyncio.sleep(random.uniform(2, 3))

        # Submit
        for selector in [
            'button.comments-comment-box__submit-button',
            'button[class*="submit"]',
        ]:
            btn = await page.query_selector(selector)
            if btn:
                await page.evaluate("el => el.click()", btn)
                await asyncio.sleep(3)
                print(f"  Comment reply posted!")
                return True

        await reply_box.press("Control+Enter")
        await asyncio.sleep(3)
        print(f"  Comment reply posted via Ctrl+Enter!")
        return True

    except Exception as e:
        print(f"  Reply error: {e}")
        return False

# ============================================================
# REPLY TO DM
# ============================================================
async def reply_to_dm(page, conv_url, reply_text):
    try:
        await page.goto(conv_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(4, 6))

        msg_box = await page.query_selector(
            'div.msg-form__contenteditable, '
            'div[role="textbox"], '
            'div[contenteditable="true"]'
        )
        if not msg_box:
            return False

        await page.evaluate("el => { el.click(); el.focus(); }", msg_box)
        await asyncio.sleep(1)
        await human_type(msg_box, reply_text)
        await asyncio.sleep(random.uniform(2, 3))

        sent = await page.evaluate("""
            () => {
                const btn = Array.from(document.querySelectorAll('button'))
                    .find(b => b.innerText.trim() === 'Send' && !b.disabled);
                if (btn) { btn.click(); return true; }
                return false;
            }
        """)

        await asyncio.sleep(2)
        if sent:
            print(f"  DM reply sent!")
            return True
        return False

    except Exception as e:
        print(f"  DM reply error: {e}")
        return False

# ============================================================
# PROCESS INBOX DMs — WITH FULL HISTORY SCROLL
# ============================================================
async def process_inbox(page):
    print("\n--- Checking Inbox DMs ---")
    try:
        await page.goto("https://www.linkedin.com/messaging/", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(5, 7))

        # Get unread conversations
        unread_convs = await page.evaluate("""
            () => {
                return Array.from(document.querySelectorAll('.msg-conversation-listitem'))
                    .filter(c => c.querySelector('.msg-conversation-listitem__unread-count'))
                    .map(c => ({
                        url:  c.querySelector('a')?.href || '',
                        name: c.querySelector('.msg-conversation-listitem__participant-names')
                                ?.innerText?.trim() || ''
                    }))
                    .filter(c => c.url);
            }
        """)

        print(f"  Unread DMs: {len(unread_convs)}")

        for conv in unread_convs:
            try:
                print(f"\n  Processing: {conv['name']}")

                # ── Step 1: Read FULL DM history from LinkedIn
                dm_history = await read_dm_history(page, conv['url'])

                if not dm_history:
                    continue

                # ── Step 2: Get last client message
                last_client_msg = ""
                for msg in reversed(dm_history):
                    if msg['role'] == 'Client':
                        last_client_msg = msg['text']
                        break

                if not last_client_msg:
                    continue

                print(f"  Last msg: {last_client_msg[:60]}")

                # ── Step 3: Get profile URL
                profile_url = await page.evaluate("""
                    () => {
                        const link = document.querySelector(
                            '.msg-thread__link-to-profile, '
                            'a[href*="/in/"], a[href*="/company/"]'
                        );
                        return link ? link.href.split('?')[0] : null;
                    }
                """)

                if not profile_url:
                    continue

                # ── Step 4: Client type from Supabase or detect from history
                supabase_history = get_supabase_conversation(profile_url)
                client_type = (
                    supabase_history[0].get('client_type', 'Main Client')
                    if supabase_history
                    else get_client_type(' '.join([m['text'] for m in dm_history]))
                )

                # ── Step 5: Detect special requests
                if detect_whatsapp_request(last_client_msg):
                    reply  = "Happy to connect on WhatsApp! Could you share your number? I will reach out right away."
                    signal = "Green"

                elif detect_dm_request(last_client_msg):
                    reply  = "Sure, I am already here in your DMs! What would you like to discuss?"
                    signal = "Green"

                else:
                    signal = detect_signal(last_client_msg)

                    if signal == "Red":
                        print(f"  Red signal — closing")
                        supabase_update("conversations", "profile_url", profile_url,
                                       {"status": "closed"})
                        continue

                    # Case studies if needed
                    case_studies = ""
                    if detect_proof_request(last_client_msg):
                        kw = ("chatbot"  if "chatbot"  in last_client_msg.lower() else
                              "lead"     if "lead"     in last_client_msg.lower() else
                              "workflow" if "workflow" in last_client_msg.lower() else
                              "content")
                        case_studies = get_case_studies(kw)

                    # ── Generate reply with FULL DM context
                    reply = generate_reply(
                        their_message = last_client_msg,
                        context       = {"dm_history": dm_history},
                        client_type   = client_type,
                        case_studies  = case_studies
                    )

                # ── Step 6: Send reply
                success = await reply_to_dm(page, conv['url'], reply)

                if success:
                    supabase_insert("conversations", {
                        "platform":     "linkedin",
                        "profile_url":  profile_url,
                        "message":      last_client_msg,
                        "sender":       "client",
                        "message_type": "dm",
                        "status":       "conversation_started"
                    })
                    supabase_insert("conversations", {
                        "platform":     "linkedin",
                        "profile_url":  profile_url,
                        "client_type":  client_type,
                        "message":      reply,
                        "sender":       "agent",
                        "message_type": "dm",
                        "status":       "conversation_started"
                    })
                    supabase_update("leads_queue", "potential_client_profile", profile_url,
                                   {"status": "warm" if signal == "Green" else "active"})

                    if signal in ["Green", "Yellow"]:
                        notify_manager(signal, profile_url, conv['name'],
                                      client_type, reply, last_client_msg)

                    supabase_insert("agent_logs", {
                        "agent_name": "linkedin_replier",
                        "action":     f"DM replied to {conv['name']} | Signal: {signal}",
                        "details":    profile_url,
                        "status":     "success"
                    })

                await asyncio.sleep(random.uniform(15, 30))

            except Exception as e:
                print(f"  Conv error: {e}")
                continue

    except Exception as e:
        print(f"  Inbox error: {e}")

# ============================================================
# PROCESS COMMENT REPLIES — VIA NOTIFICATIONS
# ============================================================
async def process_notifications(page):
    print("\n--- Checking Notifications ---")
    try:
        await page.goto("https://www.linkedin.com/notifications/", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(5, 7))
        await dismiss_cookie_banner(page)

        # Get all reply notifications
        notifications = await page.evaluate("""
            () => {
                const results = [];
                const items = document.querySelectorAll(
                    '.nt-card, .notification-card, '
                    '[data-urn*="notification"], '
                    '.artdeco-list__item'
                );

                for (const item of items) {
                    const text = item.innerText || '';

                    // Only process "replied to your comment" notifications
                    if (!text.toLowerCase().includes('replied') &&
                        !text.toLowerCase().includes('comment')) {
                        continue;
                    }

                    // Get notification link (post URL)
                    const link = item.querySelector('a[href*="activity"], a[href*="ugcPost"]');
                    const postUrl = link ? link.href.split('?')[0] : '';

                    // Get author name
                    const nameEl = item.querySelector(
                        '.nt-card__headline, .notification-card__headline, '
                        'span[aria-hidden="true"]'
                    );
                    const authorName = nameEl ? nameEl.innerText.trim().split(' ')[0] : '';

                    // Get author profile URL
                    const profileLink = item.querySelector('a[href*="/in/"]');
                    const profileUrl  = profileLink ? profileLink.href.split('?')[0] : '';

                    // Check if unread
                    const isUnread = item.classList.contains('unread') ||
                                     !!item.querySelector('.notification-badge, .unread-indicator');

                    if (postUrl && profileUrl) {
                        results.push({
                            postUrl,
                            profileUrl,
                            authorName,
                            isUnread,
                            notifText: text.substring(0, 100)
                        });
                    }
                }
                return results;
            }
        """)

        # Filter unread only
        unread_notifs = [n for n in notifications if n['isUnread']]
        print(f"  Reply notifications: {len(unread_notifs)} unread")

        for notif in unread_notifs:
            try:
                profile_url = notif['profileUrl']
                post_url    = notif['postUrl']
                author_name = notif['authorName']

                print(f"\n  From: {author_name} | Post: {post_url[:50]}")

                # Already replied? skip
                if is_already_replied(profile_url, "comment_reply"):
                    print(f"  Already replied — skip")
                    continue

                # Get comment_id from Supabase
                supabase_rows = supabase_get("conversations", {
                    "profile_url":  f"eq.{profile_url}",
                    "message_type": "eq.comment",
                    "select":       "*",
                    "limit":        "1"
                })
                comment_id  = supabase_rows[0].get('comment_id', None) if supabase_rows else None
                client_type = supabase_rows[0].get('client_type', 'Main Client') if supabase_rows else 'Main Client'

                # ── Read FULL post context
                post_context = await read_post_context(page, post_url, comment_id)

                # ── Now get their reply text from the post
                their_reply = await page.evaluate(f"""
                    () => {{
                        const items = document.querySelectorAll('.comments-comment-item');
                        for (const item of items) {{
                            const profileLink = item.querySelector('a[href*="/in/"]');
                            if (profileLink && profileLink.href.includes('{profile_url.split("/in/")[-1]}')) {{
                                const textEl = item.querySelector(
                                    '.comments-comment__main-content, '
                                    '.comments-comment-item__main-content'
                                );
                                return textEl ? textEl.innerText.trim() : '';
                            }}
                        }}
                        return '';
                    }}
                """)

                if not their_reply:
                    print(f"  Could not find their reply text — skip")
                    continue

                print(f"  Their reply: {their_reply[:60]}")

                signal = detect_signal(their_reply)

                # Handle special requests
                if detect_dm_request(their_reply):
                    note_text = generate_connect_note(their_reply, client_type)
                    success   = await send_connect_with_note(page, profile_url, note_text)
                    reply_msg = note_text
                    signal    = "Green"

                elif detect_whatsapp_request(their_reply):
                    reply_msg = "Happy to connect on WhatsApp! Could you drop your number here and I will reach out right away?"
                    success   = await reply_to_comment(page, post_url, reply_msg, comment_id)
                    signal    = "Green"

                elif signal == "Red":
                    print(f"  Red signal — closing")
                    supabase_update("conversations", "profile_url", profile_url,
                                   {"status": "closed"})
                    continue

                else:
                    case_stud = ""
                    if detect_proof_request(their_reply):
                        kw = ("chatbot"  if "chatbot"  in their_reply.lower() else
                              "lead"     if "lead"     in their_reply.lower() else
                              "workflow" if "workflow" in their_reply.lower() else
                              "content")
                        case_stud = get_case_studies(kw)

                    # ── Generate reply with FULL post context
                    reply_msg = generate_reply(
                        their_message = their_reply,
                        context       = {
                            "post_description": post_context["post_description"],
                            "our_comment":      post_context["our_comment"]
                        },
                        client_type   = client_type,
                        case_studies  = case_stud
                    )
                    success = await reply_to_comment(page, post_url, reply_msg, comment_id)

                if success:
                    supabase_insert("conversations", {
                        "platform":     "linkedin",
                        "profile_url":  profile_url,
                        "post_url":     post_url,
                        "client_type":  client_type,
                        "message":      their_reply,
                        "sender":       "client",
                        "message_type": "comment_reply",
                        "status":       "conversation_started"
                    })
                    supabase_insert("conversations", {
                        "platform":     "linkedin",
                        "profile_url":  profile_url,
                        "post_url":     post_url,
                        "client_type":  client_type,
                        "message":      reply_msg,
                        "sender":       "agent",
                        "message_type": "comment_reply",
                        "status":       "conversation_started"
                    })
                    supabase_update("conversations", "profile_url", profile_url,
                                   {"status": "conversation_started"})

                    if signal in ["Green", "Yellow"]:
                        notify_manager(signal, profile_url, author_name,
                                      client_type, reply_msg, their_reply)

                    supabase_insert("agent_logs", {
                        "agent_name": "linkedin_replier",
                        "action":     f"Comment replied to {author_name} | Signal: {signal}",
                        "details":    profile_url,
                        "status":     "success"
                    })

                await asyncio.sleep(random.uniform(15, 25))

            except Exception as e:
                print(f"  Notification error: {e}")
                continue

    except Exception as e:
        print(f"  Notifications error: {e}")

# ============================================================
# MAIN
# ============================================================
async def run_replier():
    print(f"\n{'='*50}")
    print(f"LinkedIn Replier Started")
    print(f"{'='*50}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768}
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        await context.add_cookies(get_cookies())
        page = await context.new_page()

        # Login check
        await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
        await asyncio.sleep(3)
        if "feed" not in page.url:
            print("Session expired — refresh cookies!")
            await browser.close()
            return

        print("Session valid!\n")

        # ── Run both
        await process_inbox(page)       # DMs — full history scroll
        await process_notifications(page)  # Comment replies — via notifications

        print(f"\n{'='*50}")
        print(f"Replier Done!")
        print(f"{'='*50}")
        await browser.close()

asyncio.run(run_replier())
