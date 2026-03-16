# ============================================================
# linkedin_replier.py
# NoErrors AI Automation Agency — LinkedIn Replier Agent
# Handles ALL replies — DMs + Comment replies
# Watcher handles first outreach ONLY
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

def get_conversation(profile_url):
    rows = supabase_get("conversations", {
        "profile_url": f"eq.{profile_url}",
        "select":      "*",
        "order":       "timestamp.desc",
        "limit":       "10"
    })
    return rows

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

def notify_manager(signal, profile_url, client_name, client_type, reply_text, their_message):
    supabase_insert("agent_signals", {
        "from_agent":  "linkedin_replier",
        "to_agent":    "manager_agent",
        "signal_type": f"{signal.lower()}_alert",
        "payload":     str({
            "platform":     "linkedin",
            "client_name":  client_name,
            "profile_url":  profile_url,
            "client_type":  client_type,
            "their_message": their_message,
            "our_reply":    reply_text,
            "signal":       signal
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
    text = text.lower()
    return any(x in text for x in ["dm me", "message me", "inbox me",
                                    "send me a message", "private message",
                                    "direct message"])

def detect_whatsapp_request(text):
    text = text.lower()
    return any(x in text for x in ["whatsapp", "whats app", "wa me",
                                    "contact on whatsapp", "whatsapp me"])

def detect_proof_request(text):
    text = text.lower()
    return any(x in text for x in ["proof", "results", "case study", "portfolio",
                                    "past work", "examples", "references",
                                    "have you done", "show me", "experience"])

def generate_reply(their_message, conversation_history, client_type, case_studies=""):
    history_text = ""
    for msg in conversation_history[-6:]:
        role          = "Bilal" if msg.get("sender") == "agent" else "Client"
        history_text += f"{role}: {msg.get('message','')}\n"

    extra = f"\n\nRelevant case studies:\n{case_studies}" if case_studies else ""
    temp  = 0.3 if client_type == "Opportunity" else 0.5

    reply = call_openai([
        {"role": "system", "content": SYSTEM_PROMPT + extra},
        {"role": "user",   "content": (
            f"Conversation history:\n{history_text}\n\n"
            f"Client just said: {their_message}\n"
            f"Client type: {client_type}\n\n"
            f"Write next reply. Max 3 sentences. Plain text only. "
            f"No price. Continue conversation naturally."
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
            f"Max 280 chars. Plain text only. Sparky and curious."
        )}
    ], max_tokens=80, temperature=0.6)
    note = note.replace("**","").replace("*","").replace("#","").replace("\n"," ")
    return note[:280] if len(note) > 280 else note

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

# ============================================================
# SEND CONNECT REQUEST WITH NOTE
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
            print(f"  Connect button not found")
            return False

        await connect_btn.click()
        await asyncio.sleep(random.uniform(2, 3))

        # Add note button
        add_note_btn = await page.query_selector(
            'button:has-text("Add a note"), button[aria-label*="note"]'
        )
        if add_note_btn:
            await add_note_btn.click()
            await asyncio.sleep(2)

        note_box = await page.query_selector(
            'textarea[name="message"], textarea#custom-message'
        )
        if not note_box:
            note_box = await page.query_selector('textarea')

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
# REPLY TO COMMENT
# ============================================================
async def reply_to_comment(page, post_url, reply_text):
    try:
        await page.goto(post_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(5, 7))

        # Find our comment and click Reply
        our_comment = await page.query_selector(
            '.comments-comment-item:has-text("Bilal"), '
            '.comments-comment-item:has-text("NoError")'
        )

        if not our_comment:
            # Fallback — find first Reply button
            our_comment = await page.query_selector('.comments-comment-item')

        if not our_comment:
            print(f"  Our comment not found on post")
            return False

        reply_btn = await our_comment.query_selector(
            'button:has-text("Reply"), button[aria-label*="Reply"]'
        )
        if not reply_btn:
            print(f"  Reply button not found")
            return False

        await reply_btn.click()
        await asyncio.sleep(random.uniform(2, 3))

        reply_box = await page.query_selector(
            'div.ql-editor, div[contenteditable="true"]'
        )
        if not reply_box:
            return False

        await reply_box.click()
        await asyncio.sleep(1)
        await human_type(reply_box, reply_text)
        await asyncio.sleep(random.uniform(2, 3))

        submit_btn = await page.query_selector(
            'button.comments-comment-box__submit-button'
        )
        if not submit_btn:
            await reply_box.press("Control+Return")
        else:
            await submit_btn.click()

        await asyncio.sleep(3)
        print(f"  Comment reply posted!")
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

        await msg_box.click()
        await asyncio.sleep(1)
        await human_type(msg_box, reply_text)
        await asyncio.sleep(random.uniform(2, 3))

        send_btn = await page.evaluate("""
            () => {
                const btn = Array.from(document.querySelectorAll('button'))
                    .find(b => b.innerText.trim() === 'Send' && !b.disabled);
                if (btn) { btn.click(); return true; }
                return false;
            }
        """)

        await asyncio.sleep(2)
        if send_btn:
            print(f"  DM reply sent!")
            return True
        return False

    except Exception as e:
        print(f"  DM reply error: {e}")
        return False

# ============================================================
# PROCESS INBOX DMs
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
                await page.goto(conv['url'], wait_until="domcontentloaded")
                await asyncio.sleep(random.uniform(4, 6))

                # Get profile URL from conversation
                profile_url = await page.evaluate("""
                    () => {
                        const link = document.querySelector(
                            '.msg-thread__link-to-profile, a[href*="/in/"], a[href*="/company/"]'
                        );
                        return link ? link.href.split('?')[0] : null;
                    }
                """)

                if not profile_url:
                    continue

                # Get last client message
                messages = await page.evaluate("""
                    () => {
                        return Array.from(document.querySelectorAll('.msg-s-message-list__event'))
                            .map(m => ({
                                text:    m.querySelector('.msg-s-event-listitem__body')?.innerText?.trim() || '',
                                is_mine: !!m.querySelector('.msg-s-message-group__meta')
                            }))
                            .filter(m => m.text);
                    }
                """)

                if not messages:
                    continue

                # Find last client message
                last_client_msg = ""
                for msg in reversed(messages):
                    if not msg['is_mine']:
                        last_client_msg = msg['text']
                        break

                if not last_client_msg:
                    continue

                print(f"  From: {conv['name']} | Message: {last_client_msg[:60]}")

                # Get conversation history from Supabase
                history     = get_conversation(profile_url)
                client_type = history[0].get('client_type', 'Main Client') if history else 'Main Client'

                # Detect special requests
                if detect_whatsapp_request(last_client_msg):
                    reply = "Happy to connect on WhatsApp! Could you share your number? I will reach out right away."
                    signal = "Green"

                elif detect_dm_request(last_client_msg):
                    reply = "Sure, I am already here in your DMs! What would you like to discuss?"
                    signal = "Green"

                else:
                    # Detect signal
                    signal = detect_signal(last_client_msg)

                    # Case studies if needed
                    case_studies = ""
                    if detect_proof_request(last_client_msg):
                        service_kw   = "chatbot" if "chatbot" in last_client_msg.lower() else \
                                       "lead" if "lead" in last_client_msg.lower() else \
                                       "workflow" if "workflow" in last_client_msg.lower() else \
                                       "content"
                        case_studies = get_case_studies(service_kw)

                    if signal == "Red":
                        print(f"  Red signal — closing")
                        supabase_update("conversations", "profile_url", profile_url,
                                       {"status": "closed"})
                        supabase_insert("agent_logs", {
                            "agent_name": "linkedin_replier",
                            "action":     f"Red signal from {conv['name']}",
                            "details":    profile_url,
                            "status":     "closed"
                        })
                        continue

                    reply = generate_reply(last_client_msg, history, client_type, case_studies)

                # Send reply
                success = await reply_to_dm(page, conv['url'], reply)

                if success:
                    # Save to Supabase
                    supabase_insert("conversations", {
                        "platform":    "linkedin",
                        "profile_url": profile_url,
                        "message":     last_client_msg,
                        "sender":      "client",
                        "message_type": "dm",
                        "status":      "conversation_started"
                    })
                    supabase_insert("conversations", {
                        "platform":    "linkedin",
                        "profile_url": profile_url,
                        "client_type": client_type,
                        "message":     reply,
                        "sender":      "agent",
                        "message_type": "dm",
                        "status":      "conversation_started"
                    })

                    # Update lead status
                    supabase_update("leads_queue", "potential_client_profile", profile_url,
                                   {"status": "warm" if signal == "Green" else "active"})

                    # Notify manager if Green or Yellow
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
# PROCESS COMMENT REPLIES
# ============================================================
async def process_comment_replies(page):
    print("\n--- Checking Comment Replies ---")
    try:
        # Get all posts where we commented from Supabase
        commented_posts = supabase_get("conversations", {
            "message_type": "eq.comment",
            "status":       "eq.approval_sent",
            "select":       "*"
        })

        print(f"  Posts to check: {len(commented_posts)}")

        for conv in commented_posts:
            post_url    = conv.get('post_url', '')
            profile_url = conv.get('profile_url', '')
            client_type = conv.get('client_type', 'Main Client')

            if not post_url:
                continue

            try:
                await page.goto(post_url, wait_until="domcontentloaded")
                await asyncio.sleep(random.uniform(5, 7))

                # Get replies on our comment
                replies = await page.evaluate("""
                    () => {
                        const ourComment = Array.from(
                            document.querySelectorAll('.comments-comment-item')
                        ).find(c =>
                            c.innerText.includes('NoError') ||
                            c.innerText.includes('Bilal')
                        );

                        if (!ourComment) return [];

                        const replyItems = ourComment.querySelectorAll(
                            '.comments-comment-item, .comments-reply-item'
                        );

                        return Array.from(replyItems).map(r => ({
                            text:       r.querySelector('.comments-comment__main-content')
                                         ?.innerText?.trim() || '',
                            authorName: r.querySelector('.comments-post-meta__name-text')
                                         ?.innerText?.trim() || '',
                            authorUrl:  r.querySelector('a[href*="/in/"]')
                                         ?.href?.split('?')[0] || ''
                        })).filter(r => r.text && !r.text.includes('Bilal')
                                                && !r.text.includes('NoError'));
                    }
                """)

                if not replies:
                    continue

                print(f"  {len(replies)} replies on post")

                for reply_item in replies:
                    their_msg    = reply_item['text']
                    author_name  = reply_item['authorName']
                    author_url   = reply_item['authorUrl'] or profile_url

                    print(f"  Reply from {author_name}: {their_msg[:60]}")

                    # Check if already replied
                    existing = supabase_get("conversations", {
                        "profile_url": f"eq.{author_url}",
                        "sender":      "eq.agent",
                        "message_type": "eq.comment_reply",
                        "select":      "conv_id"
                    })
                    if existing:
                        print(f"  Already replied — skip")
                        continue

                    signal = detect_signal(their_msg)

                    # Handle special requests
                    if detect_dm_request(their_msg):
                        note_text = generate_connect_note(their_msg, client_type)
                        print(f"  DM request detected — sending connect + note")
                        success   = await send_connect_with_note(page, author_url, note_text)
                        reply_msg = note_text
                        signal    = "Green"

                    elif detect_whatsapp_request(their_msg):
                        reply_msg = "Happy to connect on WhatsApp! Could you drop your number here and I will reach out right away?"
                        await reply_to_comment(page, post_url, reply_msg)
                        success   = True
                        signal    = "Green"

                    elif signal == "Red":
                        print(f"  Red signal — skip")
                        supabase_update("conversations", "profile_url", profile_url,
                                       {"status": "closed"})
                        continue

                    else:
                        history   = get_conversation(author_url) or get_conversation(profile_url)
                        case_stud = ""
                        if detect_proof_request(their_msg):
                            case_stud = get_case_studies("chatbot" if "chatbot" in their_msg.lower()
                                                         else "lead" if "lead" in their_msg.lower()
                                                         else "workflow")
                        reply_msg = generate_reply(their_msg, history, client_type, case_stud)
                        success   = await reply_to_comment(page, post_url, reply_msg)

                    if success:
                        # Save messages
                        supabase_insert("conversations", {
                            "platform":    "linkedin",
                            "profile_url": author_url,
                            "post_url":    post_url,
                            "client_type": client_type,
                            "message":     their_msg,
                            "sender":      "client",
                            "message_type": "comment_reply",
                            "status":      "conversation_started"
                        })
                        supabase_insert("conversations", {
                            "platform":    "linkedin",
                            "profile_url": author_url,
                            "post_url":    post_url,
                            "client_type": client_type,
                            "message":     reply_msg,
                            "sender":      "agent",
                            "message_type": "comment_reply",
                            "status":      "conversation_started"
                        })

                        # Update original conversation status
                        supabase_update("conversations", "profile_url", profile_url,
                                       {"status": "conversation_started"})

                        # Notify manager
                        if signal in ["Green", "Yellow"]:
                            notify_manager(signal, author_url, author_name,
                                          client_type, reply_msg, their_msg)

                        supabase_insert("agent_logs", {
                            "agent_name": "linkedin_replier",
                            "action":     f"Comment replied to {author_name} | Signal: {signal}",
                            "details":    author_url,
                            "status":     "success"
                        })

                    await asyncio.sleep(random.uniform(15, 25))

            except Exception as e:
                print(f"  Post check error: {e}")
                continue

    except Exception as e:
        print(f"  Comment replies error: {e}")

# ============================================================
# MAIN REPLIER
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

        # Run both checks
        await process_inbox(page)
        await process_comment_replies(page)

        print(f"\n{'='*50}")
        print(f"Replier Done!")
        print(f"{'='*50}")
        await browser.close()

# ============================================================
# RUN
# ============================================================
asyncio.run(run_replier())
