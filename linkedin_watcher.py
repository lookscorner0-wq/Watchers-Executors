# ============================================================
# linkedin_watcher.py — FINAL VERSION
# ============================================================

import os
import asyncio
import random
import requests
from playwright.async_api import async_playwright

LI_AT         = os.environ.get("LI_AT", "")
LI_JSESSIONID = os.environ.get("LI_JSESSIONID", "")
OPENAI_KEY    = os.environ.get("OPENAI_KEY", "")
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")

MAX_ACTIONS_PER_RUN = 5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.0.0 Safari/537.36",
]

SYSTEM_PROMPT = """
You are Bilal Dev, sales manager for NoErrors — an AI Automation Agency.
Your role is to find potential clients on LinkedIn who need our services and convert them through smart, human, non-salesy conversation.

SERVICES WE OFFER:
1. Lead Generation (AI-powered outreach, LinkedIn automation, email campaigns, B2B database scraping)
2. Social Media Content Marketing (AI content calendar, auto-posting, LinkedIn/Instagram/Facebook/TikTok)
3. AI Chatbots and Agents (customer support bots, appointment bots, WhatsApp bots, website chatbots)
4. Custom Workflows (N8N, Make, Zapier, Pabbly automation setup and management)

CLIENT TYPES:
- Main Client: Small business, solo founder, startup — casual friendly tone, curiosity hooks, humor allowed, emojis allowed
- GoodClient: Growing company, SaaS, agency, mid-size — professional but warm, mirror their technical language, free audit offer works well
- Opportunity: Enterprise, Fortune, multinational, large corporation — zero emojis, formal tone, lead with credibility and case study, push for discovery call always

HARD RULES:
1. NEVER share price in first or second message
2. NEVER sound salesy — always sound helpful and specific
3. NEVER use markdown, bold, bullets, or formatting — plain text only
4. NEVER reveal you are an AI
5. NEVER mention services client did not ask for in first message
6. NEVER send more than 180 characters in first outreach
7. NEVER use emojis with Opportunity clients

WHAT WORKS:
1. Mirror exact technical language from their post
2. Frame hook around what client is LOSING not what we offer
3. Include specific result number when possible
4. For Main Client — curiosity question hook, casual tone
5. For GoodClient — lead with free audit offer
6. For Opportunity — open with enterprise reference and specific result
7. Never start with Hi I am Bilal — start with their pain point

OUTPUT FORMAT FOR COMMENTS:
- Plain text only
- Maximum 180 characters
- No hashtags
- End with soft question

OUTPUT FORMAT FOR DMs:
- Plain text only
- Maximum 2 sentences
- Start with their pain point
"""

# ============================================================
# KEYWORD GENERATOR
# ============================================================
def generate_keywords():
    KEYWORDS_POOL = [
        "need AI automation",
        "need lead generation",
        "need chatbot developer",
        "looking for workflow automation",
        "need leads funnel expert",
        "hiring ai agent developer",
        "need social media automation",
        "need social media manager",
        "need CRM automation",
        "need customer support bot",
        "need ai integration",
    ]
    keywords = random.sample(KEYWORDS_POOL, 4)
    print(f"Keywords this run: {keywords}")
    return keywords
# ============================================================
# 24 HOUR FILTER
# ============================================================
def is_post_recent(time_text):
    if not time_text:
        return False
    t = time_text.lower().strip()
    # Minutes = always recent
    if 'm' in t and 'h' not in t and 'd' not in t:
        return True
    # Hours — max 23
    if 'h' in t:
        try:
            return int(''.join(filter(str.isdigit, t))) <= 23
        except:
            return True
    # Only 1d accepted
    if '1d' in t:
        return True
    return False

# ============================================================
# SUPABASE
# ============================================================
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
            json=data, timeout=10
        )
        return res.status_code in [200, 201]
    except Exception as e:
        print(f"Supabase error: {e}")
        return False

def is_already_contacted(profile_url):
    try:
        res = requests.get(
            f"{SUPABASE_URL}/rest/v1/conversations",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
            },
            params={"profile_url": f"eq.{profile_url}", "select": "conv_id"},
            timeout=10
        )
        return len(res.json()) > 0
    except:
        return False

# ============================================================
# OPENAI
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

def get_client_type(text):
    text = text.lower()
    if any(x in text for x in ["enterprise","fortune","global","multinational","corporate"]):
        return "Opportunity"
    if any(x in text for x in ["startup","saas","growing","series a","scale up","agency"]):
        return "GoodClient"
    return "Main Client"

def is_relevant(post_text):
    result = call_openai([
        {"role": "system", "content": (
            "You are a lead qualifier for an AI Automation Agency. "
            "Reply ONLY: relevant: yes OR relevant: no. "
            "relevant: yes if the person mentions a business problem, "
            "pain point, or need that AI automation, chatbots, lead generation, "
            "or workflow tools could solve — even if they are not explicitly hiring. "
            "relevant: no ONLY if it is purely a tip, tutorial, job listing, or self-promotion."
        )},
        {"role": "user", "content": f"Post: {post_text[:200]}"}
    ], max_tokens=10, temperature=0.1)
    return "relevant: yes" in result.lower()

def generate_comment(post_text, client_type):
    temp    = 0.7 if client_type == "Main Client" else 0.4
    comment = call_openai([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": (
            f"Write a LinkedIn comment. Client type: {client_type}.\n"
            f"Post: {post_text[:200]}\n"
            f"Max 180 chars. Plain text. End with question. No hashtags."
        )}
    ], max_tokens=80, temperature=temp)
    comment = comment.replace("**","").replace("*","").replace("#","").replace("\n"," ")
    return comment[:180]

def generate_dm(post_text, client_type):
    temp = 0.3 if client_type == "Opportunity" else 0.5
    dm   = call_openai([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": (
            f"Write a LinkedIn DM. Client type: {client_type}.\n"
            f"Post: {post_text[:200]}\n"
            f"Max 2 sentences. Plain text. Start with pain point."
        )}
    ], max_tokens=100, temperature=temp)
    return dm.replace("**","").replace("*","").replace("#","").replace("\n"," ")

# ============================================================
# BROWSER
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
        await element.type(char, delay=random.randint(50, 100))

async def safe_goto(page, url):
    for attempt in range(2):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
            return True
        except Exception as e:
            print(f"  Goto attempt {attempt+1} failed: {e}")
            await asyncio.sleep(2)
    return False

# ============================================================
# COMMENT
# ============================================================
async def comment_on_post(page, card, comment_text):
    try:
        await card.scroll_into_view_if_needed()
        await asyncio.sleep(1)

        comment_btn = await card.query_selector(
            'button[aria-label*="comment"], button:has-text("Comment")'
        )
        if not comment_btn:
            print(f"  Comment button not found")
            return False

        await comment_btn.click()
        await asyncio.sleep(2)

        comment_box = await page.query_selector(
            'div.ql-editor, div[contenteditable="true"]'
        )
        if not comment_box:
            print(f"  Comment box not found")
            return False

        # JS click — bypass interception
        await page.evaluate("el => el.click()", comment_box)
        await asyncio.sleep(1)
        await human_type(comment_box, comment_text)
        await asyncio.sleep(2)

        submit_btn = await page.query_selector(
            'button.comments-comment-box__submit-button'
        )
        if not submit_btn:
            await comment_box.press("Control+Enter")
        else:
            await page.evaluate("el => el.click()", submit_btn)

        await asyncio.sleep(2)
        print(f"  Comment posted!")
        return True

    except Exception as e:
        print(f"  Comment error: {e}")
        return False

# ============================================================
# DM
# ============================================================
async def dm_company(page, profile_url, dm_text):
    try:
        ok = await safe_goto(page, profile_url)
        if not ok:
            return False

        # Message button
        msg_btn = await page.query_selector(
            'button:has-text("Message"), a:has-text("Message")'
        )
        if not msg_btn:
            dots = await page.query_selector('button[aria-label*="More"]')
            if dots:
                await page.evaluate("el => el.click()", dots)
                await asyncio.sleep(1)
                msg_btn = await page.query_selector(
                    'li:has-text("Message"), div:has-text("Send message")'
                )
        if not msg_btn:
            print(f"  Message button not found")
            return False

        await page.evaluate("el => el.click()", msg_btn)
        await asyncio.sleep(2)

        # Topic dropdown
        for topic in ["Service Request", "Request a Demo", "General Inquiry"]:
            try:
                opt = await page.query_selector(
                    f'option:has-text("{topic}"), li:has-text("{topic}")'
                )
                if opt:
                    await page.evaluate("el => el.click()", opt)
                    await asyncio.sleep(1)
                    print(f"  Topic: {topic}")
                    break
            except:
                continue

        # Message box — try multiple selectors
        msg_box = await page.query_selector('div[aria-label="Write a message…"]')
        if not msg_box:
            msg_box = await page.query_selector('div.msg-form__contenteditable')
        if not msg_box:
            msg_box = await page.query_selector('textarea')
        if not msg_box:
            print(f"  Message box not found")
            return False

        # JS click + focus
        await page.evaluate("el => { el.click(); el.focus(); }", msg_box)
        await asyncio.sleep(1)
        await human_type(msg_box, dm_text)
        await asyncio.sleep(2)

        # Send button
        send_btn = await page.query_selector(
            'button.msg-form__send-button, '
            'button[aria-label="Send"], '
            'button:has-text("Send message")'
        )
        if not send_btn:
            send_btn = await page.query_selector('button:has-text("Send")')

        if send_btn:
            await page.evaluate("el => el.click()", send_btn)
            await asyncio.sleep(2)
            print(f"  DM sent!")
            return True

        print(f"  Send button not found")
        return False

    except Exception as e:
        print(f"  DM error: {e}")
        return False

# ============================================================
# MAIN
# ============================================================
async def run_watcher():
    print(f"\n{'='*50}")
    print(f"LinkedIn Watcher Started")
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
        ok = await safe_goto(page, "https://www.linkedin.com/feed/")
        if not ok or "feed" not in page.url:
            print("Session expired — refresh cookies!")
            await browser.close()
            return

        print("Session valid!\n")
        actions_done = 0
        KEYWORDS = generate_keywords()
        for keyword in KEYWORDS:

            print(f"\nSearching: '{keyword}'")
            search_url = (
                f"https://www.linkedin.com/search/results/content/"
                f"?keywords={keyword.replace(' ', '%20')}&sortBy=date_posted"
            )

            ok = await safe_goto(page, search_url)
            if not ok:
                print(f"  Search failed — skip")
                continue

            await asyncio.sleep(4)
            for _ in range(2):
                await page.evaluate("window.scrollBy(0, 600)")
                await asyncio.sleep(1)

            cards = await page.query_selector_all('.occludable-update')
            print(f"  Found {len(cards)} posts")

            i = 0
            while i < len(cards):
                if actions_done >= MAX_ACTIONS_PER_RUN:
                    break

                card = cards[i]
                i   += 1

                try:
                    # 24 hour filter
                    time_el   = await card.query_selector(
                        '.update-components-actor__sub-description '
                        'span:not(.visually-hidden)'
                    )
                    time_text = await time_el.inner_text() if time_el else ""
                    time_text = time_text.strip().split('•')[0].strip()

                    if not is_post_recent(time_text):
                        print(f"  Old ({time_text}) — skip")
                        continue

                    # Post text
                    text_el   = await card.query_selector('span.break-words')
                    post_text = await text_el.inner_text() if text_el else ""
                    if not post_text:
                        continue

                    # Profile URL
                    links       = await card.query_selector_all('a')
                    profile_url = ""
                    is_company  = False
                    for link in links:
                        href = await link.get_attribute('href') or ''
                        if '/in/' in href:
                            profile_url = href.split('?')[0]
                            break
                        if '/company/' in href:
                            profile_url = href.split('?')[0]
                            is_company  = True
                            break

                    if not profile_url:
                        continue

                    # Duplicate check
                    if is_already_contacted(profile_url):
                        print(f"  Already contacted — skip")
                        continue

                    # GPT qualify
                    if not is_relevant(post_text):
                        print(f"  Not relevant — skip")
                        continue

                    client_type = get_client_type(post_text)
                    author_el   = await card.query_selector(
                        '.update-components-actor__title span:not(.visually-hidden)'
                    )
                    author_name = await author_el.inner_text() if author_el else "Unknown"
                    author_name = author_name.strip().split('\n')[0]

                    print(f"  {author_name} | {client_type} | "
                          f"Company:{is_company} | {time_text}")

                    post_url = page.url.split('?')[0]
                    success  = False

                    if is_company:
                        dm_text  = generate_dm(post_text, client_type)
                        print(f"  → DM: {dm_text[:80]}...")
                        success  = await dm_company(page, profile_url, dm_text)
                        message  = dm_text
                        msg_type = "dm"

                        # Reload search after DM
                        await safe_goto(page, search_url)
                        await asyncio.sleep(4)
                        for _ in range(2):
                            await page.evaluate("window.scrollBy(0, 600)")
                            await asyncio.sleep(1)
                        cards = await page.query_selector_all('.occludable-update')

                    else:
                        comment_text = generate_comment(post_text, client_type)
                        print(f"  → Comment: {comment_text[:80]}...")
                        success  = await comment_on_post(page, card, comment_text)
                        message  = comment_text
                        msg_type = "comment"

                       comment_id = await page.evaluate("""
                           () => {
                               const items = document.querySelectorAll('.comments-comment-item');
                               for (const c of items) {
                                   if (c.getAttribute('data-id')) return c.getAttribute('data-id');
                               }
                               return null;
                           }
                       """)
                       print(f"  Comment ID: {comment_id}")
                    
                    if success:
                        actions_done += 1

                        supabase_insert("leads_queue", {
                            "platform":                 "linkedin",
                            "potential_client_name":    author_name,
                            "potential_client_profile": profile_url,
                            "post_content":             post_text[:500],
                            "post_url":                 post_url,
                            "assigned_to":              "linkedin_watcher",
                            "status":                   "contacted"
                        })
                        supabase_insert("conversations", {
                            "platform":    "linkedin",
                            "profile_url": profile_url,
                            "post_url":    post_url,
                            "client_type": client_type,
                            "message":     message,
                            "sender":      "agent",
                            "message_type": msg_type,
                            "status":      "approval_sent"
                        })
                        supabase_insert("agent_logs", {
                            "agent_name": "linkedin_watcher",
                            "action":     f"{'DM' if is_company else 'Comment'} "
                                         f"to {author_name}",
                            "details":    profile_url,
                            "status":     "success"
                        })

                        print(f"  Saved! {actions_done}/{MAX_ACTIONS_PER_RUN}\n")
                        await asyncio.sleep(random.uniform(8, 12))

                except Exception as e:
                    print(f"  Error: {e}")
                    continue

            await asyncio.sleep(3)

        print(f"\n{'='*50}")
        print(f"Done! Actions: {actions_done}")
        print(f"{'='*50}")
        await browser.close()

asyncio.run(run_watcher())
