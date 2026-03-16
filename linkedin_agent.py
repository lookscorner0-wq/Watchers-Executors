# ============================================================
# linkedin_watcher.py
# NoErrors AI Automation Agency — LinkedIn Watcher Agent
# First outreach ONLY — comments + DMs
# Replier handles everything after
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

MAX_ACTIONS_PER_RUN = 10

# ============================================================
# KEYWORDS
# ============================================================
KEYWORDS = [
    "need lead generation",
    "looking for automation",
    "need chatbot",
    "want AI agent",
    "hire marketing agency",
    "need social media manager",
    "looking for web automation",
    "need workflow automation",
    "AI automation help",
    "need outreach automation",
]

# ============================================================
# USER AGENTS ROTATION
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

HOW TO CLASSIFY CLIENT:
- Opportunity: words like enterprise, fortune, global, multinational, 100 plus employees, corporate
- GoodClient: words like startup, SaaS, growing, series A, scale up, agency
- Main Client: everything else

HARD RULES — NEVER BREAK THESE:
1. NEVER share price in first or second message
2. NEVER sound salesy — always sound helpful and specific
3. NEVER use markdown, bold, bullets, or formatting — plain text only
4. NEVER reveal you are an AI
5. NEVER mention services client did not ask for in first message
6. NEVER promise delivery timeline before scoping
7. NEVER reveal team size
8. NEVER give price to Opportunity client
9. NEVER send more than 180 characters in first outreach
10. NEVER use emojis with Opportunity clients
11. NEVER agree to free trial — offer free audit instead
12. NEVER compete on price — compete on value and ROI

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
            json=data,
            timeout=10
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
    if any(x in text for x in ["enterprise", "fortune", "global", "multinational", "corporate"]):
        return "Opportunity"
    if any(x in text for x in ["startup", "saas", "growing", "series a", "scale up", "agency"]):
        return "GoodClient"
    return "Main Client"

def is_relevant(post_text):
    result = call_openai([
        {"role": "system", "content": "You are a lead qualification expert for an AI Automation Agency. Reply ONLY: relevant: yes OR relevant: no"},
        {"role": "user",   "content": f"Is this post from someone who might need AI automation, chatbots, lead generation, or workflow automation services?\n\nPost: {post_text[:300]}"}
    ], max_tokens=10, temperature=0.1)
    return "relevant: yes" in result.lower()

def generate_comment(post_text, client_type):
    temp    = 0.7 if client_type == "Main Client" else 0.4
    comment = call_openai([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": (
            f"Write a LinkedIn comment for this post. Client type: {client_type}.\n"
            f"Post: {post_text[:300]}\n\n"
            f"Rules: Plain text only. Maximum 180 chars. "
            f"End with soft question. No hashtags. No emojis for Opportunity."
        )}
    ], max_tokens=80, temperature=temp)
    comment = comment.replace("**","").replace("*","").replace("#","").replace("\n"," ")
    return comment[:180] if len(comment) > 180 else comment

def generate_dm(post_text, client_type):
    temp = 0.3 if client_type == "Opportunity" else 0.5
    dm   = call_openai([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": (
            f"Write a LinkedIn DM for this post. Client type: {client_type}.\n"
            f"Post: {post_text[:300]}\n\n"
            f"Rules: Plain text only. Max 2 sentences. "
            f"Start with their pain point. No price. No emojis for Opportunity."
        )}
    ], max_tokens=100, temperature=temp)
    return dm.replace("**","").replace("*","").replace("#","").replace("\n"," ")

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
# COMMENT ON PERSONAL POST
# ============================================================
async def comment_on_post(page, card, comment_text):
    try:
        await card.scroll_into_view_if_needed()
        await asyncio.sleep(random.uniform(2, 3))

        comment_btn = await card.query_selector(
            'button[aria-label*="comment"], button:has-text("Comment")'
        )
        if not comment_btn:
            return False, ""

        await comment_btn.click()
        await asyncio.sleep(random.uniform(2, 3))

        comment_box = await page.query_selector('div.ql-editor, div[contenteditable="true"]')
        if not comment_box:
            return False, ""

        await comment_box.click()
        await asyncio.sleep(1)
        await human_type(comment_box, comment_text)
        await asyncio.sleep(random.uniform(2, 3))

        submit_btn = await page.query_selector('button.comments-comment-box__submit-button')
        if not submit_btn:
            await comment_box.press("Control+Return")
        else:
            await submit_btn.click()

        await asyncio.sleep(3)

        # Get post URL after comment
        post_url = page.url.split('?')[0]
        print(f"  Comment posted!")
        return True, post_url

    except Exception as e:
        print(f"  Comment error: {e}")
        return False, ""

# ============================================================
# DM ON COMPANY PAGE
# ============================================================
async def dm_company(page, profile_url, dm_text):
    try:
        await page.goto(profile_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(4, 6))

        msg_btn = await page.query_selector(
            'button:has-text("Message"), a:has-text("Message")'
        )
        if not msg_btn:
            dots = await page.query_selector('button[aria-label*="More"]')
            if dots:
                await dots.click()
                await asyncio.sleep(2)
                msg_btn = await page.query_selector(
                    'li:has-text("Message"), div:has-text("Send message")'
                )

        if not msg_btn:
            return False

        await msg_btn.click()
        await asyncio.sleep(random.uniform(3, 4))

        # Topic dropdown — Service Request → Request a Demo → General Inquiry
        topic_options = ["Service Request", "Request a Demo", "General Inquiry"]
        for topic in topic_options:
            try:
                option = await page.query_selector(
                    f'option:has-text("{topic}"), li:has-text("{topic}")'
                )
                if option:
                    await option.click()
                    await asyncio.sleep(1)
                    break
            except:
                continue

        msg_box = await page.query_selector('textarea, div[contenteditable="true"]')
        if not msg_box:
            return False

        await msg_box.click()
        await asyncio.sleep(1)
        await human_type(msg_box, dm_text)
        await asyncio.sleep(random.uniform(2, 3))

        send_btn = await page.query_selector(
            'button:has-text("Send message"), button:has-text("Send")'
        )
        if send_btn:
            await send_btn.click()
            await asyncio.sleep(2)
            print(f"  DM sent!")
            return True

        return False

    except Exception as e:
        print(f"  DM error: {e}")
        return False

# ============================================================
# MAIN WATCHER
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
        await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
        await asyncio.sleep(3)
        if "feed" not in page.url:
            print("Session expired — refresh cookies!")
            await browser.close()
            return

        print("Session valid!\n")
        actions_done = 0

        for keyword in KEYWORDS:
            if actions_done >= MAX_ACTIONS_PER_RUN:
                break

            print(f"Searching: '{keyword}'")
            search_url = f"https://www.linkedin.com/search/results/content/?keywords={keyword.replace(' ', '%20')}&sortBy=date_posted"
            await page.goto(search_url, wait_until="networkidle")
            await asyncio.sleep(8)

            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 600)")
                await asyncio.sleep(2)

            cards = await page.query_selector_all('.occludable-update')
            print(f"  Found {len(cards)} posts\n")

            for card in cards:
                if actions_done >= MAX_ACTIONS_PER_RUN:
                    break

                try:
                    # Get post text
                    text_el   = await card.query_selector('span.break-words')
                    post_text = await text_el.inner_text() if text_el else ""
                    if not post_text:
                        continue

                    # Get profile URL + check company
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

                    # GPT qualify check
                    if not is_relevant(post_text):
                        print(f"  Not relevant — skip")
                        continue

                    client_type = get_client_type(post_text)

                    # Get author name
                    author_el   = await card.query_selector(
                        '.update-components-actor__title span:not(.visually-hidden)'
                    )
                    author_name = await author_el.inner_text() if author_el else "Unknown"
                    author_name = author_name.strip().split('\n')[0]

                    print(f"  Author: {author_name} | Type: {client_type} | Company: {is_company}")

                    # ACTION
                    post_url = ""
                    success  = False

                    if is_company:
                        dm_text = generate_dm(post_text, client_type)
                        print(f"  → Sending DM...")
                        success  = await dm_company(page, profile_url, dm_text)
                        message  = dm_text
                        msg_type = "dm"
                        post_url = profile_url
                    else:
                        comment_text        = generate_comment(post_text, client_type)
                        print(f"  → Commenting...")
                        success, post_url   = await comment_on_post(page, card, comment_text)
                        message             = comment_text
                        msg_type            = "comment"

                    if success:
                        actions_done += 1

                        # Save lead
                        supabase_insert("leads_queue", {
                            "platform":                 "linkedin",
                            "potential_client_name":    author_name,
                            "potential_client_profile": profile_url,
                            "post_content":             post_text[:500],
                            "post_url":                 post_url,
                            "assigned_to":              "linkedin_watcher",
                            "status":                   "contacted"
                        })

                        # Save conversation
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

                        # Save log
                        supabase_insert("agent_logs", {
                            "agent_name": "linkedin_watcher",
                            "action":     f"{'DM' if is_company else 'Comment'} sent to {author_name}",
                            "details":    profile_url,
                            "status":     "success"
                        })

                        print(f"  Saved! Status: approval_sent\n")
                        await asyncio.sleep(random.uniform(20, 40))

                except Exception as e:
                    print(f"  Post error: {e}")
                    continue

            await asyncio.sleep(random.uniform(5, 10))

        print(f"\n{'='*50}")
        print(f"Done! Actions: {actions_done}")
        print(f"{'='*50}")
        await browser.close()

# ============================================================
# RUN
# ============================================================
asyncio.run(run_watcher())
