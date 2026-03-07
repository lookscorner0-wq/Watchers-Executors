import os
import json
import time
import random
import re
import requests
from datetime import datetime

SERPAPI_KEY   = os.environ["SERPAPI_KEY"]
OPENAI_KEY    = os.environ["OPENAI_KEY"]
LI_AT         = os.environ["LI_AT"]
LI_JSESSIONID = os.environ["LI_JSESSIONID"]
SHEET_URL     = "https://script.google.com/macros/s/AKfycbyC9kD6TNqrnSoFWeelHfF1kVkZxufVjOPfysicYaGYAQaqmuoFMSOyOUw--4XfPO5t/exec"
SEEN_FILE     = "seen_urls.json"

QUERIES = [
    "AI Automation Expert",
    "Social Media Marketing Manager",
    "Chatbot Developer",
    "Custom Flow Workflow Builder"
]

def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()

def save_seen(seen):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)

def get_session():
    s = requests.Session()
    s.cookies.set("li_at", LI_AT, domain=".linkedin.com")
    s.cookies.set("JSESSIONID", f'"{LI_JSESSIONID}"', domain=".linkedin.com")
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Csrf-Token": LI_JSESSIONID,
        "X-RestLi-Protocol-Version": "2.0.0",
        "X-Li-Lang": "en_US"
    })
    return s

def serp_search(query):
    res  = requests.get("https://serpapi.com/search", params={"q": f"site:linkedin.com/jobs {query}", "api_key": SERPAPI_KEY, "num": 5})
    data = res.json()
    print(f"SerpAPI '{query}': {len(data.get('organic_results', []))} results")
    return data.get("organic_results", [])

def get_job_data(url, s):
    try:
        time.sleep(random.uniform(2, 4))
        match = re.search(r'(\d+)', url.split("/jobs/")[-1])
        if not match:
            return None
        job_id = match.group(1)
        res    = s.get(f"https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}")
        if res.status_code != 200:
            return None
        data   = res.json()
        date   = data.get("listedAt", "")
        if date:
            date = datetime.fromtimestamp(int(date) / 1000).strftime("%Y-%m-%d")
        return {
            "description": data.get("description", {}).get("text", "")[:300],
            "location":    data.get("formattedLocation", ""),
            "post_date":   date,
            "profile_url": data.get("jobPostingUrl", url),
            "website_url": data.get("applyMethod", {}).get("com.linkedin.voyager.jobs.ComplexOnsiteApply", {}).get("easyApplyUrl", "")
        }
    except Exception as e:
        print(f"LinkedIn error: {e}")
        return None

def is_relevant(title, description, query):
    try:
        res = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_KEY}"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{
                    "role": "user",
                    "content": f"Is this job relevant to '{query}'?\nTitle: {title}\nDescription: {description[:200]}\nReply ONLY YES or NO."
                }]
            }
        )
        answer = res.json()["choices"][0]["message"]["content"].strip().upper()
        return "YES" in answer
    except:
        return True

def save_to_sheet(row):
    res = requests.post(SHEET_URL, json=row)
    print(f"Sheet: {res.text}")

seen = load_seen()
s    = get_session()

for query in QUERIES:
    time.sleep(random.uniform(3, 6))
    results = serp_search(query)

    for rank, r in enumerate(results, 1):
        url   = r.get("link", "")
        title = r.get("title", "")
        print(f"\nRank {rank}: {title[:50]}")

        if not url or "linkedin.com/jobs" not in url or url in seen:
            print("Skip!")
            continue

        # LinkedIn se data lo
        data = get_job_data(url, s)
        if not data:
            print("No LinkedIn data — skip!")
            continue

        # AI relevance check
        if not is_relevant(title, data.get("description", ""), query):
            print(f"Not relevant — skip!")
            continue

        lead_score = max(10, 100 - rank)
        save_to_sheet({
            "timestamp":   time.strftime("%Y-%m-%d %H:%M"),
            "title":       title,
            "description": data.get("description", ""),
            "location":    data.get("location", ""),
            "lead_score":  lead_score,
            "job_date":    data.get("post_date", ""),
            "query":       query,
            "profile_url": data.get("profile_url", url),
            "website_url": data.get("website_url", "")
        })
        seen.add(url)
        print(f"Saved! Score: {lead_score}")
        time.sleep(random.uniform(1, 3))

save_seen(seen)
print("Done!")
