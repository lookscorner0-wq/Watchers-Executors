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
LI_EMAIL      = os.environ["LI_EMAIL"]
LI_PASSWORD   = os.environ["LI_PASSWORD"]
LI_JSESSIONID = os.environ["LI_JSESSIONID"]
SHEET_URL     = "https://script.google.com/macros/s/AKfycbyC9kD6TNqrnSoFWeelHfF1kVkZxufVjOPfysicYaGYAQaqmuoFMSOyOUw--4XfPO5t/exec"
SEEN_FILE     = "seen_urls.json"

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
    print(f"SerpAPI: {len(data.get('organic_results', []))} results")
    return data.get("organic_results", [])

def get_linkedin_data(url, s):
    try:
        time.sleep(random.uniform(2, 4))
        if "/jobs/" in url:
            match = re.search(r'(\d+)', url.split("/jobs/")[-1])
            if not match:
                return None
            job_id = match.group(1)
            res    = s.get(f"https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}")
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
        if "/posts/" in url:
            post_id = url.split("/posts/")[-1].split("/")[0].split("?")[0]
            res     = s.get(f"https://www.linkedin.com/voyager/api/feed/updates/{post_id}")
            data    = res.json()
            date    = data.get("createdAt", "")
            if date:
                date = datetime.fromtimestamp(int(date) / 1000).strftime("%Y-%m-%d")
            return {
                "description": data.get("commentary", {}).get("text", {}).get("text", ""),
                "location":    data.get("actor", {}).get("subDescription", {}).get("text", ""),
                "post_date":   date,
                "profile_url": url,
                "website_url": ""
            }
    except Exception as e:
        print(f"LinkedIn error: {e}")
        return None

def save_to_sheet(row):
    res = requests.post(SHEET_URL, json=row)
    print(f"Sheet: {res.text}")

seen    = load_seen()
s       = get_session()
results = serp_search("ai automation expert")

for rank, r in enumerate(results, 1):
    url   = r.get("link", "")
    title = r.get("title", "")
    print(f"\nRank {rank}: {title[:50]}")
    print(f"URL: {url}")
    if not url or "linkedin.com" not in url or url in seen or "/in/" in url:
        print("Skip!")
        continue
    lead_score = max(10, 100 - rank)
    li_data    = get_linkedin_data(url, s)
    if li_data:
        save_to_sheet({"timestamp": time.strftime("%Y-%m-%d %H:%M"), "title": title, "description": li_data.get("description", r.get("snippet", "")), "location": li_data.get("location", ""), "lead_score": lead_score, "job_date": li_data.get("post_date", ""), "query": "ai automation expert", "profile_url": li_data.get("profile_url", url), "website_url": li_data.get("website_url", "")})
        seen.add(url)
        print(f"Saved! Score: {lead_score}")
    else:
        save_to_sheet({"timestamp": time.strftime("%Y-%m-%d %H:%M"), "title": title, "description": r.get("snippet", ""), "location": "", "lead_score": lead_score, "job_date": "", "query": "ai automation expert", "profile_url": url, "website_url": ""})
        seen.add(url)
        print(f"Saved SerpAPI! Score: {lead_score}")

save_seen(seen)
print("Done!")
