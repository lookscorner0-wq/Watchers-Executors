import os
import json
import time
import random
import re
import requests
from datetime import datetime

LI_AT         = os.environ["LI_AT"]
LI_JSESSIONID = os.environ["LI_JSESSIONID"]
SHEET_URL     = "https://script.google.com/macros/s/AKfycbyC9kD6TNqrnSoFWeelHfF1kVkZxufVjOPfysicYaGYAQaqmuoFMSOyOUw--4XfPO5t/exec"
SEEN_FILE     = "seen_urls.json"

QUERIES = [
    "AI Automation Expert",
    "Social Media Marketing",
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

def search_jobs(query, s):
    res = s.get(
        "https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards",
        params={
            "decorationId": "com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-174",
            "count": 5,
            "q": "jobSearch",
            "query": f"(keywords:{query},locationUnion:(geoId:92000000),origin:JOB_SEARCH_PAGE_SEARCH_BUTTON)"
        }
    )
    print(f"Status: {res.status_code} | Length: {len(res.text)}")
    if not res.text.strip() or res.status_code != 200:
        print(f"Response: {res.text[:200]}")
        return []
    try:
        data = res.json()
        elements = data.get("elements", [])
        print(f"Elements: {len(elements)}")
        return elements
    except Exception as e:
        print(f"Error: {e}")
        return []

def get_job_data(job_id, s):
    res  = s.get(f"https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}")
    data = res.json()
    date = data.get("listedAt", "")
    if date:
        date = datetime.fromtimestamp(int(date) / 1000).strftime("%Y-%m-%d")
    return {
        "description": data.get("description", {}).get("text", "")[:300],
        "location":    data.get("formattedLocation", ""),
        "post_date":   date,
        "profile_url": data.get("jobPostingUrl", ""),
        "website_url": data.get("applyMethod", {}).get("com.linkedin.voyager.jobs.ComplexOnsiteApply", {}).get("easyApplyUrl", "")
    }

def save_to_sheet(row):
    res = requests.post(SHEET_URL, json=row)
    print(f"Sheet: {res.text}")

seen = load_seen()
s    = get_session()

for query in QUERIES:
    time.sleep(random.uniform(3, 6))
    jobs = search_jobs(query, s)
    print(f"Jobs found: {len(jobs)}")
    for rank, job in enumerate(jobs, 1):
        try:
            job_id = re.search(r'(\d+)', job.get("entityUrn", "")).group(1)
            url    = f"https://www.linkedin.com/jobs/view/{job_id}/"
            title  = job.get("title", "")
            print(f"\nRank {rank}: {title[:50]}")
            if url in seen:
                print("Skip!")
                continue
            lead_score = max(10, 100 - rank)
            time.sleep(random.uniform(2, 4))
            data = get_job_data(job_id, s)
            save_to_sheet({"timestamp": time.strftime("%Y-%m-%d %H:%M"), "title": title, "description": data.get("description", ""), "location": data.get("location", ""), "lead_score": lead_score, "job_date": data.get("post_date", ""), "query": query, "profile_url": data.get("profile_url", url), "website_url": data.get("website_url", "")})
            seen.add(url)
            print(f"Saved! Score: {lead_score}")
        except Exception as e:
            print(f"Error: {e}")
            continue

save_seen(seen)
print("Done!")
