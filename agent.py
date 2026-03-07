import os
import json
import time
import random
import re
import requests
from datetime import datetime

LI_AT         = os.environ["LI_AT"]
LI_JSESSIONID = os.environ["LI_JSESSIONID"]
OPENAI_KEY    = os.environ["OPENAI_KEY"]
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
    s.headers.update({
        "accept": "application/vnd.linkedin.normalized+json+2.1",
        "csrf-token": LI_JSESSIONID,
        "referer": "https://www.linkedin.com/jobs/search/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "x-li-lang": "en_US",
        "x-restli-protocol-version": "2.0.0",
        "x-li-deco-include-micro-schema": "true",
        "cookie": f'JSESSIONID="{LI_JSESSIONID}"; li_at={LI_AT}'
    })
    return s

def search_jobs(query, s):
    try:
        kw    = query.replace(" ", "%20")
        url   = f"https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollectionLite-88&count=5&q=jobSearch&query=(origin:JOBS_HOME_SEARCH_BUTTON,keywords:{kw},spellCorrectionEnabled:true)&servedEventEnabled=false&start=0"
        res   = s.get(url)
        print(f"Search '{query}': {res.status_code}")
        data  = res.json()
        cards = data.get("data", {}).get("metadata", {}).get("jobCardPrefetchQueries", [])
        ids   = []
        for card in cards:
            for key in card.get("prefetchJobPostingCard", {}).keys():
                match = re.search(r'\((\d+),', key)
                if match:
                    ids.append(match.group(1))
        print(f"Job IDs found: {ids}")
        return ids
    except Exception as e:
        print(f"Search error: {e}")
        return []

def get_job_data(job_id, s):
    try:
        time.sleep(random.uniform(2, 4))
        res  = s.get(f"https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}")
        data = res.json()
        date = data.get("listedAt", "")
        if date:
            date = datetime.fromtimestamp(int(date) / 1000).strftime("%Y-%m-%d")
        return {
            "title":       data.get("title", ""),
            "description": data.get("description", {}).get("text", "")[:300],
            "location":    data.get("formattedLocation", ""),
            "post_date":   date,
            "profile_url": data.get("jobPostingUrl", f"https://www.linkedin.com/jobs/view/{job_id}/"),
            "website_url": data.get("applyMethod", {}).get("com.linkedin.voyager.jobs.ComplexOnsiteApply", {}).get("easyApplyUrl", "")
        }
    except Exception as e:
        print(f"Job data error: {e}")
        return None

def is_relevant(title, description, query):
    try:
        res = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_KEY}"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": f"Is this job relevant to '{query}'?\nTitle: {title}\nDescription: {description[:200]}\nReply ONLY YES or NO."}]
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
    job_ids = search_jobs(query, s)

    for rank, job_id in enumerate(job_ids, 1):
        url = f"https://www.linkedin.com/jobs/view/{job_id}/"
        if url in seen:
            print(f"Skip {job_id}!")
            continue

        data = get_job_data(job_id, s)
        if not data:
            continue

        if not is_relevant(data.get("title", ""), data.get("description", ""), query):
            print(f"Not relevant — skip!")
            continue

        lead_score = max(10, 100 - rank)
        save_to_sheet({
            "timestamp":   time.strftime("%Y-%m-%d %H:%M"),
            "title":       data.get("title", ""),
            "description": data.get("description", ""),
            "location":    data.get("location", ""),
            "lead_score":  lead_score,
            "job_date":    data.get("post_date", ""),
            "query":       query,
            "profile_url": data.get("profile_url", url),
            "website_url": data.get("website_url", "")
        })
        seen.add(url)
        print(f"Saved {data.get('title', '')}! Score: {lead_score}")
        time.sleep(random.uniform(1, 3))

save_seen(seen)
print("Done!")
