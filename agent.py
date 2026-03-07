import os
import json
import time
import random
import requests
from datetime import datetime
from linkedin_api import Linkedin

SERPAPI_KEY  = os.environ["SERPAPI_KEY"]
OPENAI_KEY   = os.environ["OPENAI_KEY"]
LI_AT        = os.environ["LI_AT"]
LI_EMAIL     = os.environ["LI_EMAIL"]
LI_PASSWORD  = os.environ["LI_PASSWORD"]
SHEET_URL    = "https://script.google.com/macros/s/AKfycbzSmrlXx32EM3bcEo6HhW-pdKpPgQyAwORm10hhFnE1mwVMqH37dZphG_HaWrL-55kM/exec"
SEEN_FILE    = "seen_urls.json"

QUERIES = [
    "AI Automation Expert",
    "Social Media Marketing",
    "Chatbot Developer",
    "Custom Flow Workflow Builder"
]

SYSTEM_PROMPT = """You are an experienced business researcher with expertise in lead generation and market analysis. You have a keen eye for identifying high-quality prospects and understand various industries. You're methodical in your research approach, always verifying information from multiple sources and ensuring data accuracy. Your research helps sales teams focus their efforts on the most promising opportunities."""

def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()

def save_seen(seen):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)

def get_queries():
    res = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_KEY}"},
        json={
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Based on these topics: {QUERIES}\nGenerate 8 unique Google search queries to find recent LinkedIn posts and jobs. Return ONLY a JSON array of strings."}
            ]
        }
    )
    data = res.json()
    print(f"OpenAI response: {data}")  # debug
    if "choices" not in data:
        print(f"OpenAI error: {data.get('error', 'unknown')}")
        return QUERIES
    text = data["choices"][0]["message"]["content"].strip()
    text = text.replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(text)
    except:
        return QUERIES

def serp_search(query):
    res = requests.get("https://serpapi.com/search", params={
        "q": f"site:linkedin.com/posts OR site:linkedin.com/jobs {query}",
        "api_key": SERPAPI_KEY,
        "num": 10,
        "tbs": "qdr:d"
    })
    return res.json().get("organic_results", [])

def is_relevant(title, snippet, query):
    res = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_KEY}"},
        json={
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Is this LinkedIn result relevant to '{query}'?\nTitle: {title}\nSnippet: {snippet}\nReply ONLY with YES or NO."}
            ]
        }
    )
    answer = res.json()["choices"][0]["message"]["content"].strip().upper()
    return "YES" in answer

def get_linkedin_data(url):
    try:
        api = Linkedin(LI_EMAIL, LI_PASSWORD, cookies={"li_at": LI_AT})
        time.sleep(random.uniform(3, 6))

        if "/posts/" in url:
            post_id = url.split("/posts/")[-1].split("/")[0].split("?")[0]
            data    = api.get_post_comments(post_id, comment_count=0)
            return {
                "description": data.get("commentary", {}).get("text", {}).get("text", ""),
                "location":    data.get("actor", {}).get("subDescription", {}).get("text", ""),
                "post_date":   data.get("createdAt", ""),
                "profile_url": url,
                "website_url": ""
            }

        if "/jobs/" in url:
            job_id = url.split("/jobs/")[-1].split("/")[0].split("?")[0]
            data   = api.get_job(job_id)
            return {
                "description": data.get("description", {}).get("text", "")[:300],
                "location":    data.get("formattedLocation", ""),
                "post_date":   data.get("listedAt", ""),
                "profile_url": data.get("companyDetails", {}).get("com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", {}).get("companyResolutionResult", {}).get("url", ""),
                "website_url": data.get("applyMethod", {}).get("com.linkedin.voyager.jobs.OffsiteApply", {}).get("companyApplyUrl", "")
            }
    except Exception as e:
        print(f"LinkedIn API error: {e}")
        return None

def save_to_sheet(row):
    requests.post(SHEET_URL, json=row)

seen = load_seen()
queries = get_queries()
print(f"Queries: {len(queries)}")

saved = 0
for query in queries[:2]:
    time.sleep(random.uniform(5, 10))
    results = serp_search(query)
    print(f"Search: {query} -> {len(results)} results")

    for rank, r in enumerate(results, 1):
        url     = r.get("link", "")
        title   = r.get("title", "")
        snippet = r.get("snippet", "")

        if not url or "linkedin.com" not in url:
            continue
        if url in seen:
            print(f"Skip duplicate: {url[:50]}")
            continue
        if "/in/" in url:
            continue

        time.sleep(random.uniform(2, 4))

        if not is_relevant(title, snippet, query):
            print(f"Not relevant rank {rank}: {title[:40]}")
            continue

        lead_score = max(10, 100 - rank)

        time.sleep(random.uniform(3, 7))
        li_data = get_linkedin_data(url)

        if li_data:
            post_date = li_data.get("post_date", "")
            if isinstance(post_date, int):
                post_date = datetime.fromtimestamp(post_date / 1000).strftime("%Y-%m-%d")

            save_to_sheet({
                "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M"),
                "title":       title,
                "description": li_data.get("description", snippet),
                "location":    li_data.get("location", ""),
                "lead_score":  lead_score,
                "job_date":    post_date,
                "query":       query,
                "profile_url": li_data.get("profile_url", url),
                "website_url": li_data.get("website_url", "")
            })
            seen.add(url)
            saved += 1
            print(f"Saved {saved}: {title[:50]} | Score: {lead_score}")
            time.sleep(random.uniform(2, 5))

save_seen(seen)
print(f"Done! {saved} leads saved.")
