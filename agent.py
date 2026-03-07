import os
import json
import time
import random
import re 
import requests
from linkedin_api import Linkedin

def extract_location(title, snippet):
    # Remote check
    if "remote" in title.lower() or "remote" in snippet.lower():
        return "Remote"
    # Location pattern like "New York, NY" or "London, UK"
    match = re.search(r'([A-Z][a-z]+,\s*[A-Z]{2})', title + " " + snippet)
    if match:
        return match.group(1)
    return ""

def clean_title(title):
    return re.sub(r'\s*[\(\[].*?[\)\]]', '', title).strip()

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

def serp_search(query):
    res = requests.get("https://serpapi.com/search", params={
        "q": f"site:linkedin.com/jobs {query}",
        "api_key": SERPAPI_KEY,
        "num": 5
    })
    data = res.json()
    print(f"SerpAPI: {len(data.get('organic_results', []))} results")
    return data.get("organic_results", [])

def get_linkedin_data(url):
    try:
        api = Linkedin(LI_EMAIL, LI_PASSWORD)
        api.client.session.cookies.set("li_at", LI_AT)
        api.client.session.cookies.set("JSESSIONID", LI_JSESSIONID)
        time.sleep(random.uniform(2, 4))

        if "/jobs/" in url:
            job_id = url.split("/jobs/view/")[-1].split("/")[0].split("?")[0]
            data   = api.get_job(job_id)
            print(f"Job data: {data}")
            return {
                "description": data.get("description", {}).get("text", "")[:300],
                "location":    data.get("formattedLocation", ""),
                "post_date":   data.get("listedAt", ""),
                "profile_url": data.get("companyDetails", {}).get(
                    "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", {}
                ).get("companyResolutionResult", {}).get("url", ""),
                "website_url": data.get("applyMethod", {}).get(
                    "com.linkedin.voyager.jobs.OffsiteApply", {}
                ).get("companyApplyUrl", "")
            }

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

    except Exception as e:
        print(f"LinkedIn error: {e}")
        return None

def save_to_sheet(row):
    res = requests.post(SHEET_URL, json=row)
    print(f"Sheet: {res.text}")

seen    = load_seen()
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
    time.sleep(random.uniform(2, 4))
    li_data = get_linkedin_data(url)

    if li_data:
        save_to_sheet({
            "timestamp":   time.strftime("%Y-%m-%d %H:%M"),
            "title":       title,
            "description": li_data.get("description", r.get("snippet", "")),
            "location":    extract_location(title, r.get("snippet", ""))),
            "lead_score":  lead_score,
            "job_date":    str(li_data.get("post_date", "")),
            "query":       "ai automation expert",
            "profile_url": li_data.get("profile_url", url),
            "website_url": li_data.get("website_url", "")
        })
        seen.add(url)
        print(f"Saved! Score: {lead_score}")
    else:
        print("No LinkedIn data — saving SerpAPI data")
        save_to_sheet({
            "timestamp":   time.strftime("%Y-%m-%d %H:%M"),
            "title":       title,
            "description": r.get("snippet", ""),
            "location":    "",
            "lead_score":  lead_score,
            "job_date":    "",
            "query":       "ai automation expert",
            "profile_url": url,
            "website_url": ""
        })
        seen.add(url)
        print(f"Saved from SerpAPI! Score: {lead_score}")

save_seen(seen)
print("Done!")
