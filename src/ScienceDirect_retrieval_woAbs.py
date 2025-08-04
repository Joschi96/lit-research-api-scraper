import requests
import json
import itertools
import time
import os

# Fetches results from ScienceDirect API based on specified search terms
# This script combines results from multiple queries to avoid API limits and deduplicates by DOI
# results dont contain abstracts, only metadata
# Therefore use ....py script to fetch abstracts TODO: add script later

# Config
API_KEY = os.getenv("API_KEY_SCIENDEDIRECT")
API_URL = "https://api.elsevier.com/content/search/sciencedirect"

SHOW = 25  # Treffer pro Seite: 10, 25, 50, 100
MAX_RESULTS_PER_QUERY = 500  # Safety-Limit pro Query, um API-Quota zu schützen
SLEEP_BASE = 0.7  # Sekunden warten, um Rate-Limits zu vermeiden (2 Requests pro Sekunde erlaubt)
OUTPUT_FILE = "combined_results.json"

HEADERS = {
    "Accept": "application/json",
    "X-ELS-APIKey": API_KEY
}

# Create results directory if it doesn't exist
results_dir = "results"
if not os.path.exists(results_dir):
    os.makedirs(results_dir)

# ---- Search String GROUPS ----
group_A = [
    "responsible AI",
    "trustworthy AI",
    "ethical AI",
    "explainable AI"
]

group_B = [
    "cyber-physical systems",
    "manufacturing",
    "Industry 4.0",
    "smart factory",
    "production system",
    "Industrial AI"
]

group_C = [
    "value-based engineering",
    "value integration",
    "value-driven design",
    "value-sensitive design",
    "design for values",
    "ethics by design",
    "responsible design",
    "system design",
    "design methodology",
    "design process"
]

# ---- FUNCTIONS ----
def make_request(query, offset=0, show=SHOW):
    payload = {"qs": query, "offset": offset, "show": show}
    try:
        response = requests.put(API_URL, headers=HEADERS, data=json.dumps(payload))
    except requests.RequestException as e:
        print(f"Network error: {e}")
        return None

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:  # Rate limit
        retry_after = int(response.headers.get("Retry-After", 30))
        print(f"Rate limit reached. Waiting {retry_after} seconds...")
        time.sleep(retry_after)
        return make_request(query, offset, show)
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

# ---- MAIN LOGIC ----
all_results = []
seen_dois = set()
total_queries = len(group_A) * len(group_B) * len(group_C)
processed_queries = 0
empty_streak = 0

for a, b, c in itertools.product(group_A, group_B, group_C):
    processed_queries += 1
    query = f"\"{a}\" AND \"{b}\" AND \"{c}\""
    print(f"\n[{processed_queries}/{total_queries}] Query: {query}")

    # --- 1. Vorprüfung: Nur 1 Ergebnis holen ---
    check = make_request(query, offset=0, show=1)
    if not check or "results" not in check:
        print("API error during check.")
        continue

    total_results = check.get("resultsFound") or check.get("totalResults") or len(check["results"])
    print(f"Total results: {total_results}")

    if total_results == 0:
        empty_streak += 1
        # Adaptive Sleep bei mehreren leeren Treffern
        time.sleep(SLEEP_BASE * empty_streak)
        print(f"No results. Empty streak: {empty_streak}. Waiting {SLEEP_BASE * empty_streak:.2f}s...")
        continue
    else:
        empty_streak = 0  # Reset bei Treffer

    # --- 2. Erste Seite laden (mit SHOW) ---
    data = make_request(query, offset=0, show=SHOW)
    if not data or "results" not in data:
        continue

    for item in data["results"]:
        doi = item.get("doi")
        if doi and doi not in seen_dois:
            seen_dois.add(doi)
            all_results.append(item)

    # --- 3. Pagination ---
    offset = SHOW
    while offset < total_results and offset < MAX_RESULTS_PER_QUERY:
        page = make_request(query, offset=offset)
        if not page or "results" not in page:
            break
        for item in page["results"]:
            doi = item.get("doi")
            if doi and doi not in seen_dois:
                seen_dois.add(doi)
                all_results.append(item)
        offset += SHOW
        print(f"Fetched {offset}/{min(total_results, MAX_RESULTS_PER_QUERY)} for this query...")
        time.sleep(SLEEP_BASE)  # Standardpause pro Page

    # Kleine Pause zwischen Queries
    time.sleep(SLEEP_BASE)

print(f"\n✅ Finished. Total unique results: {len(all_results)}")

# ---- SAVE RESULTS ----
all_results_file = os.path.join(results_dir, OUTPUT_FILE)
with open(all_results_file, "w", encoding="utf-8") as f:
    json.dump(all_results, f, indent=2)

print(f"Results saved to {all_results_file}")