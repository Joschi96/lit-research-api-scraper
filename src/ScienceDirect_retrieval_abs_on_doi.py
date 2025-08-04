import requests
import json
import itertools
import time
from api_keys import api_key_sciencedirect

# Fetches results from ScienceDirect Metadata API based on specified DOIs
# This script retrieves detailed metadata including abstracts for articles
# Then merges this metadata with previously fetched results
# and saves the enriched results to a new JSON file


# Config
API_KEY = api_key_sciencedirect
API_URL = "https://api.elsevier.com/content/metadata/article"

INPUT_FILE = "combined_results.json"
OUTPUT_FILE = "merged_with_metadata.json"
LOG_FILE = "doi_metadata_log.txt"

SLEEP_BASE = 0.8  # Sekunden warten zwischen Anfragen
MAX_RETRIES = 3   # Bei Netzwerkfehlern

# Wichtige Felder (Abstract = description)
FIELDS = "identifier,title,description,author,publicationName,coverDate,openAccess,link"

HEADERS = {
    "Accept": "application/json",
    "X-ELS-APIKey": API_KEY
}

# ---- FUNCTIONS ----
def get_metadata_by_doi(doi):
    url = f"{API_URL}?query=doi({doi})&field={FIELDS}"
    retries = 0
    while retries < MAX_RETRIES:
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:  # Rate-Limit
            retry_after = int(resp.headers.get("Retry-After", 30))
            print(f"Rate limit hit. Waiting {retry_after}s...")
            time.sleep(retry_after)
        else:
            print(f"Error {resp.status_code} for DOI {doi}: {resp.text}")
            break
        retries += 1
    return None

# ---- MAIN LOGIC ----
# 1. Lade ursprüngliche Ergebnisse
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    original_data = json.load(f)

# 2. DOIs extrahieren
dois = list({item.get("doi") for item in original_data if item.get("doi")})
print(f"Found {len(dois)} unique DOIs.")

merged_results = []
log_entries = []

for i, doi in enumerate(dois, start=1):
    print(f"\n[{i}/{len(dois)}] Fetching metadata for DOI: {doi}")
    metadata = get_metadata_by_doi(doi)
    
    if metadata and "search-results" in metadata:
        entries = metadata["search-results"].get("entry", [])
        if entries:
            enriched_entry = entries[0]  # Normalerweise 1 Treffer pro DOI
            
            # Finde Originaldatensatz
            original_entry = next((x for x in original_data if x.get("doi") == doi), None)
            
            # Merge beide Dicts
            combined = {
                "doi": doi,
                "original": original_entry,
                "metadata": enriched_entry
            }
            merged_results.append(combined)
            log_entries.append(f"{doi}: SUCCESS")
        else:
            print(f"No metadata found for DOI: {doi}")
            log_entries.append(f"{doi}: NO_METADATA")
    else:
        print(f"Failed to retrieve metadata for DOI: {doi}")
        log_entries.append(f"{doi}: FAILED")
    
    time.sleep(SLEEP_BASE)

# 3. Speichern
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(merged_results, f, indent=2)

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(log_entries))

print(f"\n✅ Done! {len(merged_results)} merged records saved to {OUTPUT_FILE}")
print(f"Log written to {LOG_FILE}")