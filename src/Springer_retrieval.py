# -*- coding: utf-8 -*-
"""
Springer API Retrieval Script - Retrieves raw data from Springer Nature API
"""
import sys
import requests
import os
import json
import time  # For rate limiting if needed
from datetime import datetime
import logging

API_KEY = os.getenv("API_KEY_SPRINGER")

# Setup logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_filename = os.path.join(
    log_dir, datetime.now().strftime("logs_springer_retrieval_%Y-%m-%d_%H-%M-%S.txt")
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Create results directory if it doesn't exist
results_dir = "results"
if not os.path.exists(results_dir):
    os.makedirs(results_dir)

# Define search terms categories
manufacturing = ["manufacturing", "Industry 4.0", "industrial AI", "smart factory", "cyber-physical systems", "production system"]
rai = ["responsible AI", "trustworthy AI", "ethical AI", "explainable AI"]
vbe = ["value-based engineering", "value integration", "value-driven design", "value-sensitive design", "design for values", "ethics by design", "responsible design", "system design", "design methodology", "design process"]

# String to search for
search_string = '(' + ' OR '.join('"' + item + '"' for item in manufacturing) + ') AND (' + ' OR '.join('"' + item + '"' for item in rai) + ') AND (' + ' OR '.join('"' + item + '"' for item in vbe) + ')'
logger.info(f'Search String: {search_string}')

# Define the start date for the search
startdate = "2016"

# Initialize the search_results list
search_results = []

# Maximum number of results per page (basic plan = 25 max)
page_size = 25

# Initialize the page number and counters
page = 1
api_calls = 0

try:
    # Build the base URL for API requests
    base_url = "https://api.springernature.com/meta/v2/json"
    
    # Construct query according to Springer API documentation
    query = f'{search_string} AND dateFrom:"{startdate}"'
    
    # Set up basic parameters (pagination and API key only)
    query_params = {
        "p": str(page_size),
        "s": str(page),
        "api_key": API_KEY,
        "q": query
    }
    
    logger.info(f"Sending request to: {base_url}")
    logger.info(f"Query string: {query}")
    response = requests.get(base_url, params=query_params)
    
    if response.status_code != 200:
        logger.error(f"Error fetching initial results: Status Code {response.status_code}")
        logger.error(f"Response content: {response.text}")
        logger.error(f"Request URL: {response.url}")
        try:
            error_data = response.json()
            logger.error(f"Error details: {json.dumps(error_data, indent=2)}")
        except:
            logger.error("Could not parse error response as JSON")
        raise Exception(f"API Error: Status Code {response.status_code}")
        
    data = response.json()
    number_results_total = int(data['result'][0]['total'])
    logger.info(f'Number of publications in total = {number_results_total}')
    
    # Save the initial response as JSON
    initial_results_file = os.path.join(results_dir, "springer_initial_response.json")
    with open(initial_results_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    # Extend the search results with the initial data
    if "records" in data:
        search_results.extend(data["records"])
        logger.info(f"Publications 1 - {min(page_size, number_results_total)} successfully retrieved")
    
    # Continue fetching more pages if available
    current_record = page_size + 1

    while current_record <= number_results_total:
        # Update starting record position for pagination
        query_params["s"] = str(current_record)
        
        logger.info(f"Fetching records {current_record} - {min(current_record + page_size - 1, number_results_total)}...")
        
        # Add delay to respect rate limits if needed
        # time.sleep(0.6)  # Uncomment if hitting rate limits
        
        response = requests.get(base_url, params=query_params)
        logger.info(f"Full request URL: {response.url}")
        api_calls += 1
        
        if response.status_code == 200:
            data = response.json()
            
            # Check for nextPage in response which indicates successful pagination
            if "nextPage" in data:
                logger.info(f"Next page URL provided by API: {data['nextPage']}")
            
            if "records" in data and data["records"]:
                search_results.extend(data["records"])
                logger.info(f"Publications {current_record} - {min(current_record + page_size - 1, number_results_total)} successfully retrieved")
                logger.info(f"Retrieved {len(data['records'])} records in this batch")
            else:
                logger.warning(f"No records found in page starting at {current_record}")
                break
        else:
            logger.error(f"Error accessing Springer API: Status Code {response.status_code}")
            logger.error(f"Response content: {response.text[:500]}...")
            logger.error(f"Skipping records {current_record} - {min(current_record + page_size - 1, number_results_total)}")
        
        current_record += page_size
    
    # Save all retrieved data as a single JSON file
    if search_results:
        all_results_file = os.path.join(results_dir, "springer_all_results.json")
        with open(all_results_file, 'w', encoding='utf-8') as f:
            json.dump(search_results, f, ensure_ascii=False, indent=2)
        logger.info(f"Retrieved {len(search_results)} records. All results saved to {all_results_file}")
        
        # Analyze content types to understand what we're getting
        content_types = {}
        publication_types = {}
        languages = {}
        
        for record in search_results:
            # Count content types
            content_type = record.get('contentType', 'Unknown')
            if content_type in content_types:
                content_types[content_type] += 1
            else:
                content_types[content_type] = 1
                
            # Count publication types
            pub_type = record.get('publicationType', 'Unknown')
            if pub_type in publication_types:
                publication_types[pub_type] += 1
            else:
                publication_types[pub_type] = 1
                
            # Count languages
            lang = record.get('language', 'Unknown')
            if lang in languages:
                languages[lang] += 1
            else:
                languages[lang] = 1
        
        logger.info("\nContent Type Breakdown:")
        for ctype, count in content_types.items():
            logger.info(f"  {ctype}: {count} records ({count/len(search_results)*100:.1f}%)")
            
        logger.info("\nPublication Type Breakdown:")
        for ptype, count in publication_types.items():
            logger.info(f"  {ptype}: {count} records ({count/len(search_results)*100:.1f}%)")
            
        logger.info("\nLanguage Breakdown:")
        for lang, count in languages.items():
            logger.info(f"  {lang}: {count} records ({count/len(search_results)*100:.1f}%)")
    
except Exception as e:
    logger.error(f'Error during API request: {str(e)}')
    logger.error('Data retrieval process ended')

logger.info("Retrieval script execution completed")
logger.info(f"Retrieved {len(search_results)} records from Springer API")
logger.info(f"Results saved to {results_dir}/springer_all_results.json")
logger.info("Run the Springer_processing.py script to process and filter these results")
# End of script
