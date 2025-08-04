# -*- coding: utf-8 -*-
"""
Springer API Processing Script - Processes and filters data retrieved by Springer_retrieval.py
"""
import sys
import os
import pandas as pd
import numpy as np
import json
import csv
from datetime import datetime
from pandas import json_normalize  # Updated import statement

class Logger:
    def __init__(self):
        self.terminal = sys.stdout
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        filename = os.path.join(log_dir, datetime.now().strftime("logs_springer_processing_%Y-%m-%d_%H-%M-%S.txt"))
        self.log = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()
        
    def close(self):
        self.log.close()

# Setup logging
old_stdout = sys.stdout  # Save the current stdout
logger = Logger()
sys.stdout = logger

# Paths
results_dir = "results"
input_file = os.path.join(results_dir, "springer_all_results.json")
output_file = os.path.join(results_dir, "results_springer_v2.csv")

# Check if the input file exists
if not os.path.exists(input_file):
    print(f"Error: Input file '{input_file}' not found.")
    print("Please run Springer_retrieval.py first to retrieve data.")
    sys.exit(1)

print(f"Processing Springer results from {input_file}")

# Load the retrieved data
with open(input_file, 'r', encoding='utf-8') as f:
    search_results = json.load(f)

# Process the retrieved publications
if search_results:
    print(f"Processing {len(search_results)} publications...")
    
    # Analyze the content types before filtering
    content_types = {}
    for record in search_results:
        content_type = record.get('contentType', 'Unknown')
        if content_type in content_types:
            content_types[content_type] += 1
        else:
            content_types[content_type] = 1
    
    print("\nContent Types before filtering:")
    for ctype, count in sorted(content_types.items(), key=lambda x: x[1], reverse=True):
        print(f"  {ctype}: {count} records ({count/len(search_results)*100:.1f}%)")
    
    # Filter records based on manual search criteria from the URL
    # URL: https://link.springer.com/search?new-search=true&query=...&content-type=Article&content-type=Research&content-type=Conference+Paper&content-type=Review&date=custom&dateFrom=2016&dateTo=&sortBy=relevance
    
    import re
    valid_ct = ['Article']
    review_pattern = re.compile(r'^review', re.IGNORECASE)

    def is_conference(ct):
        return ct.endswith('ConferencePaper')

    def is_review(genres):
        if isinstance(genres, str):
            genres = [genres]
        return any(review_pattern.match(str(g)) for g in genres)

    filtered_results = []
    for rec in search_results:
        ct = rec.get('contentType', '')
        genres = rec.get('genre', [])
        lang = rec.get('language', '')
        pub_date = rec.get('publicationDate', '')
        year = pub_date[:4] if pub_date else ''
        if (ct in valid_ct or is_conference(ct) or is_review(genres)) \
           and lang == 'en' \
           and year >= '2016':
            filtered_results.append(rec)
    print(f"\nFiltered from {len(search_results)} to {len(filtered_results)} records")
    print(f"Filtering removed {len(search_results) - len(filtered_results)} records")
    
    # Analyze the content types after filtering
    filtered_content_types = {}
    for record in filtered_results:
        content_type = record.get('contentType', 'Unknown')
        if content_type in filtered_content_types:
            filtered_content_types[content_type] += 1
        else:
            filtered_content_types[content_type] = 1
    
    print("\nContent Types after filtering:")
    for ctype, count in sorted(filtered_content_types.items(), key=lambda x: x[1], reverse=True):
        print(f"  {ctype}: {count} records ({count/len(filtered_results)*100:.1f}%)")
    
    # Convert to DataFrame using json_normalize
    df = json_normalize(filtered_results)
    
    # Show sample of DataFrame structure
    print("\nDataFrame columns:")
    for col in df.columns:
        print(f"  {col}")
    
    # Select and rename columns of interest
    columns_mapping = {
        'title': 'title',
        'creators': 'author',
        'abstract': 'abstract',
        'keyword': 'keywords',
        'publisher': 'publisher',
        'publicationDate': 'publicationDate',
        'language': 'language',
        'publicationType': 'publicationType',
        'openaccess': 'openaccess',
        'startingPage': 'starting_page',
        'endingPage': 'ending_page',
        'doi': 'doi',
        'url': 'urls'
    }
    
    # Create a new DataFrame with only the columns we need
    results = pd.DataFrame()
    
    # Process each column, handling missing values and nested structures
    for src_col, target_col in columns_mapping.items():
        if src_col in df.columns:
            results[target_col] = df[src_col]
        else:
            # Fill with appropriate default values based on column type
            if src_col in ['abstract', 'title', 'publisher', 'doi']:
                results[target_col] = "Not available"
            elif src_col in ['startingPage', 'endingPage']:
                results[target_col] = 0
            elif src_col == 'creators' or src_col == 'keyword':
                results[target_col] = None
            else:
                results[target_col] = None
    
    # Process authors - handle nested list structures based on Springer API response format
    def process_authors(authors_data):
        # Handle None, NaN, empty list, or other invalid data
        if authors_data is None or (isinstance(authors_data, (list, tuple)) and len(authors_data) == 0):
            return "No author information"
        
        # Special handling for pandas NA values or numpy arrays
        try:
            if pd.isna(authors_data).all():
                return "No author information"
        except (TypeError, ValueError, AttributeError):
            # If it's not a pandas or numpy object that supports isna().all()
            pass
        
        authors = []
        # Make sure we're working with a list
        author_list = authors_data if isinstance(authors_data, list) else [authors_data]
        
        for author in author_list:
            if isinstance(author, dict):
                # Extract creator information (always present)
                if 'creator' in author:
                    authors.append(author['creator'])
                    
        return " / ".join(authors) if authors else "No author information"
    
    # Apply author processing
    if 'author' in results.columns:
        # Convert to Python native objects before processing to avoid pandas/numpy issues
        results['author'] = results['author'].apply(lambda x: process_authors(x))
    
    # Process keywords - handle nested list structures
    def process_keywords(keywords_data):
        # Handle None, NaN, or empty values
        if keywords_data is None or (isinstance(keywords_data, (list, tuple)) and len(keywords_data) == 0):
            return "No keywords"
        
        # Special handling for pandas NA values or numpy arrays
        try:
            if pd.isna(keywords_data).all():
                return "No keywords"
        except (TypeError, ValueError, AttributeError):
            # If it's not a pandas or numpy object that supports isna().all()
            pass
        
        if isinstance(keywords_data, list):
            # Filter out None or NaN values before joining
            valid_keywords = [str(keyword) for keyword in keywords_data if keyword is not None and not (hasattr(keyword, 'is_na') and keyword.is_na())]
            return " / ".join(valid_keywords) if valid_keywords else "No keywords"
        else:
            # Handle single values
            try:
                return str(keywords_data) if not pd.isna(keywords_data) else "No keywords"
            except (TypeError, ValueError):
                return str(keywords_data) if keywords_data is not None else "No keywords"
    
    # Apply keyword processing
    if 'keywords' in results.columns:
        # Convert to Python native objects before processing
        results['keywords'] = results['keywords'].apply(lambda x: process_keywords(x))
    
    # Process URLs to extract PDF link based on Springer API response format
    def extract_pdf_link(urls_data):
        # Handle None, NaN, or empty values
        if urls_data is None or (isinstance(urls_data, (list, tuple)) and len(urls_data) == 0):
            return "No PDF link"
        
        # Special handling for pandas NA values or numpy arrays
        try:
            if pd.isna(urls_data).all():
                return "No PDF link"
        except (TypeError, ValueError, AttributeError):
            # If it's not a pandas or numpy object that supports isna().all()
            pass
        
        if isinstance(urls_data, list):
            # First look for PDF format specifically
            for url_item in urls_data:
                if isinstance(url_item, dict) and 'format' in url_item and url_item['format'] == 'pdf':
                    return url_item.get('value', "No PDF link")
            
            # If no PDF found, try to get DOI link
            for url_item in urls_data:
                if isinstance(url_item, dict) and 'value' in url_item and 'doi.org' in url_item['value']:
                    return url_item.get('value', "No PDF link")
            
            # Fallback to any URL
            if len(urls_data) > 0 and isinstance(urls_data[0], dict):
                return urls_data[0].get('value', "No PDF link")
        
        # If it's a string (already a URL), return it
        elif isinstance(urls_data, str):
            return urls_data
        
        return "No PDF link"
    
    # Apply PDF link extraction
    if 'urls' in results.columns:
        results['pdf_link'] = results['urls'].apply(extract_pdf_link)
        results.drop('urls', axis=1, inplace=True)
    else:
        results['pdf_link'] = "No PDF link"
    
    # Process page numbers
    if 'starting_page' in results.columns and 'ending_page' in results.columns:
        # Convert to numeric, coerce errors to NaN
        results['starting_page'] = pd.to_numeric(results['starting_page'], errors='coerce').fillna(0).astype(int)
        results['ending_page'] = pd.to_numeric(results['ending_page'], errors='coerce').fillna(0).astype(int)
        
        # Calculate number of pages
        results['number_of_pages'] = results.apply(
            lambda row: max(0, row['ending_page'] - row['starting_page'] + 1) 
            if row['ending_page'] > 0 else 0, 
            axis=1
        )
    
    # Reorder columns for the final output (no cited_by column)
    final_columns = [
        'doi', 'title', 'author', 'abstract', 'keywords', 
        'publisher', 'publicationDate', 'publicationType', 'pdf_link'
    ]
    
    # Create the final DataFrame with selected columns
    results_springer = results[final_columns].copy()
    
    # Clean text fields to prevent CSV issues
    text_columns = ['title', 'abstract', 'author', 'keywords']
    for col in text_columns:
        if col in results_springer.columns:
            # Replace problematic characters and newlines
            results_springer[col] = results_springer[col].astype(str).apply(
                lambda x: x.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
            )
    
    # Write the dataframe to a CSV file with proper encoding and quoting
    # Use CSV quoting to handle text with commas, quotes, etc.
    results_springer.to_csv(
        output_file, 
        index=False, 
        encoding='utf-8-sig',  # Use UTF-8 with BOM for Excel compatibility
        quoting=csv.QUOTE_NONNUMERIC,  # Quote all non-numeric fields
        escapechar='\\',  # Use backslash as escape character
        doublequote=True  # Double quotes within fields
    )
    
    print(f"\nProcessed and filtered {len(results_springer)} publications")
    print(f"Results saved to {output_file}")
else:
    print("No results to process")

# Restore original stdout and close logger
sys.stdout = old_stdout
logger.close()
print("Processing script execution completed")
