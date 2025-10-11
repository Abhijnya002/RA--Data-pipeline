import pandas as pd
import requests
import pdfplumber
import re
from datetime import datetime
import io
import time
from pathlib import Path

# Configuration
API_KEY = "rWRXjXvflW9STOTgc8JKH9kFkkyY7j5qR3GcWba6"
EXCEL_FILE = "301-1000(IDs+attachment_urls).xlsx"
OUTPUT_FILE = "301-1000(date+withdrawn col).xlsx"

# API headers
headers = {
    "X-Api-Key": API_KEY
}

def get_attachments(document_id):
    """Fetch attachments for a document from the API"""
    url = f"https://api.regulations.gov/v4/documents/{document_id}/attachments"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get('data', [])
    except Exception as e:
        print(f"Error fetching attachments for {document_id}: {e}")
        return []

def find_re_attachment(attachments):
    """Find the 'Re_' attachment (USTR response letter)"""
    for att in attachments:
        title = att.get('attributes', {}).get('title', '')
        if title.startswith('Re_') or 'Re_' in title:
            file_formats = att.get('attributes', {}).get('fileFormats', [])
            if file_formats:
                return file_formats[0].get('fileUrl')
    return None

def download_pdf(url):
    """Download PDF from URL"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return io.BytesIO(response.content)
    except Exception as e:
        print(f"Error downloading PDF from {url}: {e}")
        return None

def extract_date_from_pdf(pdf_bytes):
    """Extract the date from the beginning of the USTR response letter"""
    if not pdf_bytes:
        return None
    
    try:
        with pdfplumber.open(pdf_bytes) as pdf:
            # Get first page text
            first_page = pdf.pages[0]
            text = first_page.extract_text()
            
            if not text:
                return None
            
            # Look for date patterns in the first ~1000 characters
            # Common formats: "September 26, 2019" or "February 4, 2019"
            text_start = text[:1000]
            
            # Pattern for Month Day, Year format
            date_pattern = r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})'
            
            match = re.search(date_pattern, text_start)
            if match:
                date_str = match.group(0)
                # Normalize the date string (remove extra comma if needed)
                date_str = date_str.replace(',', '')
                try:
                    # Parse the date
                    parsed_date = datetime.strptime(date_str.replace(',', ''), '%B %d %Y')
                    return parsed_date.strftime('%Y-%m-%d')  # Return as ISO format
                except:
                    return date_str  # Return original if parsing fails
            
            return None
    except Exception as e:
        print(f"Error extracting date from PDF: {e}")
        return None

def process_documents(df):
    """Process all documents and extract USTR response dates"""
    results = []
    
    for idx, row in df.iterrows():
        doc_id = row['documentId']
        print(f"\nProcessing {idx+1}/{len(df)}: {doc_id}")
        
        # Get attachments from API
        attachments = get_attachments(doc_id)
        
        if not attachments:
            print(f"  No attachments found")
            results.append(None)
            continue
        
        # Find the Re_ attachment
        re_url = find_re_attachment(attachments)
        
        if not re_url:
            print(f"  No 'Re_' attachment found")
            results.append(None)
            continue
        
        print(f"  Found Re_ attachment: {re_url}")
        
        # Download and extract date
        pdf_bytes = download_pdf(re_url)
        date = extract_date_from_pdf(pdf_bytes)
        
        if date:
            print(f"  Extracted date: {date}")
        else:
            print(f"  Could not extract date")
        
        results.append(date)
        
        # Be nice to the API - small delay
        time.sleep(0.5)
    
    return results

def main():
    print("Loading Excel file...")
    df = pd.read_excel(EXCEL_FILE)
    
    print(f"Found {len(df)} documents to process")
    print(f"Current columns: {list(df.columns)}")
    
    # Remove existing date columns (except Posted Date and Last Modified Date)
    date_columns_to_remove = [col for col in df.columns if 'date' in col.lower() 
                              and col not in ['Posted Date', 'Last Modified Date']]
    
    if date_columns_to_remove:
        print(f"\nRemoving existing date columns: {date_columns_to_remove}")
        df = df.drop(columns=date_columns_to_remove)
    
    # Process documents and extract USTR response dates
    print("\nExtracting USTR response dates...")
    ustr_dates = process_documents(df)
    
    # Add the new column
    df['USTR Response Date'] = ustr_dates
    
    # Report statistics
    dates_found = sum(1 for d in ustr_dates if d is not None)
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total documents: {len(df)}")
    print(f"USTR response dates found: {dates_found}")
    print(f"Missing dates: {len(df) - dates_found}")
    
    # Save to new Excel file
    print(f"\nSaving results to {OUTPUT_FILE}...")
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Done! Results saved to {OUTPUT_FILE}")
    
    # Show sample of results
    print("\nSample of extracted dates:")
    sample = df[['documentId', 'USTR Response Date']].head(10)
    print(sample.to_string())

if __name__ == "__main__":
    main()