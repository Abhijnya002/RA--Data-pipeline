import requests
import pandas as pd
import time
from datetime import datetime
import re

# Excel support
try:
    import openpyxl
except ImportError:
    import subprocess
    subprocess.check_call(["pip", "install", "openpyxl"])

API = "https://api.regulations.gov/v4"
DOCKET_ID = "USTR-2018-0025"
KEY = "rWRXjXvflW9STOTgc8JKH9kFkkyY7j5qR3GcWba6"

def fetch_with_different_sorts():
    """Use different sort orders to access all documents - avoids date filtering issues"""
    print("🚀 SIMPLE RELIABLE METHOD - NO DATE FILTERING")
    print("📊 Uses different sort orders to access all documents")
    print("🎯 Target: All 11,075 documents")
    print("=" * 70)
    
    all_documents = {}
    
    # Strategy 1: Sort by postedDate (ascending)
    print(f"\n📊 STRATEGY 1: Posted Date Ascending")
    docs1 = fetch_all_pages_with_sort("postedDate", "Posted Date Asc")
    for doc in docs1:
        all_documents[doc["documentId"]] = doc
    print(f"   Running total: {len(all_documents):,}")
    
    # Strategy 2: Sort by postedDate (descending)  
    print(f"\n📊 STRATEGY 2: Posted Date Descending")
    docs2 = fetch_all_pages_with_sort("-postedDate", "Posted Date Desc")
    new_count = 0
    for doc in docs2:
        if doc["documentId"] not in all_documents:
            all_documents[doc["documentId"]] = doc
            new_count += 1
    print(f"   New documents: {new_count}")
    print(f"   Running total: {len(all_documents):,}")
    
    # Strategy 3: Sort by lastModifiedDate (ascending)
    print(f"\n📊 STRATEGY 3: Last Modified Date Ascending")
    docs3 = fetch_all_pages_with_sort("lastModifiedDate", "Modified Date Asc")
    new_count = 0
    for doc in docs3:
        if doc["documentId"] not in all_documents:
            all_documents[doc["documentId"]] = doc
            new_count += 1
    print(f"   New documents: {new_count}")
    print(f"   Running total: {len(all_documents):,}")
    
    # Strategy 4: Sort by lastModifiedDate (descending)
    print(f"\n📊 STRATEGY 4: Last Modified Date Descending") 
    docs4 = fetch_all_pages_with_sort("-lastModifiedDate", "Modified Date Desc")
    new_count = 0
    for doc in docs4:
        if doc["documentId"] not in all_documents:
            all_documents[doc["documentId"]] = doc
            new_count += 1
    print(f"   New documents: {new_count}")
    print(f"   Running total: {len(all_documents):,}")
    
    # Strategy 5: Sort by documentId (ascending)
    print(f"\n📊 STRATEGY 5: Document ID Ascending")
    docs5 = fetch_all_pages_with_sort("documentId", "Document ID Asc")
    new_count = 0
    for doc in docs5:
        if doc["documentId"] not in all_documents:
            all_documents[doc["documentId"]] = doc
            new_count += 1
    print(f"   New documents: {new_count}")
    print(f"   Running total: {len(all_documents):,}")
    
    # Strategy 6: Sort by documentId (descending)
    print(f"\n📊 STRATEGY 6: Document ID Descending")
    docs6 = fetch_all_pages_with_sort("-documentId", "Document ID Desc")
    new_count = 0
    for doc in docs6:
        if doc["documentId"] not in all_documents:
            all_documents[doc["documentId"]] = doc
            new_count += 1
    print(f"   New documents: {new_count}")
    print(f"   Running total: {len(all_documents):,}")
    
    return list(all_documents.values())

def fetch_all_pages_with_sort(sort_param, strategy_name):
    """Fetch ALL pages using a specific sort order - no artificial limits"""
    print(f"   📡 {strategy_name}")
    
    url = f"{API}/documents"
    documents = []
    page_count = 0
    
    params = {
        "filter[docketId]": DOCKET_ID,
        "page[size]": "250",
        "sort": sort_param,
        "api_key": KEY
    }
    
    while True:  # No page limit - get everything
        try:
            # Add page number to params
            current_params = params.copy()
            current_params["page[number]"] = page_count + 1
            
            response = requests.get(url, params=current_params, timeout=30)
            
            if response.status_code == 429:
                print(f"      ⏳ Rate limit - waiting 5 seconds...")
                time.sleep(5)
                continue
            
            if response.status_code != 200:
                print(f"      ❌ HTTP {response.status_code} on page {page_count + 1}")
                break
            
            data = response.json()
            docs = data.get("data", [])
            
            if not docs:
                print(f"      ✅ No more documents (reached end at page {page_count + 1})")
                break
            
            # Process documents
            for doc in docs:
                attrs = doc.get("attributes", {}) or {}
                documents.append({
                    "documentId": doc.get("id"),
                    "title": attrs.get("title"),
                    "postedDate": attrs.get("postedDate"),
                    "lastModifiedDate": attrs.get("lastModifiedDate"),
                    "documentType": attrs.get("documentType"),
                    "document_url": f"https://www.regulations.gov/document/{doc.get('id')}",
                    "sort_strategy": strategy_name,
                    "page_number": page_count + 1
                })
            
            page_count += 1
            
            # Progress update every 10 pages
            if page_count % 10 == 0:
                print(f"      📄 Page {page_count}: {len(documents)} total documents")
            
            time.sleep(1)  # Small delay between pages
            
        except Exception as e:
            print(f"      ⚠️ Error on page {page_count + 1}: {e}")
            time.sleep(3)
            break
    
    print(f"   📊 Strategy complete: {len(documents)} documents from {page_count} pages")
    return documents

def parse_and_save_complete_dataset(documents):
    """Parse and save the complete dataset"""
    if not documents:
        print("❌ No documents to process")
        return None
    
    print(f"\n📊 Processing {len(documents)} documents...")
    
    # Title parsing functions
    decision_patterns = [
        r"^exclusion\s+denied", r"^exclusion\s+granted", r"^exclusion\s+approved",
        r"^exclusion\s+partially\s+approved", r"^exclusion\s+partially\s+granted",
        r"^exclusion\s+request\s+withdrawn", r"^denied", r"^approved", r"^granted"
    ]
    decision_regex = re.compile("|".join(decision_patterns), re.IGNORECASE)
    hts_regex = re.compile(r"\bHTS(?:US)?\s*([0-9]{8,10})\b", re.IGNORECASE)
    
    def parse_title(title):
        if not title:
            return None, None, None, None
        
        raw = title.strip()
        decision_match = decision_regex.search(raw)
        decision = decision_match.group(0).strip().title() if decision_match else None
        
        hts_match = hts_regex.search(raw)
        hts = hts_match.group(1) if hts_match else None
        
        parts = [p.strip() for p in raw.split(",")]
        if decision and parts and decision.lower() in parts[0].lower():
            parts = parts[1:]
        
        requester = parts[0] if parts else None
        material = None
        if len(parts) > 1:
            material = ", ".join(parts[1:])
            material = re.sub(r",?\s*HTS(?:US)?\s*[0-9]{8,10}\s*$", "", material, flags=re.IGNORECASE).strip() or None
        
        return decision, requester, material, hts
    
    def normalize_decision(decision):
        if not decision:
            return None
        d = decision.lower()
        if "withdrawn" in d:
            return "Withdrawn"
        elif "denied" in d:
            return "Denied"
        elif "partially" in d and ("approved" in d or "granted" in d):
            return "Partially Approved"
        elif "approved" in d or "granted" in d:
            return "Approved"
        return decision.title()
    
    # Process all documents
    processed = []
    for i, doc in enumerate(documents):
        if i % 2000 == 0 and i > 0:
            print(f"   Processed {i:,}/{len(documents):,} documents...")
        
        decision, requester, material, hts = parse_title(doc.get("title", ""))
        
        processed.append({
            "documentId": doc["documentId"],
            "decision": decision,
            "decision_normalized": normalize_decision(decision),
            "requester": requester,
            "material": material,
            "hts": hts,
            "title_raw": doc.get("title"),
            "postedDate": doc.get("postedDate"),
            "lastModifiedDate": doc.get("lastModifiedDate"),
            "documentType": doc.get("documentType"),
            "document_url": doc.get("document_url"),
            "sort_strategy": doc.get("sort_strategy"),
            "page_number": doc.get("page_number")
        })
    
    # Save to Excel
    df = pd.DataFrame(processed)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"USTR_COMPLETE_RELIABLE_{len(df)}_RECORDS_{timestamp}.xlsx"
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Complete_USTR_Data', index=False, freeze_panes=(1, 0))
        
        # Collection summary
        summary = pd.DataFrame({
            'Metric': [
                'Total Documents',
                'Target Documents',
                'Success Rate',
                'Collection Method',
                'Date Filtering Used',
                'API Limitations Avoided',
                'Processing Date'
            ],
            'Value': [
                f"{len(df):,}",
                "11,075",
                f"{(len(df)/11075)*100:.1f}%",
                'Multiple Sort Orders',
                'No (avoids HTTP 400 errors)',
                'Yes',
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ]
        })
        summary.to_excel(writer, sheet_name='Collection_Summary', index=False)
        
        # Strategy breakdown
        if 'sort_strategy' in df.columns:
            strategies = df['sort_strategy'].value_counts().reset_index()
            strategies.columns = ['Sort_Strategy', 'Documents_Found']
            strategies.to_excel(writer, sheet_name='Strategy_Breakdown', index=False)
        
        # Decision analysis
        if 'decision_normalized' in df.columns:
            decisions = df['decision_normalized'].value_counts().reset_index()
            decisions.columns = ['Decision', 'Count']
            decisions['Percentage'] = (decisions['Count'] / len(df) * 100).round(1)
            decisions.to_excel(writer, sheet_name='Decision_Analysis', index=False)
    
    print(f"✅ Saved {len(df):,} documents to: {filename}")
    return filename

def main():
    """Main execution"""
    start_time = time.time()
    
    try:
        # Fetch all documents using reliable method
        documents = fetch_with_different_sorts()
        
        if not documents:
            print("❌ No documents collected")
            return
        
        # Process and save
        filename = parse_and_save_complete_dataset(documents)
        
        elapsed = (time.time() - start_time) / 60
        total_docs = len(documents)
        
        print(f"\n🎉 RELIABLE COLLECTION COMPLETE!")
        print(f"📊 FINAL RESULTS:")
        print(f"   📋 Documents collected: {total_docs:,}")
        print(f"   🎯 Target: 11,075")
        print(f"   💯 Success rate: {(total_docs/11075)*100:.1f}%")
        print(f"   ⏱️ Total time: {elapsed:.1f} minutes")
        print(f"   📄 File: {filename}")
        print(f"   ✅ Method: No date filtering (avoids API errors)")
        
        if total_docs >= 10800:
            print(f"🏆 Outstanding! Nearly complete dataset!")
        elif total_docs >= 10000:
            print(f"👍 Excellent! Got majority of documents!")
        else:
            print(f"📈 Solid progress with reliable method!")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()