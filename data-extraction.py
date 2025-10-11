import pandas as pd
import requests
from io import BytesIO
import time
from datetime import datetime
import re
import json
from difflib import SequenceMatcher
import yaml  # pip install pyyaml

try:
    import pdfplumber
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pdfplumber"])
    import pdfplumber

try:
    import pytesseract
    from PIL import Image
    import pdf2image
    HAS_OCR = True
except ImportError:
    HAS_OCR = False
    print("Warning: OCR libraries not installed. Install with:")
    print("pip install pytesseract pdf2image pillow")
    print("Also need: apt-get install tesseract-ocr poppler-utils")

# Configuration dictionary (could be loaded from YAML/JSON)
CONFIG = {
    "ocr_settings": {
        "enabled": HAS_OCR,
        "min_text_length": 100,  # Minimum text length before trying OCR
        "dpi": 200,  # DPI for pdf2image conversion
        "language": "eng"  # Tesseract language
    },
    
    "htsus_patterns": [
        r'\b\d{10}\b',  # Standard 10-digit
        r'\b\d{4}\.\d{2}\.\d{2}\.\d{2}\b',  # With dots: 1234.56.78.90
        r'\b\d{4}\s\d{2}\s\d{2}\s\d{2}\b',  # With spaces: 1234 56 78 90
        r'\b\d{4}\.\d{2}\.\d{4}\b',  # Alternative: 1234.56.7890
        r'(?:HTSUS|HTS)[\s:#]*(\d{4}[\.\s]?\d{2}[\.\s]?\d{2,4})',  # Prefixed
    ],
    
    "fuzzy_matching": {
        "enabled": True,
        "threshold": 0.85,  # Similarity threshold (0-1)
        "keywords": {
            "requestor": ["requestor", "requester", "requestee", "reqestor"],
            "organization": ["organization", "organisation", "org", "company"],
            "exclusion": ["exclusion", "exlusion", "exclussion", "exclude"],
            "section": ["section", "sec", "sect"],
            "htsus": ["htsus", "hts", "harmonized tariff"],
            "confidential": ["confidential", "confidental", "confidetial"],
        }
    },
}

def handle_cid_garbled_text(text, debug=False):
    """Handle CID-based garbled text extraction"""
    
    # First check if we have significant CID patterns
    cid_matches = re.findall(r'\(cid:\d+\)', text)
    if len(cid_matches) < 10:  # Not heavily CID-garbled
        if debug:
            print(f"Not enough CID patterns found: {len(cid_matches)}")
        return text
    
    if debug:
        print(f"Processing {len(cid_matches)} CID patterns")
        # Show first 10 unique CID numbers
        unique_cids = list(set(re.findall(r'\(cid:(\d+)\)', text)))[:10]
        print(f"Sample CID numbers: {unique_cids}")
    
    original_text = text
    
    # Specific CID mappings based on observed patterns
    specific_cid_mappings = {
        r'\(cid:52\)\(cid:70\)\(cid:68\)\(cid:85\)\(cid:74\)\(cid:80\)\(cid:79\)': 'SECTION',
        r'\(cid:52\)\(cid:49\)\(cid:49\)': '301', 
        r'\(cid:74\)\(cid:79\)\(cid:69\)\(cid:74\)\(cid:68\)\(cid:66\)\(cid:85\)\(cid:70\)': 'INDICATE',
        r'\(cid:81\)\(cid:83\)\(cid:80\)\(cid:87\)\(cid:74\)\(cid:69\)\(cid:70\)': 'PROVIDE',
        r'\(cid:83\)\(cid:70\)\(cid:82\)\(cid:86\)\(cid:70\)\(cid:84\)\(cid:85\)\(cid:80\)\(cid:83\)': 'REQUESTOR',
        r'\(cid:79\)\(cid:83\)\(cid:72\)\(cid:66\)\(cid:79\)\(cid:74\)\(cid:91\)\(cid:66\)\(cid:85\)\(cid:74\)\(cid:80\)\(cid:79\)': 'ORGANIZATION',
        r'\(cid:81\)\(cid:86\)\(cid:67\)\(cid:77\)\(cid:74\)\(cid:68\)': 'PUBLIC',
        r'\(cid:67\)\(cid:68\)\(cid:74\)': 'BCI',
        r'\(cid:87\)\(cid:70\)\(cid:83\)\(cid:84\)\(cid:74\)\(cid:80\)\(cid:79\)': 'VERSION',
        r'\(cid:68\)\(cid:80\)\(cid:79\)\(cid:85\)\(cid:74\)\(cid:79\)\(cid:86\)\(cid:70\)\(cid:69\)': 'CONTINUED',
        r'\(cid:70\)\(cid:89\)\(cid:68\)\(cid:77\)\(cid:86\)\(cid:84\)\(cid:74\)\(cid:80\)\(cid:79\)': 'EXCLUSION',
        r'\(cid:69\)\(cid:80\)\(cid:68\)\(cid:86\)\(cid:78\)\(cid:70\)\(cid:79\)\(cid:85\)': 'DOCUMENT',
        r'\(cid:71\)\(cid:80\)\(cid:83\)\(cid:78\)': 'FORM',
        r'\(cid:73\)\(cid:85\)\(cid:84\)\(cid:86\)\(cid:84\)': 'HTSUS',
        r'\(cid:68\)\(cid:73\)\(cid:74\)\(cid:79\)\(cid:66\)': 'CHINA',
    }
    
    # Apply specific CID mappings first
    replacements_made = 0
    for cid_pattern, replacement in specific_cid_mappings.items():
        before_count = len(re.findall(cid_pattern, text))
        text = re.sub(cid_pattern, replacement, text)
        after_count = len(re.findall(cid_pattern, text))
        if before_count > 0:
            replacements_made += before_count - after_count
            if debug:
                print(f"Replaced {before_count} instances of pattern -> '{replacement}'")
    
    # Create a comprehensive CID-to-character mapping
    cid_char_map = {}
    
    # Add all ASCII characters
    for i in range(32, 127):
        cid_char_map[i] = chr(i)
    
    # Convert individual CID numbers to characters
    def cid_to_char(match):
        cid_num = int(match.group(1))
        return cid_char_map.get(cid_num, ' ')  # Return space for unknown CIDs
    
    # Apply character-by-character conversion
    before_individual = len(re.findall(r'\(cid:\d+\)', text))
    text = re.sub(r'\(cid:(\d+)\)', cid_to_char, text)
    after_individual = len(re.findall(r'\(cid:\d+\)', text))
    individual_replacements = before_individual - after_individual
    
    if debug:
        print(f"Specific pattern replacements: {replacements_made}")
        print(f"Individual CID conversions: {individual_replacements}")
        before_total = len(re.findall(r'\(cid:\d+\)', original_text))
        after_total = len(re.findall(r'\(cid:\d+\)', text))
        print(f"Total CID patterns before: {before_total}")
        print(f"Total CID patterns after: {after_total}")
        if individual_replacements > 0:
            print(f"Sample converted text: {text[:200]}...")
    
    return text

def perform_ocr_on_pdf(pdf_stream, debug=False):
    """Perform OCR on PDF if text extraction fails"""
    if not HAS_OCR:
        return ""
    
    try:
        import pdf2image
        import pytesseract
        
        if debug:
            print("Attempting OCR extraction...")
        
        # Convert PDF to images
        images = pdf2image.convert_from_bytes(
            pdf_stream.getvalue(),
            dpi=CONFIG["ocr_settings"]["dpi"]
        )
        
        ocr_text = ""
        for i, image in enumerate(images[:5], 1):  # OCR first 5 pages max
            if debug:
                print(f"  OCR on page {i}...")
            page_text = pytesseract.image_to_string(
                image, 
                lang=CONFIG["ocr_settings"]["language"]
            )
            ocr_text += page_text + "\n"
        
        if debug:
            print(f"OCR extracted {len(ocr_text)} characters")
        
        return ocr_text
        
    except Exception as e:
        if debug:
            print(f"OCR failed: {e}")
        return ""

def extract_htsus_codes(text):
    """Extract HTSUS codes with multiple format support"""
    all_codes = set()
    
    for pattern in CONFIG["htsus_patterns"]:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            # Normalize the code (remove dots/spaces)
            if isinstance(match, tuple):
                match = match[0]
            normalized = re.sub(r'[\.\s]', '', str(match))
            
            # Validate it's 10 digits or 8 digits
            if len(normalized) in [8, 10] and normalized.isdigit():
                # Exclude obvious test patterns
                if normalized not in ['1234567890', '0000000000', '1111111111', '1023456789']:
                    all_codes.add(normalized)
    
    return list(all_codes)

def detect_section_301_final_enhanced(pdf_url, att_id, debug=False):
    """Enhanced detector with better CID handling and debugging"""
    
    try:
        response = requests.get(pdf_url, timeout=60)
        response.raise_for_status()
        
        pdf_stream = BytesIO(response.content)
        
        first_2_pages = ""
        full_text = ""
        
        with pdfplumber.open(pdf_stream) as pdf:
            # First 2 pages for exclusion
            for i in range(min(2, len(pdf.pages))):
                try:
                    first_2_pages += (pdf.pages[i].extract_text() or "") + "\n"
                except:
                    continue
            
            # Full document for inclusion  
            for page in pdf.pages:
                try:
                    full_text += (page.extract_text() or "") + "\n"
                except:
                    continue
        
        # Check if we need OCR (optional enhancement)
        if len(full_text.strip()) < CONFIG["ocr_settings"]["min_text_length"] and HAS_OCR:
            if debug:
                print(f"Text extraction yielded only {len(full_text)} chars, trying OCR...")
            ocr_text = perform_ocr_on_pdf(pdf_stream, debug)
            if ocr_text:
                full_text = ocr_text
                first_2_pages = "\n".join(ocr_text.split("\n")[:100])
        
        if len(full_text.strip()) < 50:
            return False, "Text extraction failed", 0
        
        if debug:
            print(f"DEBUG for {att_id}")
            print(f"Full document length: {len(full_text)} characters")
            print(f"First 500 chars of ORIGINAL text:")
            print(repr(first_2_pages[:500]))
        
        # Clean text - Remove control characters
        clean_first_2 = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', first_2_pages)
        clean_full = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', full_text)
        
        # Handle CID garbled text
        if '(cid:' in clean_first_2 or '(cid:' in clean_full:
            if debug:
                print("Detected CID patterns, attempting to decode...")
                cid_count = len(re.findall(r'\(cid:\d+\)', clean_first_2 + clean_full))
                print(f"CID pattern count: {cid_count}")
            clean_first_2 = handle_cid_garbled_text(clean_first_2, debug=debug)
            clean_full = handle_cid_garbled_text(clean_full, debug=debug)
            
            if debug:
                print(f"First 500 chars of CID-CONVERTED text:")
                print(repr(clean_first_2[:500]))
        
        # FIX FOR DOUBLED CHARACTERS
        def deduplicate_chars(text):
            """Remove consecutive duplicate characters"""
            result = []
            prev_char = ''
            for char in text:
                if char != prev_char:
                    result.append(char)
                    prev_char = char
            return ''.join(result)
        
        # Check if text has doubled characters
        sample = clean_first_2[:500] if len(clean_first_2) > 500 else clean_first_2
        if sample:
            doubled_count = sum(1 for i in range(1, len(sample)) if sample[i] == sample[i-1])
            if doubled_count > len(sample) * 0.3:  # More than 30% doubled
                if debug:
                    print(f"Detected doubled characters, applying deduplication...")
                clean_first_2 = deduplicate_chars(clean_first_2)
                clean_full = deduplicate_chars(clean_full)
        
        # Apply garbled text fixes
        fixes = {'0SHBOJ[BUJPO':'Organization', '/BNF':'Name', '3FRVFTUPS':'Requestor', 
                '4FDUJPO':'Section', '*OWFTUJHBUJPO':'Investigation', '7&34*0/':'VERSION', 
                '$0/5*/6&%':'CONTINUED', '#&-08':'BELOW'}
        for garbled, clean in fixes.items():
            clean_first_2 = clean_first_2.replace(garbled, clean)
            clean_full = clean_full.replace(garbled, clean)
        
        # ========== EXCLUSION LOGIC ==========
        
        # EXCLUSION 1: Traditional business letter format
        has_dear = bool(re.search(r'Dear\s+(Ambassador|Mr\.|Ms\.|Dr\.)', first_2_pages + clean_first_2))
        
        if has_dear:
            return False, f"EXCLUDED: Dear letter format", -100
        
        # EXCLUSION 3: Narrative business letter
        narrative_indicators = [
            "respectfully request" in (first_2_pages + clean_first_2).lower(),
            "hereby request" in (first_2_pages + clean_first_2).lower(), 
            "we request" in (first_2_pages + clean_first_2).lower(),
            "submits this request" in (first_2_pages + clean_first_2).lower(),
            "request to exclude" in (first_2_pages + clean_first_2).lower(),
            "exclude certain products" in (first_2_pages + clean_first_2).lower(),
            "exclusion from the tariff" in (first_2_pages + clean_first_2).lower(),
            "seeks an exclusion" in (first_2_pages + clean_first_2).lower(),
            "request for exclusion" in (first_2_pages + clean_first_2).lower(),
            "this exclusion request" in (first_2_pages + clean_first_2).lower(),
            "on behalf of" in (first_2_pages + clean_first_2).lower(),
        ]
        
        has_narrative_structure = (
            len(full_text) > 3000 and
            len(re.findall(r'\n\n', full_text)) > 5
        )
        
        narrative_count = sum(narrative_indicators)
        is_narrative_letter = (
            (narrative_count >= 2 and has_narrative_structure) or
            (narrative_count >= 3 and len(full_text) > 2000)
        )
        
        if debug:
            print(f"NARRATIVE EXCLUSION DEBUG:")
            print(f"  Narrative indicators found: {narrative_count}")
            print(f"  Is narrative letter: {is_narrative_letter}")
        
        if is_narrative_letter:
            return False, f"EXCLUDED: Narrative business letter (indicators:{narrative_count})", -100
        
        # EXCLUSION 4: Company letterhead format
        business_checks = [
            bool(re.search(r'[A-Z\s]+ LLC', first_2_pages + clean_first_2)),
            bool(re.search(r'[A-Z\s]+ GROUP', first_2_pages + clean_first_2)),
            bool(re.search(r'[A-Z\s]+,?\s+(INC|Inc)\.?', first_2_pages + clean_first_2)),
            bool(re.search(r'\d+.*Street.*Suite', first_2_pages + clean_first_2)),
            bool(re.search(r'Phone.*\d{3}', first_2_pages + clean_first_2)),
            bool(re.search(r'Email:.*@', first_2_pages + clean_first_2)),
            "On behalf of" in (first_2_pages + clean_first_2),
            "hereby submit" in (first_2_pages + clean_first_2),
            "respectfully request" in (first_2_pages + clean_first_2),
            "Office of U.S. Trade Representative" in (first_2_pages + clean_first_2),
        ]
        
        business_count = sum(business_checks)
        
        if business_count >= 4:
            return False, f"EXCLUDED: Business letterhead (indicators:{business_count})", -100
        
        # EXCLUSION 5: Documents ABOUT exclusion requests
        if len(first_2_pages + clean_first_2) > 1000:
            about_exclusion = (
                "exclusion request is being submitted" in (first_2_pages + clean_first_2).lower() or
                "this exclusion request is for" in (first_2_pages + clean_first_2).lower() or
                "exclusion would undermine" in (first_2_pages + clean_first_2).lower() or
                "we respectfully request that you exclude" in (first_2_pages + clean_first_2).lower() or
                "respectfully request that you exclude" in (first_2_pages + clean_first_2).lower() or
                "we request that you exclude" in (first_2_pages + clean_first_2).lower() or
                "section 301 exclusion request" in (first_2_pages + clean_first_2).lower()
            )
            if about_exclusion:
                return False, f"EXCLUDED: Document about exclusion, not the form itself", -100
        
        # ========== FORM DETECTION LOGIC ==========
        
        # Check for ACTUAL form questions
        questions_v1 = re.findall(r'^\s*(\d+)\.\s*(?:Indicate|Provide|List|Identify|Specify|Enter|Submit)', 
                                 first_2_pages + clean_full, re.MULTILINE | re.IGNORECASE)
        questions_v2 = re.findall(r'\n\s*(\d+)\.\s*(?:Indicate|Provide|List|Identify|Specify|Enter|Submit)', 
                                 first_2_pages + clean_full, re.IGNORECASE)
        questions_v3 = re.findall(r'(\d+)\.(?:Indicate|Provide|List|Identify)', 
                                 first_2_pages + clean_full, re.IGNORECASE)
        questions_v4 = re.findall(r'^\s*(\d+)\s+(?:Indicate|Provide|List|Identify|Specify)', 
                                 first_2_pages + clean_full, re.MULTILINE | re.IGNORECASE)
        
        all_questions = set(questions_v1 + questions_v2 + questions_v3 + questions_v4)
        form_questions = [q for q in all_questions if q in ['1','2','3','4','5','6','7','8','9','10','11','12']]
        has_form_structure = len(form_questions) >= 3
        
        # Check for business letter numbered arguments
        business_arguments = re.findall(r'^\s*\d+\.\s*(?:The|Given|If|When|While|Although|Because)', 
                                       first_2_pages + clean_full, re.MULTILINE | re.IGNORECASE)
        if len(business_arguments) >= 2 and len(form_questions) < 3:
            has_form_structure = False
        
        # Alternative: Check for form field markers
        first_2_lower = (first_2_pages + clean_first_2).lower()
        full_lower = (first_2_pages + clean_first_2 + clean_full).lower()
        
        form_markers = [
            "indicate whether" in first_2_lower,
            "provide any" in first_2_lower,
            "indicate the" in first_2_lower,
            "provide the" in first_2_lower,
            "complete and correct" in first_2_lower,
            "best of your knowledge" in first_2_lower,
            "product exclusion request" in first_2_lower,
            "section 301" in first_2_lower,
            "section" in full_lower and "301" in full_lower,
            "exclusion" in full_lower and ("form" in full_lower or "request" in full_lower),
            "requestor" in full_lower and "organization" in full_lower,
            "public" in full_lower and "bci" in full_lower,
            "version" in full_lower and "continued" in full_lower,
            re.search(r'\b1\b.*\bindicate\b', full_lower) is not None,
            re.search(r'\b2\b.*\bprovide\b', full_lower) is not None,
        ]
        has_form_markers = sum(form_markers) >= 3
        
        # Government form structure patterns
        gov_form_patterns = [
            bool(re.search(r'form\s+to\s+request', full_lower)),
            bool(re.search(r'u\.?s\.?\s+trade\s+representative', full_lower)),
            bool(re.search(r'office.*trade.*representative', full_lower)),
            bool(re.search(r'china.*acts.*section.*301', full_lower)),
            bool(re.search(r'htsus.*subheading', full_lower)),
            bool(re.search(r'product.*exclusion.*301', full_lower)),
            len(re.findall(r'\b\d+\.\s*[A-Za-z]', clean_full)) >= 5,
            bool(re.search(r'business.*confidential.*information', full_lower)),
        ]
        has_gov_form_patterns = sum(gov_form_patterns) >= 2
        
        # Detect filled Section 301 forms
        filled_form_indicators = [
            bool(re.search(r'\b\d{4}\.\d{2}\.\d{2}\.\d{2}\b', full_text + clean_full)),
            "PublicDocument" in (full_text + clean_full) or "Public Document" in (full_text + clean_full),
            "ContainsBCI" in (full_text + clean_full) or "Contains BCI" in (full_text + clean_full),
            "U.S.Producer" in (full_text + clean_full) or "U.S. Producer" in (full_text + clean_full),
            "Pleaseseeattachedsubmission" in (full_text + clean_full) or "Please see attached submission" in (full_text + clean_full),
            bool(re.search(r'[A-Z][a-z]+,[A-Z][a-z]+', full_text + clean_full)),
            bool(re.search(r'[A-Z][A-Za-z0-9]+,Inc\.|Inc\.|LLC', full_text + clean_full)),
            len(full_text + clean_full) < 2000,
        ]
        
        filled_form_score = sum(filled_form_indicators)
        is_filled_form = filled_form_score >= 4
        
        if debug:
            print(f"FILLED FORM DETECTION:")
            print(f"  Filled form indicators: {filled_form_score}/8")
            print(f"  Is filled form: {is_filled_form}")
        
        # Consider it a form
        is_form = has_form_structure or has_form_markers or has_gov_form_patterns or is_filled_form
        
        if debug:
            print(f"Form detection results:")
            print(f"  Form questions found: {len(form_questions)} ({form_questions})")
            print(f"  Form markers: {sum(form_markers)}/15")
            print(f"  Gov form patterns: {sum(gov_form_patterns)}/8")
            print(f"  Is form: {is_form}")
        
        # ========== INCLUSION LOGIC ==========
        
        def check_both(pattern):
            return (pattern in full_text or pattern in clean_full or 
                   pattern.lower() in full_text.lower() or pattern.lower() in clean_full.lower())
        
        def check_both_flexible(pattern):
            exact_match = (pattern in full_text or pattern in clean_full or 
                          pattern.lower() in full_text.lower() or pattern.lower() in clean_full.lower())
            
            if exact_match:
                return True
                
            if pattern == "Section 301":
                flexible_patterns = ["Section301", "Section 301", "SECTION301", "SECTION 301"]
                for fp in flexible_patterns:
                    if (fp in full_text or fp in clean_full or 
                        fp.lower() in full_text.lower() or fp.lower() in clean_full.lower()):
                        return True
            
            elif pattern == "Form to Request Exclusion":
                flexible_patterns = ["FormtoRequestExclusion", "Form to Request Exclusion", 
                                   "FORMTOREQUESTEXCLUSION", "FormRequestExclusion"]
                for fp in flexible_patterns:
                    if (fp in full_text or fp in clean_full or 
                        fp.lower() in full_text.lower() or fp.lower() in clean_full.lower()):
                        return True
            
            elif pattern == "Requestor Name":
                flexible_patterns = ["RequestorName", "Requestor Name", "REQUESTORNAME"]
                for fp in flexible_patterns:
                    if (fp in full_text or fp in clean_full or 
                        fp.lower() in full_text.lower() or fp.lower() in clean_full.lower()):
                        return True
                        
            elif pattern == "Organization Name":
                flexible_patterns = ["OrganizationName", "Organization Name", "ORGANIZATIONNAME"]
                for fp in flexible_patterns:
                    if (fp in full_text or fp in clean_full or 
                        fp.lower() in full_text.lower() or fp.lower() in clean_full.lower()):
                        return True
            
            return False
        
        form_elements = ["VERSION", "CONTINUED", "BCI", "Requestor Name", "Organization Name", 
                        "Section 301", "Form to Request Exclusion", "China Acts"]
        form_found = sum(1 for pattern in form_elements if check_both_flexible(pattern))
        
        if debug:
            print(f"FORM ELEMENT DEBUG:")
            for pattern in form_elements:
                found = check_both_flexible(pattern)
                print(f"  '{pattern}': {found}")
        
        # HTSUS codes with enhanced patterns
        htsus_codes = extract_htsus_codes(full_text + " " + clean_full)
        has_htsus = len(htsus_codes) > 0
        
        has_public = check_both("Public Document") or check_both("Public Version")
        name_format = bool(re.search(r'[A-Z][a-z]+,\s*[A-Z][a-z]+', full_text + clean_full))
        company_format = bool(re.search(r'[A-Za-z\s]+ (LLC|Inc|Corp|Ltd)', full_text + clean_full))
        
        # SCORING SYSTEM
        score = 0
        
        if is_form and form_found >= 1:
            score = form_found * 10
            if form_found >= 3:
                score += 20
            if form_found >= 5:
                score += 30
            return True, f"Form (patterns: {form_found})", score
            
        elif has_public and has_htsus and (name_format or company_format):
            score = 15
            score += len(htsus_codes) * 3
            if name_format:
                score += 5
            if company_format:
                score += 5
            return True, f"Scrambled (HTSUS: {htsus_codes[0]})", score
            
        elif has_htsus and form_found >= 1:
            score = 10
            score += len(htsus_codes) * 2
            score += form_found * 5
            return True, f"Minimal (HTSUS: {htsus_codes[0]})", score
        else:
            return False, "Not Section 301", 0
        
    except Exception as e:
        return False, f"Error: {str(e)[:50]}", -1

def process_rows_enhanced(excel_file, api_key, start_row, end_row, debug_doc_id=None):
    """Process specified rows with enhanced CID handling and optional debugging"""
    
    df = pd.read_excel(excel_file)
    
    # Convert to 0-indexed
    start_idx = start_row - 1
    end_idx = end_row - 1
    
    # Make sure we don't exceed dataframe length
    end_idx = min(end_idx, len(df) - 1)
    
    batch_df = df.iloc[start_idx:end_idx+1]  # +1 because iloc end is exclusive
    doc_id_column = 'documentId' if 'documentId' in df.columns else 'Document_ID'
    
    print(f"Processing {len(batch_df)} documents (rows {start_row} to {end_row})")
    if debug_doc_id:
        print(f"DEBUG MODE enabled for document: {debug_doc_id}")
    print(f"Document IDs: {batch_df[doc_id_column].tolist()[:5]}...")
    print("="*50)
    
    results = []
    forms_found = 0
    excluded_count = 0
    
    for idx, (_, row) in enumerate(batch_df.iterrows(), 1):
        doc_id = row[doc_id_column]
        actual_row = start_idx + idx
        print(f"[{idx}/{len(batch_df)}] Row {actual_row}: {doc_id}")
        
        # Enable debug mode for specific document
        debug_mode = (debug_doc_id and doc_id == debug_doc_id)
        
        result = {
            'documentId': doc_id, 
            'row_number': actual_row, 
            'attachment_url': '', 
            'attachment_id': '',
            'attachment_title': '', 
            'file_size': 0, 
            'detection_result': '', 
            'selection_reason': 'No Section 301 form',
            'best_score': 0,
            'total_attachments_checked': 0
        }
        
        # Copy all columns from original dataframe
        for col in df.columns:
            if col not in result:
                result[col] = row[col]
        
        try:
            response = requests.get(
                f"https://api.regulations.gov/v4/documents/{doc_id}/attachments", 
                params={"api_key": api_key}, 
                timeout=30
            )
            response.raise_for_status()
            attachments_data = response.json()
            
            if 'data' in attachments_data:
                candidates = []
                excluded_attachments = []
                attachments_checked = 0
                
                for j, att in enumerate(attachments_data['data'], 1):
                    try:
                        att_id = att.get('id', f'att_{j}')
                        attributes = att.get('attributes', {})
                        title = attributes.get('title', '')
                        
                        # Skip Re_ files
                        if title and title.startswith('Re_'):
                            print(f"  Att {j}: Skipped (Re_ prefix)")
                            continue
                        
                        # Check what formats are available
                        file_formats = attributes.get('fileFormats', [])
                        if file_formats is None:
                            file_formats = []
                        
                        if not file_formats:
                            continue
                            
                        # Try to process the attachment if it's PDF
                        pdf_processed = False
                        
                        for fmt in file_formats:
                            if isinstance(fmt, dict) and fmt.get('format') == 'pdf':
                                pdf_url = fmt.get('fileUrl', '')
                                file_size = fmt.get('size', 0)
                                
                                if not pdf_url:
                                    pdf_processed = True
                                    break
                                    
                                if file_size <= 5000:
                                    print(f"  Att {j}: Skipped (too small)")
                                    pdf_processed = True
                                    break
                                
                                # Process the PDF with enhanced detection
                                attachments_checked += 1
                                is_form, msg, score = detect_section_301_final_enhanced(pdf_url, att_id, debug=debug_mode)
                                
                                if "EXCLUDED:" in msg:
                                    print(f"  Att {j}: {msg}")
                                    excluded_attachments.append({
                                        'attachment_num': j,
                                        'msg': msg
                                    })
                                elif is_form:
                                    print(f"  Att {j}: DETECTED (Score: {score})")
                                    candidates.append({
                                        'attachment_url': pdf_url,
                                        'attachment_id': att_id,
                                        'attachment_title': title or f'Attachment_{j}',
                                        'file_size': file_size,
                                        'detection_result': msg,
                                        'score': score,
                                        'attachment_num': j
                                    })
                                else:
                                    print(f"  Att {j}: Not Section 301")
                                
                                pdf_processed = True
                                break
                        
                        if not pdf_processed:
                            pass  # Not a PDF, skip
                    
                    except Exception as e:
                        if debug_mode:
                            print(f"  ERROR processing attachment {j}: {e}")
                        continue
                
                result['total_attachments_checked'] = attachments_checked
                
                # Select the best candidate based on highest score
                if candidates:
                    best_candidate = max(candidates, key=lambda x: x['score'])
                    result.update({
                        'attachment_url': best_candidate['attachment_url'],
                        'attachment_id': best_candidate['attachment_id'],
                        'attachment_title': best_candidate['attachment_title'],
                        'file_size': best_candidate['file_size'],
                        'detection_result': best_candidate['detection_result'],
                        'best_score': best_candidate['score'],
                        'selection_reason': f"BEST (score={best_candidate['score']}) in att#{best_candidate['attachment_num']} of {len(candidates)} forms"
                    })
                    forms_found += 1
                    print(f"  ✓ SELECTED: Attachment {best_candidate['attachment_num']} with score {best_candidate['score']}")
                    
                    # Show other candidates if multiple found
                    if len(candidates) > 1:
                        print(f"  Other candidates:")
                        for cand in candidates:
                            if cand != best_candidate:
                                print(f"    - Att {cand['attachment_num']}: Score {cand['score']}")
                
                elif excluded_attachments:
                    excluded_count += 1
                    result['selection_reason'] = f"All {len(excluded_attachments)} attachments excluded"
                    print(f"  ✗ All attachments excluded")
                else:
                    print(f"  ✗ No Section 301 forms found in {attachments_checked} attachments")
        
        except Exception as e:
            if debug_mode:
                print(f"  API ERROR: {str(e)}")
            result['selection_reason'] = f"API Error: {str(e)[:50]}"
        
        results.append(result)
        time.sleep(2)  # Rate limiting
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"section301_enhanced_{start_row}_{end_row}_{timestamp}.xlsx"
    pd.DataFrame(results).to_excel(output_file, index=False)
    
    print("\n" + "="*50)
    print(f"PROCESSING COMPLETE")
    print(f"Documents processed: {len(results)}")
    print(f"Forms found: {forms_found}")
    print(f"Documents excluded: {excluded_count}")
    print(f"Output saved to: {output_file}")
    
    # Show summary of which documents had forms
    forms_list = [r['documentId'] for r in results if r['attachment_url']]
    if forms_list:
        print(f"\nDocuments with Section 301 forms:")
        for doc_id in forms_list:
            print(f"  - {doc_id}")
    
    return results, output_file

def save_config(config_dict, filepath="config.yaml"):
    """Save configuration to file for easy modification"""
    with open(filepath, 'w') as f:
        yaml.dump(config_dict, f, default_flow_style=False)

def load_config(filepath="config.yaml"):
    """Load configuration from file"""
    try:
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        return CONFIG  # Use default if no config file

if __name__ == "__main__":
    EXCEL_FILE = "excel_ids.xlsx"
    API_KEY = "rWRXjXvflW9STOTgc8JKH9kFkkyY7j5qR3GcWba6"
    
    # ============================================
    # CHANGE THESE VALUES FOR YOUR ROW RANGE
    # ============================================
    START_ROW = 1001      # Change this to your start row
    END_ROW = 1100     # Change this to your end row
    
    # Enable debug for specific document (set to None to disable)
    DEBUG_DOC_ID = None  # Set to "USTR-2018-0025-0141" to debug specific document
    
    # Save/load config
    save_config(CONFIG, "section301_config.yaml")
    config = load_config("section301_config.yaml")
    CONFIG.update(config)
    
    print("SECTION 301 DETECTOR - ENHANCED VERSION")
    print(f"Processing rows {START_ROW} to {END_ROW} from Excel file")
    if DEBUG_DOC_ID:
        print(f"DEBUG MODE enabled for: {DEBUG_DOC_ID}")
    print("="*50)
    print("FEATURES:")
    print("- Enhanced CID pattern handling for garbled PDFs")
    print("- Better character mapping for government forms") 
    print("- Exclusions checked BEFORE form detection")
    print("- Debug mode for problematic documents")
    print("- Selects highest scoring attachment")
    print(f"- OCR Support: {'Enabled' if HAS_OCR else 'Disabled (install pytesseract, pdf2image)'}")
    print("- Enhanced HTSUS pattern detection")
    print("="*50)
    
    results, output = process_rows_enhanced(EXCEL_FILE, API_KEY, START_ROW, END_ROW, DEBUG_DOC_ID)
    
#best one works for 500(urls)