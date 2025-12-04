import google.generativeai as genai
import time
import json
import logging
import random
import re
from config import PREFERRED_MODELS, RATE_LIMIT_DELAY
from datetime import datetime, date 

# --- DATE PARSER ---
def parse_library_date(date_str):
    """
    Parses various date formats from library data into a datetime.date object.
    This function prioritizes finding a day/month/year (DD/MM/YYYY) for accuracy,
    and defaults to Jan 1st for year-only formats to allow comparison.
    
    Returns: datetime.date object or None.
    """
    if not date_str: return None
    s = str(date_str).strip().upper().replace('N/A', '')
    # Check for known placeholders and empty strings
    if s in ['NONE', 'NAN', 'NULL', 'UNKNOWN', '', '0', 'YYYY', 'MMDD', '0000', '000000']: return None
    
    # 1. ISO Format (YYYY)
    match_iso_year = re.search(r'^(19|20)\d{2}$', s)
    if match_iso_year: 
        try: return date(int(match_iso_year.group(0)), 1, 1) # Use Jan 1 for year-only dates
        except: pass
    
    # 2. 6-digit Compact (YYMMDD) - MARC Standard
    # Example: 211207 -> 2021/12/07 (Dec 7, 2021)
    if len(s) == 6 and s.isdigit():
        try:
            yy = int(s[:2])
            month = int(s[2:4])
            day = int(s[4:])
            
            # MARC Heuristic: YY 00-50 -> 2000s, YY 51-99 -> 1900s
            year = 1900 + yy if yy > 50 else 2000 + yy
            
            # Basic validation
            if 1 <= month <= 12 and 1 <= day <= 31:
                # Use datetime to ensure the date is valid (e.g., handles 2/30)
                return date(year, month, day)
        except ValueError:
            # Catches invalid dates like 200230 (Feb 30)
            pass
        except: 
            pass
            
    # 3. Short Format D/M/YY or DD/MM/YYYY
    match_short = re.match(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})', s)
    if match_short:
        try:
            day, month, y_part = map(int, match_short.groups())
            if len(str(y_part)) == 2:
                # Heuristic for 20th vs 21st century
                year = 1900 + y_part if y_part > 50 else 2000 + y_part
            else:
                year = y_part

            # Swap day/month if month > 12 and day <= 12 (assuming common DD/MM/YYYY format)
            if month > 12 and day <= 12:
                 day, month = month, day
            
            return date(year, month, day)
        except: pass

    return None

def find_oldest_date(date_candidates):
    """
    Parses a list of raw date strings and returns the oldest valid date object.
    """
    valid_dates = []
    for raw_date in date_candidates:
        d = parse_library_date(raw_date)
        if d:
            valid_dates.append(d)
    
    if valid_dates:
        return min(valid_dates)
    return None

def resolve_provenance(parts):
    """
    Determines the single most efficient provenance context (Edition or Oldest Date) 
    based on the user's priority rules.
    
    Priority 1: Edition (parts[2])
    Priority 2: Oldest of (PubYear (parts[3]), BillDate (parts[5]), DateAcquired (parts[6]))
    
    Returns: A formatted string (e.g., "(Ed. 5th Edition)" or "(01/01/1975)") or None.
    """
    # Fingerprint structure: 
    # 0:Title, 1:Author, 2:Edition, 3:PubYear, 4:DateEntered, 5:BillDate, 6:DateAcquired, 7:DateLastSeen
    
    p_edition = parts[2] if len(parts) > 2 else ""
    
    # --- PRIORITY 1: EDITION ---
    edition = p_edition.strip()
    if edition and len(edition) > 1 and edition.upper() not in ['NONE', 'NAN', 'NULL', 'UNKNOWN', '0']:
        return f"(Ed. {edition})"
    
    # --- PRIORITY 2: OLDEST DATE ---
    # The list of fields to check for the oldest date.
    # Excludes 'Date Entered' (index 4) and 'Date Last Seen' (index 7).
    date_candidates = [
        parts[3] if len(parts)>3 else "", # Pub Year (008)
        parts[5] if len(parts)>5 else "", # Bill Date (952b)
        parts[6] if len(parts)>6 else ""  # Date Acquired (952d)
    ]
    
    oldest_date = find_oldest_date(date_candidates)
    
    if oldest_date:
        # Format as dd/mm/yyyy as requested
        return f"({oldest_date.strftime('%d/%m/%Y')})"

    return None

# --- FUZZY MATCHER ---
def find_data_for_fingerprint(target_fp, response_json):
    if target_fp in response_json: return response_json[target_fp]
    target_title = target_fp.split('|||')[0].strip().lower()
    for json_key, data in response_json.items():
        j_key_clean = json_key.strip().lower()
        if j_key_clean in target_fp.lower() or target_title in j_key_clean:
            return data
    return None

def worker_task(batch, key_manager, data_manager):
    if not batch: return
    retry_count = 0
    
    while retry_count < 3:
        key_obj = key_manager.get_available_key()
        if not key_obj:
            time.sleep(10)
            continue

        api_key = key_obj.key
        time_since_use = time.time() - key_obj.last_used
        if time_since_use < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - time_since_use)

        try:
            genai.configure(api_key=api_key)
            
            # --- OPTIMIZED INPUT FORMATTING ---
            book_lines = []
            for fp in batch:
                parts = fp.split('|||')
                title = parts[0] if len(parts) > 0 else "Unknown Title"
                author = parts[1] if len(parts) > 1 else "Unknown Author"
                
                # Get Edition or Oldest Date or None using the new logic
                date_context = resolve_provenance(parts)
                
                # Format: "ID -> {Full Book Title}, By {Author / s Name} ({Context})"
                if date_context:
                    # New, cleaner format for the LLM input
                    line_text = f"{title}, By {author} {date_context}"
                else:
                    line_text = f"{title}, By {author}"
                
                # We map it to ID so the AI knows where to put the JSON key
                book_lines.append(f"ID: '{fp}' -> {line_text}")

            batch_text = "\n".join(book_lines)
            
            # --- YOUR REQUESTED PROMPT ---
            prompt = f"""
            You are a Bibliographic Data Enrichment Engine for a Technical University Library.
            I will provide a list of {len(batch)} books with unique string IDs.
            
            YOUR TASK:
            Analyze the Title, Author, Edition, and Year (current year is 2025) to infer technical content and current-day relevance.
            
            OUTPUT REQUIREMENTS:
            Return a single, valid JSON object where:
            1. The dictionary KEYS are exactly the "ID" strings provided in the input (Do not modify them).
            2. The VALUES are objects containing these EXACT fields:
               
               [CONTENT FIELDS]
               - "description": A dense, 2 to 3 sentences of technical summary (approx 25-80 words). Focus on specific concepts the student will learn (e.g., "Covers thermodynamics, entropy, and heat transfer...").
               - "tags": A list of 6 to 8 specific domain keywords to aid search discovery. [Note: Avoid reusing words already present in the Title/Author].

               [SMART LIBRARIAN FIELDS]
               - "critical_review": A 1-sentence judgment on the book's utility in 2025. Explicitly mention if the edition is old or the technology is deprecated (e.g., "Refers to Python 2.7" or "Predates the iPhone").
               - "is_outdated": Boolean (true if the technology or edition is considered obsolete today).
               - "scores": An object with integer ratings (1-10):
                    - "relevance": (1=Obsolete/History, 10=Essential Modern Standard).
                    - "readability": (1=Dense/Research Paper, 10=Beginner Friendly/Novel-like).
                    - "depth": (1=Surface/Intro, 10=Deep/Specialized).
            
            INPUT DATA:
            {batch_text}
            
            STRICT JSON FORMAT EXAMPLE:
            {{
              "engineering drawing | bhatt...": {{
                "description": "A comprehensive guide to orthographic projections and isometric views, covering standard conventions in engineering graphics. It details the geometry of solids and machine parts for manufacturing.",
                "tags": ["Orthographic", "Isometric", "CAD", "Drafting", "Geometry"],
                "critical_review": "A classic foundational text, though modern students may prefer texts with integrated AutoCAD examples.",
                "is_outdated": false,
                "scores": {{ "relevance": 7, "readability": 6, "depth": 5 }}
              }}
            }}
            """
            
            result = None
            for model_name in PREFERRED_MODELS:
                try:
                    model = genai.GenerativeModel(model_name)
                    resp = model.generate_content(prompt)
                    text = resp.text.replace("```json", "").replace("```", "").strip()
                    if "{" in text: text = text[text.find("{"):text.rfind("}")+1]
                    result = json.loads(text)
                    break 
                except Exception as e:
                    if "429" in str(e): raise e
                    continue
            
            if result:
                key_manager.report_success(api_key)
                
                clean_result = {}
                missing_items = []

                for fp in batch:
                    data = find_data_for_fingerprint(fp, result)
                    if data:
                        clean_result[fp] = data
                    else:
                        missing_items.append(fp)
                
                if clean_result:
                    data_manager.submit_result(clean_result)
                
                if missing_items:
                    batch = missing_items
                    retry_count += 1
                    time.sleep(1)
                    continue
                else:
                    return
            
            else:
                raise Exception("Models failed to generate JSON")

        except Exception as e:
            if "429" in str(e): key_manager.report_failure(api_key, "Quota")
            elif "400" in str(e): key_manager.report_failure(api_key, "Invalid")
            else: logging.error(f"Worker Error: {e}")
            time.sleep(random.uniform(2, 5))