import google.generativeai as genai
import time
import json
import logging
import random
import re
from config import PREFERRED_MODELS, RATE_LIMIT_DELAY

# --- DATE PARSER ---
def parse_library_date(date_str):
    if not date_str: return None
    s = str(date_str).strip().upper()
    if s in ['NONE', 'NAN', 'NULL', 'UNKNOWN', '', '0']: return None
    
    # ISO Format
    match_iso = re.search(r'(19|20)\d{2}', s)
    if match_iso: return match_iso.group(0)
    
    # 6-digit Compact (YYMMDD) - MARC Standard
    if len(s) == 6 and s.isdigit():
        yy = int(s[:2])
        if 1 <= int(s[2:4]) <= 12 and 1 <= int(s[4:]) <= 31:
            return f"19{yy:02d}" if yy > 50 else f"20{yy:02d}"
            
    # Short Format D/M/YY
    match_short = re.search(r'\d{1,2}[/-]\d{1,2}[/-](\d{2,4})', s)
    if match_short:
        y_part = match_short.group(1)
        if len(y_part) == 4: return y_part
        if len(y_part) == 2: return f"19{y_part}" if int(y_part) > 50 else f"20{y_part}"
    return None

def resolve_provenance(parts):
    """
    Returns ONLY the raw Date/Edition string. No labels.
    """
    # Indices from data_manager.py:
    # 0:Title, 1:Author, 2:Edition, 3:PubYear, 4:Added, 5:Bill, 6:Acq, 7:Seen
    
    p_edition = parts[2] if len(parts) > 2 else ""
    
    # Priority 1: Edition (If valid text)
    if p_edition and len(p_edition) > 1 and p_edition.upper() not in ['NONE', 'NAN']:
        return p_edition # e.g., "5th Edition"
    
    # Priority 2-5: Check Dates (Removed 'Last Seen' as it is misleading)
    date_candidates = [
        parts[3] if len(parts)>3 else "", # Pub Year
        parts[4] if len(parts)>4 else "", # Added
        parts[5] if len(parts)>5 else "", # Bill
        parts[6] if len(parts)>6 else ""  # Acq
    ]
    
    for raw_date in date_candidates:
        clean_year = parse_library_date(raw_date)
        if clean_year:
            return clean_year # e.g., "2021"

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
                
                # Get Year or None
                date_context = resolve_provenance(parts)
                
                # Format: "ID -> Title, by Author (Year)"
                if date_context:
                    line_text = f"{title}, by {author} ({date_context})"
                else:
                    line_text = f"{title}, by {author}"
                
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
               - "description": A dense, 2-3-sentence technical summary (approx 25-80 words). Focus on specific concepts the student will learn (e.g., "Covers thermodynamics, entropy, and heat transfer...").
               - "tags": A list of 6-8 specific domain keywords to aid search discovery. [Note: Avoid reusing words already present in the Title/Author].

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