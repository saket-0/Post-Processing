import google.generativeai as genai
import time
import json
import logging
import random
from config import PREFERRED_MODELS, RATE_LIMIT_DELAY

def worker_task(batch_groups, key_manager, data_manager):
    """
    batch_groups: List[BookGroup] objects.
    Each BookGroup represents a unique Title+Author combination (deduplicated).
    """
    if not batch_groups: return
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
            
            # --- 1. PREPARE INPUT ---
            # We use the BookGroup to generate the prompt line with the HASH ID.
            # Example: "ID: 'a4f3d2e1' -> Physics, By Resnick (Ed. 5)"
            input_lines = [bg.get_prompt_line() for bg in batch_groups]
            batch_text = "\n".join(input_lines)
            
            # --- 2. PROMPT (UNCHANGED) ---
            # We keep the prompt exactly as is. The prompt expects "ID" strings.
            # We are simply providing shorter, safer ID strings.
            prompt = f"""
            You are a Bibliographic Data Enrichment Engine for a Technical University Library.
            I will provide a list of {len(batch_groups)} books with unique string IDs.
            
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
            
            # --- 3. CALL API ---
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
            
            # --- 4. STRICT MAPPING (No more fuzzy guessing) ---
            if result:
                key_manager.report_success(api_key)
                
                clean_result = {}
                failed_items = []

                for bg in batch_groups:
                    # We look up using the Hash ID we generated
                    if bg.content_hash in result:
                        clean_result[bg.content_hash] = result[bg.content_hash]
                    else:
                        failed_items.append(bg)
                
                if clean_result:
                    data_manager.submit_result(clean_result)
                
                if failed_items:
                    # If specific items failed, retry only those
                    batch_groups = failed_items
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