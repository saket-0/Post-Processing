import google.generativeai as genai
import time
import json
import logging
import random
from config import PREFERRED_MODELS, RATE_LIMIT_DELAY

def worker_task(batch, key_manager, data_manager):
    
    while True:
        key_obj = key_manager.get_available_key()
        if not key_obj:
            time.sleep(10)
            continue

        api_key = key_obj.key
        
        # Rate Limit Sleep
        time_since_use = time.time() - key_obj.last_used
        if time_since_use < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - time_since_use)

        try:
            genai.configure(api_key=api_key)
            
            # --- CONSTRUCTING THE CONTEXTUAL INPUT ---
            # We parse the 'fingerprint' to give the AI clean data fields
            book_list_text = []
            for fp in batch:
                parts = fp.split('|')
                title = parts[0] if len(parts) > 0 else "Unknown"
                author = parts[1] if len(parts) > 1 else "Unknown"
                edition = parts[2] if len(parts) > 2 else ""
                year = parts[3] if len(parts) > 3 else ""
                
                # We format it nicely for the AI
                book_list_text.append(f"ID: '{fp}'\n   - Title: {title}\n   - Author: {author}\n   - Edition: {edition}\n   - Year: {year}")

            batch_input = "\n".join(book_list_text)
            
            # --- THE "CRITICAL REVIEWER" PROMPT ---
            prompt = f"""
            Act as a Critical Technical Librarian. I have a list of books.
            Analyze them based on today's standards (Current Year: 2025).

            Your Goal: Help a student decide if they should read this book.

            For EACH book (mapped by its exact ID), return a JSON object with:
            1. "summary": A 1-sentence critical review. Mention if it is obsolete.
            2. "tags": 4-5 technical keywords.
            3. "scores": A nested object with integer scores (1-10):
               - "relevance": How useful is this content TODAY? (e.g., Windows 98 book = 1/10. Calculus book = 10/10).
               - "readability": 10 = For Dummies/Novels, 1 = Dense Research Papers.
               - "depth": 1 = Surface level, 10 = Deep/Advanced.
            4. "is_outdated": Boolean (true/false). True if the technology is dead (e.g., Flash, VB6, Old Editions).

            INPUT LIST:
            {batch_input}

            OUTPUT FORMAT (Strict JSON):
            {{
                "ID_STRING_HERE": {{
                    "summary": "Classic text but uses obsolete ES5 syntax; prefer the 7th edition.",
                    "tags": ["JavaScript", "Web Dev", "Legacy Code"],
                    "scores": {{ "relevance": 3, "readability": 7, "depth": 8 }},
                    "is_outdated": true
                }}
            }}
            """
            
            # Model Selection Loop
            result = None
            for model_name in PREFERRED_MODELS:
                try:
                    model = genai.GenerativeModel(model_name)
                    resp = model.generate_content(prompt)
                    
                    # Robust JSON Cleaning
                    text = resp.text.replace("```json", "").replace("```", "").strip()
                    if "{" in text: text = text[text.find("{"):text.rfind("}")+1]
                    
                    result = json.loads(text)
                    break 
                except Exception as e:
                    if "429" in str(e): raise e
                    continue
            
            if result:
                key_manager.report_success(api_key)
                
                # Validation & Format for saving
                clean_result = {}
                for fp in batch:
                    if fp in result:
                        clean_result[fp] = result[fp]
                    else:
                        # Fallback for missing AI response
                        clean_result[fp] = {"summary": "Processing skipped", "tags": [], "scores": {}, "is_outdated": False}
                
                data_manager.submit_result(clean_result)
                return
            else:
                raise Exception("Models failed to generate valid JSON")

        except Exception as e:
            if "429" in str(e): key_manager.report_failure(api_key, "Quota")
            elif "400" in str(e): key_manager.report_failure(api_key, "Invalid")
            else: logging.error(f"Worker Error: {e}")
            time.sleep(random.uniform(2, 5))