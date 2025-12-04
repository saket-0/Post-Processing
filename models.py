import hashlib
import re
from dataclasses import dataclass, field
from datetime import date

def parse_library_date(date_str):
    """(Same date parser as before)"""
    if not date_str: return None
    s = str(date_str).strip().upper().replace('N/A', '')
    if s in ['NONE', 'NAN', 'NULL', 'UNKNOWN', '', '0', 'YYYY', 'MMDD']: return None
    
    if re.search(r'^(19|20)\d{2}$', s): 
        return date(int(s[:4]), 1, 1)
    
    if len(s) == 6 and s.isdigit():
        try:
            yy, mm, dd = int(s[:2]), int(s[2:4]), int(s[4:])
            year = 1900 + yy if yy > 50 else 2000 + yy
            return date(year, mm, dd)
        except: pass

    match = re.match(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})', s)
    if match:
        try:
            p1, p2, p3 = map(int, match.groups())
            y = p3
            if y < 100: y = 1900 + y if y > 50 else 2000 + y
            d, m = p1, p2
            if m > 12 and d <= 12: d, m = m, d
            return date(y, m, d)
        except: pass
    return None

@dataclass
class BookGroup:
    # Identity
    title: str
    author: str
    co_author: str
    
    # Context (Best available from duplicates)
    best_edition: str = ""
    oldest_date: date = None
    dewey: str = ""       # <--- NEW
    subjects: str = ""    # <--- NEW
    
    content_hash: str = field(init=False)

    def __post_init__(self):
        self.content_hash = self._generate_hash()

    def _generate_hash(self) -> str:
        # Hashing only Title/Author/CoAuthor ensures grouping stays strict
        raw = f"{str(self.title).strip().lower()}|{str(self.author).strip().lower()}|{str(self.co_author).strip().lower()}"
        return hashlib.md5(raw.encode('utf-8')).hexdigest()[:8]

    def get_prompt_line(self) -> str:
        """
        Includes Dewey and Subject in the AI prompt line.
        """
        # 1. Date Context
        date_context = ""
        if self.best_edition and len(self.best_edition) > 1:
            date_context = f"(Ed. {self.best_edition})"
        elif self.oldest_date:
            date_context = f"({self.oldest_date.strftime('%Y')})"

        # 2. Domain Context (Dewey & Subject)
        # We truncate subjects to 150 chars to save tokens while keeping key terms
        context_parts = []
        if self.dewey: 
            context_parts.append(f"DDC: {self.dewey}")
        if self.subjects:
            clean_subj = self.subjects.replace(' -- ', ', ').replace('--', ', ')
            if len(clean_subj) > 100: clean_subj = clean_subj[:100] + "..."
            context_parts.append(f"Subj: {clean_subj}")
            
        extra_context = f"[{' | '.join(context_parts)}]" if context_parts else ""

        clean_title = str(self.title).replace('"', '').replace("'", "").strip()
        clean_author = str(self.author).replace('"', '').replace("'", "").strip()
        
        # New Format: 
        # ID: 'hash' -> Title, By Author (Date) [DDC: 620 | Subj: Engineering, Mechanics]
        return f"ID: '{self.content_hash}' -> {clean_title}, By {clean_author} {date_context} {extra_context}"