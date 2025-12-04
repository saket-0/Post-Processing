import hashlib
import re
from dataclasses import dataclass, field
from datetime import date

def parse_library_date(date_str):
    """
    Parses various date formats from library data into a datetime.date object.
    Used to find the oldest date among duplicates to give the AI the best context.
    """
    if not date_str: return None
    s = str(date_str).strip().upper().replace('N/A', '')
    if s in ['NONE', 'NAN', 'NULL', 'UNKNOWN', '', '0', 'YYYY', 'MMDD']: return None
    
    # ISO Year (YYYY)
    if re.search(r'^(19|20)\d{2}$', s): 
        return date(int(s[:4]), 1, 1)
    
    # Compact YYMMDD (MARC standard often found in 008 fields)
    if len(s) == 6 and s.isdigit():
        try:
            yy, mm, dd = int(s[:2]), int(s[2:4]), int(s[4:])
            year = 1900 + yy if yy > 50 else 2000 + yy
            return date(year, mm, dd)
        except: pass

    # Slash formats (DD/MM/YYYY or MM/DD/YYYY)
    match = re.match(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})', s)
    if match:
        try:
            p1, p2, p3 = map(int, match.groups())
            # heuristic: if p3 is year
            y = p3
            if y < 100: y = 1900 + y if y > 50 else 2000 + y
            
            # Simple swap check: if p2 is obviously not a month (13+), swap
            d, m = p1, p2
            if m > 12 and d <= 12: d, m = m, d
            
            return date(y, m, d)
        except: pass
    return None

@dataclass
class BookGroup:
    """
    Represents a group of identical physical books (collapsed by Title+Author).
    This is what we send to the AI.
    """
    # Grouping Keys
    title: str
    author: str
    co_author: str
    
    # Context Data (We pick the 'best' version from the duplicates)
    best_edition: str = ""
    oldest_date: date = None
    
    # Calculated
    content_hash: str = field(init=False)

    def __post_init__(self):
        self.content_hash = self._generate_hash()

    def _generate_hash(self) -> str:
        """
        Creates a deterministic, short ID based on the content.
        This ensures that even if you restart the script, the ID is the same.
        """
        # Normalizing text to ensure minor casing/spacing diffs don't break grouping
        raw = f"{str(self.title).strip().lower()}|{str(self.author).strip().lower()}|{str(self.co_author).strip().lower()}"
        # Return first 8 chars of MD5 - sufficient collision resistance for batches
        return hashlib.md5(raw.encode('utf-8')).hexdigest()[:8]

    def get_prompt_line(self) -> str:
        """
        Formats the line for the AI input using the Hash ID.
        """
        # Resolve Provenance Context
        date_context = ""
        if self.best_edition and len(self.best_edition) > 1:
            date_context = f"(Ed. {self.best_edition})"
        elif self.oldest_date:
            date_context = f"({self.oldest_date.strftime('%Y')})"

        clean_title = str(self.title).replace('"', '').replace("'", "").strip()
        clean_author = str(self.author).replace('"', '').replace("'", "").strip()
        
        # ID: 'abc12345' -> Title, By Author (Context)
        return f"ID: '{self.content_hash}' -> {clean_title}, By {clean_author} {date_context}"