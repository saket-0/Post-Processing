from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn
from rich.text import Text
from rich.align import Align
from datetime import datetime
import time

class Dashboard:
    def __init__(self, total_items):
        self.console = Console()
        self.start_time = time.time()
        self.total_items = total_items
        self.processed_count = 0
        
        # Log buffer
        self.logs = []
        
        # Performance metrics
        self.speed_history = []
    
    def get_layout(self, key_stats, active_workers_count):
        """Generates the full-screen layout on every refresh."""
        layout = Layout()
        
        # Split into main sections
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=7)
        )
        
        # Split Body into 3 Columns: Stats | Workforce | Logs
        layout["body"].split_row(
            Layout(name="left_stats", ratio=1),
            Layout(name="center_workers", ratio=2),
            Layout(name="right_logs", ratio=1)
        )
        
        # --- HEADER ---
        elapsed = int(time.time() - self.start_time)
        header_text = f"🚀 AUTONOMOUS HIVE MIND  |  ⏱️  Run Time: {elapsed}s  |  📚 Database: {self.total_items:,} Books"
        layout["header"].update(Panel(Align.center(header_text, style="bold white"), style="on blue"))
        
        # --- LEFT: LIVE STATS ---
        # Calculate speed
        elapsed_min = elapsed / 60 if elapsed > 0 else 1
        speed = int(self.processed_count / elapsed_min) if elapsed > 5 else 0
        
        stats_table = Table.grid(padding=1)
        stats_table.add_column(style="bold cyan", justify="right")
        stats_table.add_column(style="white")
        
        stats_table.add_row("⚡ Speed:", f"{speed} books/min")
        stats_table.add_row("📦 Processed:", f"{self.processed_count:,}")
        stats_table.add_row("⏳ Remaining:", f"{self.total_items - self.processed_count:,}")
        stats_table.add_row("👥 Active Threads:", f"{active_workers_count}")
        
        # Completion Estimate
        if speed > 0:
            mins_left = int((self.total_items - self.processed_count) / speed)
            eta = f"{mins_left} mins"
        else:
            eta = "Calculating..."
        stats_table.add_row("🏁 ETA:", eta)

        layout["left_stats"].update(Panel(stats_table, title="📊 Live Metrics", border_style="cyan"))
        
        # --- CENTER: WORKFORCE STATUS ---
        # Shows state of every API key
        key_table = Table(expand=True, box=None, show_header=True)
        key_table.add_column("API Key (Worker)", style="dim")
        key_table.add_column("Status")
        key_table.add_column("Requests", justify="right")
        key_table.add_column("Errors", justify="right")

        for k in key_stats:
            status_style = "green"
            icon = "🟢"
            if "Cooldown" in k['status']: 
                status_style = "yellow"
                icon = "🟡"
            if "Dead" in k['status']: 
                status_style = "red"
                icon = "💀"
                
            key_table.add_row(
                k['key'], 
                f"[{status_style}]{icon} {k['status']}[/]", 
                str(k['reqs']),
                f"[{'red' if k['errors']>0 else 'dim'}]{k['errors']}[/]"
            )
            
        layout["center_workers"].update(Panel(key_table, title="🛠️ API Key Workforce", border_style="green"))

        # --- RIGHT: LOGS ---
        log_text = Text()
        for ts, msg, style in self.logs[-15:]: # Show last 15 logs
            log_text.append(f"{ts} ", style="dim")
            log_text.append(msg + "\n", style=style)
            
        layout["right_logs"].update(Panel(log_text, title="📜 System Logs", border_style="yellow"))

        # --- FOOTER: PROGRESS ---
        prog = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=None, style="blue", complete_style="green"),
            TextColumn("{task.percentage:>3.0f}%"),
        )
        task_id = prog.add_task("Total Completion", total=self.total_items, completed=self.processed_count)
        layout["footer"].update(Panel(prog, title="Overall Progress", border_style="white"))
        
        return layout

    def log(self, message, style="white"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append((timestamp, message, style))
        if len(self.logs) > 50: self.logs.pop(0)

    def update_progress(self, count):
        self.processed_count = count