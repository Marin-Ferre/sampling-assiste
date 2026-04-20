"""
Monitor temps réel du Community Enricher.
Lit le fichier data/enricher_stats.json mis à jour par community_enricher.py.

Usage : python monitor_enricher.py
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

STATS_FILE = Path(__file__).parent / "data" / "enricher_stats.json"
RATE_LIMIT_MAX = 60  # req/min Discogs (authentifie)

console = Console()

# Historique pour calculer req/min sur la dernière minute glissante
_req_history: list[tuple[float, int]] = []  # (timestamp, total_requests)


def load_stats() -> dict | None:
    try:
        return json.loads(STATS_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def compute_rpm(total_requests: int) -> float:
    now = time.time()
    _req_history.append((now, total_requests))
    cutoff = now - 60
    while _req_history and _req_history[0][0] < cutoff:
        _req_history.pop(0)
    if len(_req_history) < 2:
        return 0.0
    oldest_t, oldest_req = _req_history[0]
    elapsed = now - oldest_t
    return (total_requests - oldest_req) / elapsed * 60 if elapsed > 0 else 0.0


def format_duration(seconds: float) -> str:
    td = timedelta(seconds=int(seconds))
    h, rem = divmod(td.seconds, 3600)
    m, s = divmod(rem, 60)
    if td.days:
        return f"{td.days}j {h:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}:{s:02d}"


def eta(done: int, total: int, rpm: float) -> str:
    remaining = total - done
    if rpm <= 0 or remaining <= 0:
        return "—"
    minutes = remaining / rpm
    return format_duration(minutes * 60)


def build_display(stats: dict, rpm: float) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="rate"),
        Layout(name="progress"),
        Layout(name="errors", size=5),
        Layout(name="footer", size=3),
    )

    # Header
    now_str = datetime.now().strftime("%H:%M:%S")
    layout["header"].update(Panel(
        Text(f"  Community Enricher — Monitor  [{now_str}]", style="bold white on dark_magenta"),
        box=box.SIMPLE,
    ))

    # Rate limit
    layout["rate"].split_row(Layout(name="rate_left"), Layout(name="rate_right"))

    remaining = stats.get("rate_limit_remaining", RATE_LIMIT_MAX)
    limit_total = stats.get("rate_limit_total", RATE_LIMIT_MAX)
    used = limit_total - remaining
    bar_len = 40
    danger = remaining < 5
    filled = int(bar_len * used / limit_total) if limit_total else 0
    bar_color = "red" if danger else ("yellow" if remaining < 15 else "green")
    bar = f"[{bar_color}]{'█' * filled}[/][dim]{'░' * (bar_len - filled)}[/]"

    rate_table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2))
    rate_table.add_column("Metrique", style="cyan", width=28)
    rate_table.add_column("Valeur", style="bold", justify="right")

    rate_table.add_row("Requetes cette fenetre (60s)", f"[{bar_color}]{used} / {limit_total}[/]")
    rate_table.add_row("Jauge", bar)
    rate_table.add_row("Requetes/min (glissant)", f"[bold yellow]{rpm:.1f}[/]  [dim](limite : {RATE_LIMIT_MAX})[/]")
    rate_table.add_row("Total requetes session", f"{stats.get('total_requests', 0):,}")
    if stats.get("pauses"):
        rate_table.add_row("Pauses rate-limit declenchees", f"[red]{stats['pauses']}[/]")

    layout["rate_left"].update(Panel(rate_table, title="[bold]Rate Limit API Discogs", border_style="green" if not danger else "red"))

    # Vitesse
    started_at = stats.get("started_at")
    elapsed_str = format_duration(time.time() - started_at) if started_at else "—"
    last_req_at = stats.get("last_request_at")
    last_req_str = datetime.fromtimestamp(last_req_at).strftime("%H:%M:%S") if last_req_at else "—"

    speed_table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2))
    speed_table.add_column("Metrique", style="cyan", width=28)
    speed_table.add_column("Valeur", style="bold yellow", justify="right")

    enrich_rpm = stats.get("enriched_session", 0) / ((time.time() - started_at) / 60) if started_at and (time.time() - started_at) > 0 else 0
    speed_table.add_row("Duree session", elapsed_str)
    speed_table.add_row("Derniere requete", last_req_str)
    speed_table.add_row("Releases/min (session)", f"{enrich_rpm:.1f}")
    speed_table.add_row("ETA (toutes releases)", eta(
        stats.get("enriched_total", 0),
        stats.get("releases_total", 0),
        enrich_rpm,
    ))

    layout["rate_right"].update(Panel(speed_table, title="[bold]Vitesse", border_style="blue"))

    # Progression
    enriched_total = stats.get("enriched_total", 0)
    releases_total = stats.get("releases_total", 0)
    enriched_session = stats.get("enriched_session", 0)
    pct = enriched_total / releases_total * 100 if releases_total else 0
    bar2_filled = int(bar_len * pct / 100)
    bar2 = f"[magenta]{'█' * bar2_filled}[/][dim]{'░' * (bar_len - bar2_filled)}[/]"

    prog_table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2))
    prog_table.add_column("Metrique", style="cyan", width=28)
    prog_table.add_column("Valeur", style="bold magenta", justify="right")

    prog_table.add_row("Enrichies au total", f"{enriched_total:,} / {releases_total:,}  ({pct:.2f}%)")
    prog_table.add_row("Progression", bar2)
    prog_table.add_row("Enrichies cette session", f"{enriched_session:,}")

    last_title = stats.get("last_title") or "—"
    last_artist = stats.get("last_artist") or "—"
    prog_table.add_row("Derniere enrichie", f"{last_artist} — {last_title}")

    layout["progress"].update(Panel(prog_table, title="[bold]Progression Enrichissement", border_style="magenta"))

    # Erreurs
    err_404 = stats.get("errors_404", 0)
    err_other = stats.get("errors_other", 0)
    total_req = stats.get("total_requests", 1) or 1
    err_rate = (err_404 + err_other) / total_req * 100

    err_table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2))
    err_table.add_column("Metrique", style="cyan", width=28)
    err_table.add_column("Valeur", justify="right")

    err_table.add_row("Erreurs 404 (release inconnue)", f"[yellow]{err_404:,}[/]")
    err_table.add_row("Autres erreurs", f"[red]{err_other:,}[/]")
    err_table.add_row("Taux d'erreur", f"{'[red]' if err_rate > 5 else '[dim]'}{err_rate:.1f}%[/]")

    layout["errors"].update(Panel(err_table, title="[bold]Erreurs", border_style="yellow"))

    # Footer
    if stats.get("finished_at"):
        footer_text = Text("  Enrichissement termine.", style="bold green")
    elif not last_req_at:
        footer_text = Text("  En attente du demarrage de community_enricher.py...", style="italic dim")
    else:
        footer_text = Text(
            f"  Enricher actif — {enriched_session:,} releases traitees cette session",
            style="italic dim",
        )
    layout["footer"].update(Panel(footer_text, box=box.SIMPLE))

    return layout


def run() -> None:
    console.print("[bold green]Monitor Enricher demarre — Ctrl+C pour quitter[/]")
    console.print(f"[dim]Lecture de : {STATS_FILE}[/]")

    with Live(console=console, refresh_per_second=1, screen=True) as live:
        while True:
            try:
                stats = load_stats()
                if stats is None:
                    live.update(Panel(
                        Text("En attente du fichier de stats (data/enricher_stats.json)...\n"
                             "Lance community_enricher.py dans un autre terminal.", style="italic dim"),
                        title="Community Enricher Monitor",
                        border_style="yellow",
                    ))
                else:
                    rpm = compute_rpm(stats.get("total_requests", 0))
                    live.update(build_display(stats, rpm))

                time.sleep(2)

            except KeyboardInterrupt:
                break

    console.print("[bold yellow]Monitor arrete.[/]")


if __name__ == "__main__":
    run()
