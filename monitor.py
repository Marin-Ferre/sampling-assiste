"""
Monitoring temps réel de l'ingestion Discogs.
Rafraîchissement toutes les 3 secondes.

Usage : python monitor.py
"""

import time
from datetime import datetime

import psycopg2
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text
from rich import box

from ingestion.config import PG_DSN, TARGET_GENRES

console = Console()

# Historique pour calculer les vitesses
_history: list[tuple[float, int]] = []  # (timestamp, total_rows)


def get_stats(conn) -> dict:
    with conn.cursor() as cur:
        # Total lignes
        cur.execute("SELECT COUNT(*) FROM raw.releases")
        total = cur.fetchone()[0]

        # Lignes ajoutées dans la dernière minute
        cur.execute("""
            SELECT COUNT(*) FROM raw.releases
            WHERE ingested_at >= NOW() - INTERVAL '1 minute'
        """)
        last_minute = cur.fetchone()[0]

        # Lignes ajoutées dans les 10 dernières secondes
        cur.execute("""
            SELECT COUNT(*) FROM raw.releases
            WHERE ingested_at >= NOW() - INTERVAL '10 seconds'
        """)
        last_10s = cur.fetchone()[0]

        # Releases par genre cible
        cur.execute("""
            SELECT genre, COUNT(*) as cnt
            FROM raw.releases, unnest(genres) as genre
            WHERE genre = ANY(%s)
            GROUP BY genre
            ORDER BY genre
        """, (TARGET_GENRES,))
        pages_by_genre = dict(cur.fetchall())

        # Dernière release ingérée
        cur.execute("""
            SELECT title, artist, year, ingested_at
            FROM raw.releases
            ORDER BY ingested_at DESC
            LIMIT 1
        """)
        last = cur.fetchone()

        # Enrichissement community
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE community_scraped_at IS NOT NULL) as enrichies,
                COUNT(*) as total_enrichissable
            FROM raw.releases
        """)
        enrich_done, enrich_total = cur.fetchone()

        # Dernière release enrichie
        cur.execute("""
            SELECT title, artist, community_have, community_want, community_scraped_at
            FROM raw.releases
            WHERE community_scraped_at IS NOT NULL
            ORDER BY community_scraped_at DESC
            LIMIT 1
        """)
        last_enriched = cur.fetchone()

    return {
        "total": total,
        "last_minute": last_minute,
        "last_10s": last_10s,
        "pages_by_genre": pages_by_genre,
        "last_release": last,
        "enrich_done": enrich_done,
        "enrich_total": enrich_total,
        "last_enriched": last_enriched,
    }


def rows_per_minute(total: int) -> float:
    now = time.time()
    _history.append((now, total))
    # On garde 2 minutes d'historique
    cutoff = now - 120
    while _history and _history[0][0] < cutoff:
        _history.pop(0)
    if len(_history) < 2:
        return 0.0
    oldest_t, oldest_rows = _history[0]
    elapsed = now - oldest_t
    if elapsed == 0:
        return 0.0
    return (total - oldest_rows) / elapsed * 60


def build_display(stats: dict, rpm: float) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main"),
        Layout(name="enrich", size=7),
        Layout(name="footer", size=3),
    )
    layout["main"].split_row(
        Layout(name="left"),
        Layout(name="right"),
    )

    # Header
    now_str = datetime.now().strftime("%H:%M:%S")
    header_text = Text(f"  Sampling Assisté — Monitoring Ingestion Discogs  [{now_str}]", style="bold white on blue")
    layout["header"].update(Panel(header_text, box=box.SIMPLE))

    # Métriques principales
    metrics_table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2))
    metrics_table.add_column("Métrique", style="cyan", width=30)
    metrics_table.add_column("Valeur", style="bold yellow", justify="right")

    metrics_table.add_row("Total releases ingérées", f"{stats['total']:,}")
    metrics_table.add_row("Ajoutées (dernière minute)", f"{stats['last_minute']:,}")
    metrics_table.add_row("Ajoutées (10 dernières sec)", f"{stats['last_10s']:,}")
    metrics_table.add_row("Vitesse", f"{rpm:.1f} lignes/min")
    # Estimation appels API : ~1 appel search + 1 appel detail par release
    # 1 appel search pour 100 releases + 1 appel detail par release = ~1.01 appels/release
    api_rpm = rpm * 1.01
    metrics_table.add_row("Appels API estimés", f"~{api_rpm:.0f} req/min  (limite : 60)")
    layout["left"].update(Panel(metrics_table, title="[bold]Métriques", border_style="green"))

    # Progression par genre
    genre_table = Table(box=box.ROUNDED, padding=(0, 1))
    genre_table.add_column("Genre", style="cyan")
    genre_table.add_column("Releases", justify="right", style="green")

    for genre in TARGET_GENRES:
        count = stats["pages_by_genre"].get(genre, 0)
        genre_table.add_row(genre, f"{count:,}")

    layout["right"].update(Panel(genre_table, title="[bold]Progression par genre", border_style="blue"))

    # Panneau enrichissement community
    enrich_done = stats["enrich_done"]
    enrich_total = stats["enrich_total"]
    pct = enrich_done / enrich_total * 100 if enrich_total else 0
    bar_len = 40
    filled = int(bar_len * pct / 100)
    bar = "█" * filled + "░" * (bar_len - filled)

    enrich_table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2))
    enrich_table.add_column("Métrique", style="cyan", width=30)
    enrich_table.add_column("Valeur", style="bold magenta", justify="right")
    enrich_table.add_row("Enrichies (community)", f"{enrich_done:,} / {enrich_total:,}  ({pct:.1f}%)")
    enrich_table.add_row("Progression", f"[magenta]{bar}[/]")

    if stats["last_enriched"]:
        title, artist, have, want, scraped_at = stats["last_enriched"]
        local = scraped_at.astimezone().strftime('%H:%M:%S')
        enrich_table.add_row("Dernière enrichie", f"{artist} — {title}  |  have={have} want={want}  |  {local}")

    layout["enrich"].update(Panel(enrich_table, title="[bold]Enrichissement Community", border_style="magenta"))

    # Footer — dernière release
    if stats["last_release"]:
        title, artist, year, ingested_at = stats["last_release"]
        local_time = ingested_at.astimezone().strftime('%H:%M:%S')
        footer_text = Text(
            f"  Dernière release : {artist} — {title} ({year})  |  ingérée à {local_time}",
            style="italic dim"
        )
    else:
        footer_text = Text("  En attente de données...", style="italic dim")
    layout["footer"].update(Panel(footer_text, box=box.SIMPLE))

    return layout


def run():
    conn = psycopg2.connect(PG_DSN)
    console.print("[bold green]Monitoring démarré — Ctrl+C pour quitter[/]")

    with Live(console=console, refresh_per_second=1, screen=True) as live:
        while True:
            try:
                stats = get_stats(conn)
                rpm = rows_per_minute(stats["total"])
                live.update(build_display(stats, rpm))
                time.sleep(3)
            except psycopg2.OperationalError:
                conn = psycopg2.connect(PG_DSN)
            except KeyboardInterrupt:
                break

    conn.close()
    console.print("[bold yellow]Monitoring arrêté.[/]")


if __name__ == "__main__":
    run()
