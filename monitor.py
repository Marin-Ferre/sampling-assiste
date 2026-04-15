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

        # Pages traitées par genre
        cur.execute("""
            SELECT genre, COUNT(*) as pages
            FROM raw.ingestion_checkpoints
            GROUP BY genre
            ORDER BY genre
        """)
        pages_by_genre = dict(cur.fetchall())

        # Dernière release ingérée
        cur.execute("""
            SELECT title, artist, year, ingested_at
            FROM raw.releases
            ORDER BY ingested_at DESC
            LIMIT 1
        """)
        last = cur.fetchone()

        # Distribution par genre
        cur.execute("""
            SELECT unnest(genres) as genre, COUNT(*) as cnt
            FROM raw.releases
            GROUP BY genre
            ORDER BY cnt DESC
            LIMIT 10
        """)
        genre_dist = cur.fetchall()

    return {
        "total": total,
        "last_minute": last_minute,
        "last_10s": last_10s,
        "pages_by_genre": pages_by_genre,
        "last_release": last,
        "genre_dist": genre_dist,
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
    api_rpm = rpm * 2
    metrics_table.add_row("Appels API estimés", f"~{api_rpm:.0f} req/min")
    layout["left"].update(Panel(metrics_table, title="[bold]Métriques", border_style="green"))

    # Progression par genre
    genre_table = Table(box=box.ROUNDED, padding=(0, 1))
    genre_table.add_column("Genre", style="cyan")
    genre_table.add_column("Pages traitées", justify="right", style="yellow")
    genre_table.add_column("Releases (est.)", justify="right", style="green")

    for genre in TARGET_GENRES:
        pages = stats["pages_by_genre"].get(genre, 0)
        est_releases = pages * 100
        genre_table.add_row(genre, str(pages), f"~{est_releases:,}")

    layout["right"].update(Panel(genre_table, title="[bold]Progression par genre", border_style="blue"))

    # Footer — dernière release
    if stats["last_release"]:
        title, artist, year, ingested_at = stats["last_release"]
        footer_text = Text(
            f"  Dernière release : {artist} — {title} ({year})  |  ingérée à {ingested_at.strftime('%H:%M:%S')}",
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
