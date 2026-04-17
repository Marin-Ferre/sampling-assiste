"""
Client HTTP pour Neon — contourne le blocage du port 5432.
Utilise l'API HTTP de Neon (port 443) à la place de psycopg2.

Doc: https://neon.tech/docs/serverless/serverless-driver#use-the-neon-serverless-driver-over-http
"""

import base64
import os
from typing import Any

import httpx

NEON_DSN = os.environ.get("NEON_DSN", "")

# Extrait host, user, password, dbname depuis la DSN
def _parse_dsn(dsn: str) -> dict:
    # postgresql://user:password@host/dbname?...
    rest = dsn.replace("postgresql://", "")
    userpass, rest = rest.split("@", 1)
    user, password = userpass.split(":", 1)
    host, rest = rest.split("/", 1)
    dbname = rest.split("?")[0]
    return {"user": user, "password": password, "host": host, "dbname": dbname}


class NeonClient:
    """Client synchrone qui exécute du SQL sur Neon via HTTPS."""

    def __init__(self, dsn: str):
        parts = _parse_dsn(dsn)
        self.url = f"https://{parts['host']}/sql"
        self.headers = {
            "Content-Type": "application/json",
            "Neon-Connection-String": f"postgresql://{parts['user']}:{parts['password']}@{parts['host']}/{parts['dbname']}",
        }

    def execute(self, query: str, params: list = None) -> list[dict]:
        """Exécute une requête et retourne une liste de dicts."""
        payload: dict[str, Any] = {"query": query}
        if params:
            payload["params"] = [str(p) if not isinstance(p, (int, float, bool, type(None))) else p for p in params]

        with httpx.Client(timeout=30) as client:
            resp = client.post(self.url, json=payload, headers=self.headers)
            resp.raise_for_status()
            data = resp.json()

        fields = [f["name"] for f in data.get("fields", [])]
        return [dict(zip(fields, row)) for row in data.get("rows", [])]

    def execute_many(self, statements: list[dict]) -> None:
        """Exécute plusieurs requêtes en une seule transaction HTTP."""
        payload = {"queries": [
            {"query": s["query"], "params": s.get("params", [])}
            for s in statements
        ]}
        with httpx.Client(timeout=60) as client:
            resp = client.post(
                self.url.replace("/sql", "/transaction"),
                json=payload,
                headers=self.headers,
            )
            resp.raise_for_status()


def get_neon() -> NeonClient:
    return NeonClient(NEON_DSN)
