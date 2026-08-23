"""Kazdy ping healthchecku uzywany w kodzie musi byc opisany w .env.example.

DLACZEGO TEN TEST ISTNIEJE (BEAUTY_AUDIT-27gn, zmierzone 2026-08-23):
``services.healthcheck.ping`` czyta ``os.environ`` i przy braku zmiennej jest
CICHYM no-op. Dodanie pingu do crona wyglada wiec jak wlaczenie alertowania,
a bez wpisu w srodowisku na tytanie nie zmienia niczego — cisza po awarii jest
nieodrozninalna od sukcesu.

Pomiar na produkcji tego dnia: kod uzywal 19 slugow, produkcyjny .env mial 15,
a .env.example dokumentowal 3. Cztery pingi byly martwe, w tym
HC_PING_OUTREACH_PRICE_EVENTS dodany tego samego ranka wlasnie po to, zeby
nocna detekcja zmian cen meldowala awarie.

Test nie siega na produkcje (nie moze). Pilnuje jedynego progu, ktory da sie
egzekwowac z repo: nowy ping ma trafic do .env.example razem z kodem, zeby
wdrozenie na tytanie mialo skad wziac liste zmiennych do dopiecia.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Katalogi z kodem produkcyjnym. scripts/ i tests/ pominiete swiadomie —
# jednorazowe narzedzia nie musza mieć wlasnych checkow.
CODE_DIRS = ("workers", "services", "pipelines")

# Wystapienia, ktore nie sa prawdziwym slugiem:
#   HC_PING_FOO  — przyklad w docstringu services/healthcheck.py
#   HC_PING_SLO_ — goly prefiks opisujacy konwencje nazw w slo_probes.py
PLACEHOLDERS = frozenset({"HC_PING_FOO", "HC_PING_SLO_"})

SLUG_PATTERN = re.compile(r"HC_PING_[A-Z0-9_]+")


def _slugs_used_in_code() -> dict[str, set[str]]:
    """Slug -> zbior plikow, ktore go uzywaja."""
    found: dict[str, set[str]] = {}
    for directory in CODE_DIRS:
        for path in (REPO_ROOT / directory).rglob("*.py"):
            for slug in SLUG_PATTERN.findall(path.read_text(encoding="utf-8")):
                if slug in PLACEHOLDERS:
                    continue
                found.setdefault(slug, set()).add(
                    str(path.relative_to(REPO_ROOT))
                )
    return found


def _slugs_documented() -> set[str]:
    text = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    return set(SLUG_PATTERN.findall(text)) - PLACEHOLDERS


def test_every_used_ping_is_documented_in_env_example() -> None:
    used = _slugs_used_in_code()
    undocumented = sorted(set(used) - _slugs_documented())
    assert not undocumented, (
        "Ping bez wpisu w .env.example — na produkcji bedzie cichym no-op:\n"
        + "\n".join(f"  {slug}  ({', '.join(sorted(used[slug]))})" for slug in undocumented)
        + "\nDopisz zmienna do .env.example i dopnij ja na tytanie."
    )


def test_env_example_documents_no_dead_slugs() -> None:
    """Odwrotny kierunek: zmienna w .env.example, ktorej nikt juz nie pinguje.

    Zostawiona po usunieciu crona myli przy wdrozeniu — ktos zaklada check,
    ktory nigdy nie dostanie pinga, i dostaje falszywy alarm 'down'.
    """
    orphaned = sorted(_slugs_documented() - set(_slugs_used_in_code()))
    assert not orphaned, (
        "Zmienna w .env.example, ktorej zaden kod nie pinguje:\n"
        + "\n".join(f"  {slug}" for slug in orphaned)
        + "\nUsun wpis albo przywroc ping."
    )


def test_healthcheck_helper_reads_os_environ_not_settings() -> None:
    """Straznik zalozenia, na ktorym stoi caly ten plik.

    Gdyby ``ping`` przeszedl kiedys na ``config.settings``, zmienne z .env
    dzialalyby bez eksportu do srodowiska i ten test pilnowalby juz czegos
    innego, niz mysli. Wtedy trzeba go przepisac, a nie usunac.
    """
    source = (REPO_ROOT / "services" / "healthcheck.py").read_text(encoding="utf-8")
    assert "os.environ.get(env_var_name)" in source
