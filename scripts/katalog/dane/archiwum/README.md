# Archiwum źródeł ocen matchingu (30.09.2026)

`zrodla_ocen_2026-09-28.tar.xz` — usługi i pary kandydatów zbiorów ocenionych przeze mnie ręcznie (1042 pary:
`v13_sprawdzian`, `v14_sprawdzian` … `v14_sprawdzian6`; plus `v14_sprawdzian7/pary.json` — werdykty v14f na
sprawdzianie 7). Oceny (`ocena_claude.json`) są w repo; bez tych plików nie da się ich ponownie policzyć.
Telefony i e-maile zamaskowane (`scripts/katalog/maskuj_kontakty.py`) — klucze kategorii liczone są z tekstu bez
kontaktów, więc wyniki są te same co na danych sprzed maskowania.

Odtworzenie (z katalogu `bagent/`, nie nadpisuje istniejących plików):

    tar -xJkf scripts/katalog/dane/archiwum/zrodla_ocen_2026-09-28.tar.xz

Sprawdzian 7–9 (katalog usług) trzyma dane wprost w `scripts/katalog/dane/2026-09-29/sprawdzian*/`.
