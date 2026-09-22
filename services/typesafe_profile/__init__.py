"""Profil usługi z badania TypeSafe — źródło weta taksonomii w wycenie raportu.

* pytania.py    — bateria pytań (Score / Noul / Choice) i zamiana odpowiedzi na liczby,
* listy.py      — okolice ciała i techniki per branża,
* weto.py       — reguły sprzeczności na liczbach (bez zależności od SDK),
* destylacja.py — destylacja na żądanie przy wycenie + trwała pamięć w service_profile.

Włącza TAXONOMY_VETO_SOURCE=typesafe (config.py). Pomiary i decyzja: bd BEAUTY_AUDIT-8295.
Narzędzia pomiarowe (holdout, świeża partia, test w cieniu): scripts/typesafe/.
"""
