# Pomiar trafności matchingu — dane z 25–26.09.2026 (bd BEAUTY_AUDIT-asrk)

Wyniki, za które zapłacono TypeSafe (~10 USD łącznie), przeniesione z katalogu
tymczasowego sesji. Skrypty w `scripts/typesafe/` czytają te pliki wprost —
powtórny przebieg na tych samych usługach nic nie kosztuje.

## Pliki

| plik | skąd | koszt | co zawiera |
|---|---|---|---|
| `slownik_surowy.json` | `slownik_osi.py` krok 0 | 0 | 2 497 wartości cech ponad progiem z destylacji GLM (334 tys. nazw) |
| `krok1_cechy.json` | `slownik_osi.py` krok 1 | 0,21 USD | każda wartość → cecha usługi (wybór TypeSafe z przykładami nazw) |
| `krok2_kotwice.json` | `slownik_osi.py` krok 2 + domknięcie | 0,78 USD | synonimy (kotwice) i rodzic rodzaju zabiegu |
| `slownik.json` | `slownik_osi.py` | 0 | słownik kanoniczny: cecha → wartość → synonimy, liczność, rodzic |
| `schemat.json` | `schemat_osi.py` + `potwierdz_domyslne.py` | 0,01 USD | branża → rodzaje; rodzaj → cechy → wartości; wartości domyślne potwierdzone TypeSafe; rodzice |
| `destylacje_v7.json` | `schemat_pomiar.py` (v5→v7) | ~2,7 USD | 9 170 usług z 18 salonów + holdoutu: rodzaj, pewność, cechy, liczby |
| `schemat_raport_v7.json` | `schemat_pomiar.py` | 0 | pokrycie taksonomii i werdykty (wersja v7) |
| `ocena_trafnosci_wiersze.json` | `ocena_trafnosci.py` | 0,28 USD | 606 wierszy wyceny z 18 salonów (silnik na sucho) z pełnymi próbkami i werdyktem sędziego par v2 |
| `ocena_trafnosci.json` | `ocena_trafnosci.py` | — | podsumowanie tego przebiegu |
| `kalibracja_pary_sedzia_v2.json`, `kalibracja_sedzia_v2.json` | `kalibracja_sedziego.py` | 0,01 USD | sędzia par v2 na 563 parach holdoutu (mig 188) |
| `rewizja_2026-09-26.json`, `rewizja_holdout_2026-09-26.json` | `rewizja_trafnosci.py` | 0 | porównanie v7 i sędziego na TYCH SAMYCH parach (patrz niżej) |

Oceny par sędziego (v1 i v2, 26 286) są też w tabeli `matching_pair_verdict` (mig 200),
profile baterii TypeSafe (5 216) w `service_profile` (mig 198).

Nie przeniesiono (podejścia porzucone, liczby w bd asrk): klasyfikacje ręcznego drzewa
(`services/typesafe_drzewo/drzewo.py`, v1–v4), wyniki wariantów pytań sędziego, profile jako sędzia.

Wejście `schemat_osi.py`/`slownik_osi.py` (zrzut `service_taxonomy`, 5,3 MB) nie jest
w repo — odtworzenie (stan bazy się zmienia, więc wynik może się nieznacznie różnić):

```bash
ssh root@tytan "docker exec ba-supabase-db psql -U postgres -d postgres -Atc \"COPY (SELECT branza, name_key, osie FROM service_taxonomy WHERE branza <> '_legacy') TO STDOUT WITH (FORMAT csv)\"" | gzip > taksonomia.csv.gz
```

## Co z tych danych wynika (rewizja 26.09)

- W próbkach ceny 39% to ta sama usługa, ~50% warianty, 10–15% inne usługi — zgodnie
  sędzia par na 18 salonach (39/51/10) i etykiety modeli na parach przyjętych przez silnik (39/46/15).
- Trafność na holdoucie jest zawyżona: 45% jego par silnik już odrzuca. Porównuj metody na
  `decyzja_silnika='przyjety'` i zawsze na tych samych parach. Tam: sędzia v2 87,9%
  (gdy mówi „ta sama”, ma rację w 90%), taksonomia v7 81,0% (83%).
- Przesunięcie ceny „~4%” policzone tylko na 44% wierszy (tam 55% próbek tożsamych);
  w pozostałych 56% tożsame to 10% próbek — wpływ na cenę niezmierzony.
- Słabość v7: automatyczna lista rodzajów nie jest podziałem (synonimy, ogólne obok
  szczegółowych, zestawy z „i”), więc ta sama nazwa dostaje różny rodzaj w dwóch salonach.
