# typesafe_drzewo — klasyfikacja usług do porównania „ta sama / powiązana / inna”

Moduł badawczy (bd BEAUTY_AUDIT-asrk). **Silnik wyceny go nie używa.**

| plik | status | co robi |
|---|---|---|
| `osie.py` | aktualny | wspólny dla wszystkich branż katalog cech usługi (rodzaj, technika, preparat, obszar, …) |
| `schemat.py` | aktualny (v7) | destylacja usługi według schematu z danych (`scripts/typesafe/schemat_osi.py`) i porównanie „wszystkie cechy równe” |
| `drzewo.py`, `klasyfikacja.py`, `porownanie.py` | porzucony punkt odniesienia | ręcznie napisane drzewo 18 rodzin / 116 rodzajów (25.09); łamało zasadę uniwersalności (wiedza wpisana przez człowieka), zostaje do porównań w `scripts/typesafe/drzewo_sprawdzian.py` |

Wyniki i znane słabości: `scripts/typesafe/dane/2026-09-25/README.md`.
Najważniejsza: lista rodzajów w `schemat.json` musi być podziałem (opcje wyboru
wzajemnie się wykluczające), inaczej ta sama nazwa dostaje różny rodzaj.
