# Narzędzia pomiarowe: destylacja przez TypeSafe (bd BEAUTY_AUDIT-8295)

Kod produkcyjny żyje w `services/typesafe_profile/`. Tutaj są narzędzia, którymi go zmierzono.
Wszystkie tylko czytają z produkcji, nie wołają GLM i nie zapisują do `service_taxonomy`.
Klucz TypeSafe: `TYPESAFE_API_KEY` albo `~/.config/typesafe/api_key` (nigdy w repo).

| plik | co robi |
|---|---|
| `destylacja_badanie.py` | profile dla listy nazw (`--z-pliku`, `--mapa`), zapis do `profile_<przebieg>.jsonl` |
| `holdout_weto.py` | weto GLM na holdoucie `matching_holdout` (mig 188) — punkt odniesienia |
| `porownaj_weto.py` | weto profili vs weto GLM na tych samych parach |
| `nowa_partia.py` | świeża partia 320 par z 8 branż (`search_twins` jak w wycenie, ziarno 20260921) |
| `waliduj.py` | wynik weta na partii z etykietami sędziów |
| `nazwy_salonow.py`, `mapa_pytan.py` | mapa pytań per branża → `services/typesafe_profile/mapa_pytan.json` |
| `symulacja_mapy.py` | czy przycięcie pytań mapą zmienia decyzje weta (bez wywołań modelu) |
| `cien_raporty.py` | test w cieniu: prawdziwy silnik wyceny z wetem GLM vs z wetem profili, twardy limit USD |

`badanie_uslugi.py`, `weto_profil.py`, `listy_kanoniczne.py` to zwrotnice do modułu produkcyjnego,
żeby skrypty działały bez zmian.

## Pomiar trafności matchingu (bd BEAUTY_AUDIT-asrk, 25–26.09)

Dane i wnioski: `dane/2026-09-25/README.md`. Wszystkie narzędzia tylko czytają produkcję;
zapisują wyłącznie oceny par (`matching_pair_verdict`, mig 200) i profile (`service_profile`).

| plik | co robi |
|---|---|
| `kalibracja_sedziego.py` | sędzia par (`services/typesafe_ocena`) na holdoucie mig 188 vs etykiety człowieka i modeli |
| `warianty_sedziego.py` | porównanie wariantów pytań sędziego (podział parzyste/nieparzyste id); v8 → WERSJA 2 |
| `ocena_trafnosci.py` | silnik wyceny na sucho dla losowych salonów (po 2 z 9 branż) + sędzia na każdej próbce ceny |
| `rozrzut_cen.py` | rozrzut ceny za minutę w grupach „ta sama” — miara odrzucona (nie mierzy tożsamości) |
| `porownanie_na_profilach.py` | profile baterii TypeSafe jako sędzia — wynik negatywny (detektor sprzeczności, nie tożsamości) |
| `drzewo_sprawdzian.py` | ręczne drzewo zabiegów (`services/typesafe_drzewo/drzewo.py`) vs sędzia — punkt odniesienia |
| `slownik_osi.py` | słownik cech z destylacji GLM: wartość → cecha (krok 1), synonimy i rodzice (krok 2) |
| `schemat_osi.py` | schemat z danych: branża → rodzaje, rodzaj → cechy → wartości, kandydaci wartości domyślnych |
| `potwierdz_domyslne.py` | wartość domyślna tylko, gdy TypeSafe potwierdzi, że wynika z nazwy zabiegu |
| `schemat_pomiar.py` | destylacja wg schematu (`services/typesafe_drzewo/schemat.py`) + pokrycie i rozstrzygalność |
| `rewizja_trafnosci.py` | v7 vs sędzia na tych samych parach, per decyzja silnika; tabela krzyżowa na salonach |
