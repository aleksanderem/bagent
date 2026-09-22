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
