# Diagnoza matchingu (katalog usług + podpis) — skrypty robocze z 29–30.09

Przeniesione z katalogu tymczasowego sesji (30.09), żeby nie zginęły. Tylko odczyt danych z `scripts/katalog/dane/`; płatne tylko te z TypeSafe (proba_*) — przez bramkę `ZASADY_OK=1`. Uruchamiać z `bagent/`: `.venv/bin/python scripts/katalog/diagnoza/<skrypt>.py`.

- `bez_ceny.py` — Wiersze bez ceny „ta sama”: najbliżsi kandydaci z werdyktem i powodem (0 USD).
- `bilans7.py` — Bilans werdyktów sprawdzianu 7 po zmianie vs werdykty z próby (moje oceny).
- `bilans_kat.py` — Bilans: stare uzupełnianie z kategorii (HEAD) vs nowe — te same pary, kontekst, słownik, klasy.
- `diag_para.py` — Diagnoza pojedynczych par zbioru 1042 (tylko odczyt pamięci, 0 USD).
- `diff_frazy.py` — Werdykty sprawdzianu z poprawką źródeł fraz i bez niej — które ocenione pary się zmieniają (0 USD).
- `dwustronne.py` — Rozkład różnic dwustronnych na zbiorze 1042 par: wielkość różnicy vs moja ocena.
- `kat_dev.py` — Rozkład kategorii dla zbioru 1042 par (GLM z abonamentu, 0 USD).
- `limit_kandydatow.py` — Ile salonów w promieniu ma usługę o (prawie) tej samej nazwie — przy limicie 120 kandydatów vs bez limitu. Tylko odczyt (Supabase + baza wektorów), 0 USD.
- `opisowe_pomiar.py` — Czy „cecha opisowa” z planu (żaden salon nie sprzedaje obu wersji w różnych cenach) oddziela moje T od P/I na parach z różnicą jednostronną (zbiór 1042 par). Ry
- `podpis_stary.py` — Katalog usług — podpis oferty i porównanie dwóch podpisów (hybryda z planu zatwierdzonego 29.09). „Ta sama” porównuje ZBIÓR rdzeni słów oferty, nie przydział sł
- `pokaz7.py` — Wypisuje pary sprawdzianu 7 do mojej oceny (kawałkami), bez werdyktów metod — oceniam na ślepo.
- `proba_obu_stron.py` — Próba 40 nowych klas z różnic dwustronnych (zbiór 1042 par) — do obejrzenia przed pełnym przebiegiem.
- `proba_pozycji.py` — Próba pytania o pozycję kategorii na ≤ 40 kategoriach (do obejrzenia oczami) — wybór próby tylko tutaj.
- `prog_klas.py` — Reguła „nie zmienia”: poziom najbliższy średniej (dziś) vs P(„nie zmienia”) > p — na tych samych danych (0 USD).
- `rozklad_drzewa.py` — Za darmo (bez TypeSafe): z czego składa się koszt przejścia drzewa v13b na usługach zbioru 6. Odtwarza pytania trzech wywołań dokładnie tak, jak wiazka_v13 (poz
- `wazone6.py` — Zbiór 6: ważone szacunki trafności i liczby trafnych par dla v14f-w2 (v14), syn (v14p) i v13b — z moich ocen próbki. Grupy rozłączne w branży: S14 (v14 „ta sama
- `zgubione.py` — Zgubione pary T w wybranej branży: powód i obie oferty (podpis z kontekstem, słownikiem, klasami i zamianami).
