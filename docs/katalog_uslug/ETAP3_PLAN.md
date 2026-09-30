# Etap 3 — nowa metoda w raporcie za przełącznikiem (plan do akceptacji Alexa)

Stan wejściowy (30.09): sprawdzian 9 na zamrożonych regułach 95,9% trafnych par „ta sama” (bramka 95% ✓);
po poprawce przykładów 97,5–98,3% na zbiorach 7–9 i 1042 parach; porównanie „ta sama” w 36–69% wierszy
(z wierszami 1–2 salonów, decyzja Alexa 30.09). Dowód poprawki: sprawdzian 10 (nowe salony).

## Zasada
Klientka nie widzi żadnej zmiany, dopóki Alex nie przełączy. Każdy zapis na produkcji (migracja, wdrożenie,
przełączenie) dopiero po „tak” Alexa, po zobaczeniu wyniku poprzedniego kroku.

## Krok A — suchy przebieg na prawdziwych raportach (bez zmian na produkcji, bez zgody)
1. 5 ostatnich raportów konkurencji z produkcji (różne miasta i branże), tylko odczyt.
2. Te same usługi podmiotu i ta sama pula salonów co raport; kandydaci jak w sprawdzianach (wektory w promieniu
   15 km, podobieństwo ≥ 0,6, do 120 na usługę — dziś raport bierze ≥ 0,68 i 80).
3. Rozbiór ofert GLM z abonamentu (0 USD, ~1 h na raport), klasy różnic TypeSafe (grosze), wszystko lokalnie.
4. Wynik: udział wierszy z ceną „ta sama” (≥ 3 salony / 1–2 salony / tylko podobne / brak) obok starego silnika,
   zgodność median, 20 przykładów wierszy, w których metody się różnią. Oceniam sam, raport do Alexa.

## Krok B — wpięcie za przełącznikiem, domyślnie „stary” (wymaga „tak”)
1. Baza (migracja Supabase w BEAUTY_AUDIT): jedna nowa tabela pamięci katalogu — rodzaj (oferta / kategoria /
   salon / klasa / zamiana / słownik), klucz, wersja, wartość JSON, model, data. Tylko nowe wiersze, zero zmian
   w istniejących tabelach. Zasilenie z danych, które już są (sprawdziany, raport testowy, suchy przebieg).
2. bagent: w wycenie raportu nowa ścieżka za przełącznikiem `MATCHING_SOURCE` = stary | podpis. Oferta = usługa
   albo wariant z ceną; brak rozbioru oferty → dociągnięcie GLM w limicie (jak dzisiejszy pomost: 40 wywołań,
   75 s), reszta bez porównania; klasy i zamiany tylko z pamięci (w raporcie zero pytań TypeSafe, brak klasy =
   różnica istotna). Cena: ≥ 3 salony → mediana jak dziś; 1–2 salony → „ta sama usługa u N konkurentów” z ich
   cenami; tylko podobne → „ceny podobnych usług”. Struktura tabel raportu bez zmian (rodzaj wiersza w szczegółach).
3. Panel: przełącznik w „Klucze i stałe” + kontrolka w autodiagnostyce (raporty liczone podpisem, oferty bez
   rozbioru, dociągnięcia GLM, błędy doby, koszt USD).
4. Raport (frontend): wiersz „ta sama usługa u 1–2 konkurentów” — dziś takiego nie ma.
5. Wdrożenie: bagent (tytan), Convex, frontend — z przełącznikiem na „stary”, więc bez zmiany dla klientek.

## Krok C — przełączenie na „podpis” (wymaga „tak” po kroku B)
Pierwsze raporty po przełączeniu przeglądam ręcznie; powrót = przełącznik na „stary”, bez wdrożenia.

## Krok D (etap 4 planu) — rozbiór ofert w tle, najpierw regiony z raportami
Osobny plan z rachunkiem kosztu dobowego (bramka kosztów automatyzacji): GLM z abonamentu, limit dobowy,
zatrzymanie na pierwszym błędzie limitu. Limit planu Pro (30.09): 12 000 kredytów / 5 h, 60 000 / tydzień; paczka
12 ofert ≈ 2 kredyty (zmierzone) → do ~360 tys. ofert na tydzień przy całym limicie. Cały rynek (~1–1,5 mln ofert)
to 3–4 tygodnie pełnego limitu, więc NIE cały rynek naraz: regiony, w których robimy raporty, plus dociąganie pul
konkretnych raportów (zimny raport jak 279: ~15 tys. ofert ≈ 2 600 kredytów ≈ 4% tygodnia; kolejne raporty w tym
samym mieście korzystają z tych samych rozbiorów). Równoległość: pewne 4 naraz (30 naraz → 429).

## Ryzyka
- Czas raportu (zmierzone 30.09): dziś 6–13 min (raport 279, 245 usług: 13 min). Samo porównanie podpisem
  32 s dla 71 usług i ~13 tys. par, bez modelu. Wąskie gardło = rozbiór ofert przez Z.ai: ~5 tys. ofert/h przy
  4 równoległych zapytaniach (test D). Raport 279 potrzebuje ~14 tys. ofert → ~3 h bez wcześniejszego rozbioru,
  typowy salon (~60 usług, ~4 tys. ofert) ~45 min. Dlatego krok D (rozbiór w tle) PRZED krokiem C; w raporcie
  tylko dociągnięcie nowych ofert w limicie 75 s jak pomost, reszta w tle.
- Oferty bez rozbioru w nowych regionach → mniej wierszy z porównaniem do czasu kroku D.
- Zmiana promptu rozbioru = nowa wersja rekordów (stare zostają, nic się nie miesza).
