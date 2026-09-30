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

## Krok D (etap 4 planu) — rozbiór ofert całego rynku w tle
Osobny plan z rachunkiem kosztu dobowego (bramka kosztów automatyzacji): GLM z abonamentu, limit dobowy,
zatrzymanie na pierwszym błędzie limitu, najpierw regiony z raportami. Do tego czasu nowe regiony mają mniej
porównań (oferty bez rozbioru).

## Ryzyka
- Czas raportu: pobranie ofert konkurentów z bazy + dociągnięcia GLM (limit 75 s jak pomost).
- Oferty bez rozbioru w nowych regionach → mniej wierszy z porównaniem do czasu kroku D.
- Zmiana promptu rozbioru = nowa wersja rekordów (stare zostają, nic się nie miesza).
