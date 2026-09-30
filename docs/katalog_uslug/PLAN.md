> Kopia (30.09.2026) — źródło: `~/.claude/plans/precious-imagining-metcalfe.md` (plan zatwierdzony przez Alexa 29.09) + sekcje stanu dopisywane w trakcie.


# Matching usług od nowa: katalog usług + „podpis” oferty

## Kontekst
- Dziś (v14f, tylko w pomiarach): drzewo TypeSafe do zabiegu + frazy + pytania na parach. Trafność „ta sama”
  87–94% na nowych salonach, znajduje ~3/4 znanych par, ~18,4 tys. tokenów na usługę (0,00077 USD); 95% tokenów
  przejścia drzewa to menu opcji wysyłane przy każdej usłudze. Cały rynek ~850–1000 USD, każda zmiana drzewa =
  płacimy od nowa, raport w nowym rejonie 2–4 USD i minuty wywołań.
- Produkcja dziś: najbliższe wektory nazw (cosinus ≥ 0,68, 15 km) + reguły wet + mediana zł/min. Warianty
  zlepione do pierwszego, zabieg Booksy nie jest sygnałem. ~35% próbek to naprawdę ta sama usługa.
- Alex (29.09): w planowaniu pominąć wszystkie jego dotychczasowe ograniczenia — ma być lepiej, taniej, szybciej.

## Co zatwierdzasz tym planem
Odejście od: drzewa TypeSafe jako głównej metody (zostaje do tak/nie), zakazu prostego czyszczenia tekstu
w kluczu, zasady „jedna usługa na wywołanie” dla GLM (do sprawdzenia w próbie), pomijania zabiegu Booksy
(wraca jako kontekst). Model tej samej usługi (5 poziomów + skład) zostaje bez zmian. Pierwszy etap to próba
za < 1 USD — dalej tylko po wyniku.

## Decyzje Alexa do tego planu (29.09)
- Masowe wyciąganie cech: tylko Z.ai (GLM) i MiniMax — bez GPT (brak subskrypcji). Abonament Z.ai wolno
  używać masowo, limity zna Alex. TypeSafe — do krótkich tak/nie.
- Każdy wariant z własną ceną = osobna pozycja do porównania.
- Zła para gorsza niż brak porównania: „ta sama” ≥ 95% trafności; luki uzupełnia wiersz „ceny podobnych usług”.

## Dlaczego drogo i dlaczego nie lepiej
1. Klasyfikujemy przez wybór z wielkich list → płacimy za menu, nie za usługę (5% tokenów to usługa).
2. „Ta sama” zapada w pytaniach na parach → koszt rośnie z liczbą par, wynik zależy od sformułowania pytania.
3. Warianty z osobnymi cenami zlepione w jedną usługę → błędy „rodzina wariantów”.
4. Brak rozróżnienia cechy wyróżniającej (UV) i opisowej („do 15 cm”) → główny błąd zbioru 6, w obie strony.
5. Każda poprawka = ponowne przejście wszystkich usług, bo wynik jest ścieżką w konkretnej wersji drzewa.

## Gdzie dotychczasowe zasady zawęziły pomysły
- „Tylko TypeSafe + drzewo jak w cookbooku” → klasyfikacja wyborem z wielkich menu = większość kosztu.
- „Nie kategorie Booksy” → zabieg Booksy (87% usług) przestał być nawet podpowiedzią; tu jest kontekstem, nie prawdą.
- „Jedna usługa na wywołanie” (zmierzone dla TypeSafe) → nie sprawdziliśmy paczek w modelu, który pisze JSON.
- „Bez reguł i regexów” → nawet czyszczenie szumu (ceny, czas, emotki) szło przez model; czyszczenie klucza jest darmowe.
- „Porównanie par modelem” → koszt i niestabilność rosną z liczbą par; podpis przenosi decyzję do bazy.

## Podejście: katalog usług + podpis oferty
Raz na ofertę wyciągamy jej cechy w małym JSON-ie, słownik cech ujednolicamy raz dla rynku, a „ta sama” to
równość podpisu — zapytanie w bazie, zero wywołań modelu w raporcie. Model 5 poziomów Alexa + skład zostaje jako
schemat podpisu (tak robią porównywarki cen: katalog produktów + przypisanie ofert).

1. **Oferta** = usługa albo wariant z własną ceną. Tekst: kategoria w cenniku + nazwa + etykieta wariantu +
   zabieg Booksy + typ salonu (+ opis, gdy jest — tylko skład, wyłączenia, liczby).
2. **Klucz i pamięć**: klucz = znormalizowany tekst oferty; wynik w bazie z wersją promptu i modelu. Oferta
   przerabiana raz; zmiana słownika NIE wymaga ponownego wyciągania.
3. **Wyciąganie cech** — GLM-5.3-flash z Z.ai (abonament, koszt krańcowy 0 USD, klient `KlientGLM` już jest),
   odpowiedź JSON, frazy kopiowane z tekstu (kod sprawdza, że fraza jest w tekście): `pozycja` (zabieg /
   pakiet N / zestaw / dodatek / produkt / konsultacja / voucher), `zabieg`, `dziedzina`, `metoda[]`, `obszar[]`,
   `rozmiar`, `liczba+jednostka`, `dla_kogo`, `etap`, `sklad[]`, `wylaczenia[]`, `dodatki[]`, `sesje`, `szum[]`
   (imię stylistki, promocja, czas) i przy każdej cesze **wyróżniająca / opisowa**.
   W próbie: paczki 1 vs 12 ofert na wywołanie (runner GLM już pracuje paczkami po 12) i MiniMax-M3 na małej
   próbce jako drugi model (tylko do porównania; wolny: 45–140 s na paczkę).
4. **Słownik = taksonomia z danych**: frazy każdej cechy z częstością → grupy podobnych (polski model wektorów
   mmlw-e5-large z `embeddings-local/`, darmowy; na tytanie już pisze `name_embedding_mmlw`, lokalnie wymaga
   pobrania ~2 GB — za zgodą) → **TypeSafe tak/nie** „czy X i Y to to samo przy zabiegu Z” (krótkie pytania, ułamki centa) → forma
   kanoniczna = najczęstsza fraza; wartości cech w obrębie zabiegu („długie” u włosów ≠ u paznokci); zabieg
   dostaje rodzica (dziedzinę). Poprawka jednej pozycji słownika poprawia wszystkie oferty naraz.
5. **Podpis** = kanoniczne (zabieg, metoda[], obszar[], rozmiar, liczba, dla kogo, etap, skład[], dodatki[],
   sesje), tylko cechy wyróżniające. „Ta sama” = równe podpisy. Nieokreślone vs określone → podobna, chyba że
   określona wartość jest domyślna dla zabiegu (potwierdzone tak/nie).
6. **Raport**: równość podpisu w puli konkurencji → ceny „tej samej”; < 3 salony → schodzimy poziomami modelu
   (bez cech opisowych → zabieg + obszar → zabieg), wiersz „ceny podobnych usług”. Mediana zł/min bez zmian.

## Poprawki po niezależnym przeglądzie (29.09, po zatwierdzeniu — ten sam kierunek)
1. **Hybryda zamiast samej równości podpisu.** Samo „nieokreślone vs określone = podobna” zbiłoby pokrycie do
   ~45–55% (24% par T różni się tylko nadmiarem po jednej stronie, 43% po obu; produkcja już raz spadła tak
   z 92% do 57% wierszy z ceną). Cecha niepodana = „nieznana”, rozstrzygana po kolei: (a) domyślna zabiegu
   (≥ 80% ofert, które ją podają + potwierdzenie TypeSafe), (b) opisowa (żaden salon nie sprzedaje obu wersji
   w różnych cenach), (c) dopełnienie menu (ten sam salon ma obok ofertę z tą wartością → tu jej nie ma),
   (d) Score TypeSafe RAZ na klasę różnicy (zabieg, cecha, wartość) z pamięcią — nie na parę, (e) reszta = podobna.
2. **Każde słowo nazwy i etykiety wariantu musi trafić do cechy, szumu albo słownika** — oferta z nieprzypisanym
   słowem nie dostaje „ta sama” (chroni przed zgubionym „UV”).
3. **Flaga wyróżniająca / opisowa w słowniku raz na (zabieg, wartość)**, nie przy każdej ofercie; podpis liczony
   przy odczycie z zapisanych fraz; oferty niespójne w dwóch przebiegach → pytanie o klasę różnicy.
4. **Słownik z relacją 4-stanową** (to samo / węższe / szersze / inne) → graf zamiast płaskich grup; darmowy
   negatyw: salon sprzedaje obie frazy osobno → nie synonimy; nazwa bez treści (Combo Premium, BASIC) bez składu
   nigdy „ta sama”.
5. **Cena i liczenie salonów:** pole `rodzaj_ceny` (stała / „od” / za sztukę / pakiet łącznie); minuty jako
   wielkość przy zabiegach sprzedawanych na czas (decyzja raz per zabieg); poziom stylisty poza podpisem, jedna
   wartość na salon; salony z niemal identycznym cennikiem (sieci) liczone jako jeden.
6. **Kolejność próby:** B0 bez modelu (identyczna nazwa po normalizacji: 179/189 par T, ~95%) jako poprzeczka →
   test paczek {1, 1 ponownie, 12, 20 losowo, 20 wg zabiegu} na ~300 ofertach z zabezpieczeniami (id + dosłowna
   nazwa w odpowiedzi, frazy z własnej oferty, zła liczba rekordów = odrzut) → słownik ~50 zabiegów → podpis
   vs hybryda vs v14f na tych samych parach. Dowód 95%: ~400 par „ta sama” przy ~97,5% (200 par daje dolną
   granicę ~91%).

## Koszt i czas: dziś vs nowe
| | v14f (TypeSafe drzewo) | Katalog + podpis |
|---|---|---|
| Przerobienie oferty | ~18,4 tys. tok., 0,00077 USD | ~1 tys. tok. GLM, 0 USD (abonament) |
| Cały rynek (~1,0–1,5 mln kluczy) | ~850–1000 USD | 0 USD + tło kilka tygodni przy limicie abonamentu; słownik < 5 USD |
| Raport, nowy rejon | 2–4 USD, minuty | 0 USD przy ofertach w bazie; brakujące przez istniejący most GLM |
| Porównanie par w raporcie | pytania modelu | zapytanie w bazie (ms) |
| Poprawka błędu | nowe przejście wszystkich usług | zmiana pozycji słownika |

## Plan realizacji (każdy etap kończy się decyzją)
1. **Próba na ocenionych parach (GLM 0 USD, tak/nie TypeSafe < 1 USD):** usługi z 1042 moich par + eksport
   20 599 usług (`dane/2026-09-28/v12/probka.jsonl`) → wyciąganie → słownik → podpisy → trafność i pokrycie per
   branża na TYCH SAMYCH parach co v14f. Decyzja: model i paczkowanie; czy celujemy dalej.
2. **Sprawdzian 7 na 18 nowych salonach (0 USD GLM, słownik TypeSafe < 0,5 USD):** moja ocena próbki, ważenie
   jak `v14_wazone.py`. Bramka: ≥ 95% trafności „ta sama”, pokrycie ≥ v14f w ≥ 8 z 9 branż.
3. **Wpięcie za przełącznikiem** w wycenę (`MATCHING_SOURCE` = stary / podpis, w panelu admina + kontrolka
   autodiagnostyki): oferty-warianty z `salon_scrape_service_variants`, dopasowanie po haszu podpisu w puli
   `fn_competitors_in_radius`; suchy przebieg na 20 raportach obok starego silnika.
4. **Wypełnienie bazy w tle** (GLM, limit dobowy liczony przed włączeniem wg bramki kosztów) + dociąganie
   brakujących ofert w raporcie. Kolejka na wzór `taxonomy_queue` (budowana per region funkcją w bazie,
   wznawianie po fladze „done”, `scripts/taxonomy_backfill.py`) — najpierw regiony, w których są raporty.

## Pliki
- bagent, nowy moduł `services/katalog_uslug/`: `normalizacja.py`, `ekstrakcja.py` (reuse `KlientGLM` z
  `scripts/taxonomy_backfill.py` przeniesiony do `services/`), `slownik.py` (wektory z lokalnego
  `embeddings-local/server.py` — mmlw, tak/nie przez `typesafe_sdk` jak `services/typesafe_profile/destylacja.py`),
  `podpis.py`, `dopasowanie.py`.
- Wycena: `services/similarity_pricing/report_pricing.py` (`compute_pricing_comparisons_v2`) i `engine.py`
  (`compute_market_price`) — nowa ścieżka pobierania próbek po podpisie zamiast `search_twins` + warstw
  tożsamości; statystyki cen bez zmian.
- Baza (migracja w BEAUTY_AUDIT): `oferta_cechy` (klucz → JSON cech, model, wersja promptu), `slownik_cech`
  (cecha, fraza, kanoniczna, zakres zabiegu, kto rozstrzygnął), podpis per oferta (usługa / wariant → hasz).
- Panel: `convex/settings/registrySerwery.ts` (przełącznik), `convex/admin/diagnostics*` (kontrolka).
- Pomiar: `scripts/katalog/` (próba, sprawdzian 7), ponowne użycie `v14_ocena.py` i `v14_wazone.py`.

## Weryfikacja
- Etap 1–2: trafność i odzysk per branża na parach ocenionych według modelu tej samej usługi, bilans z v14f na
  tych samych parach (ile lepiej / gorzej), rozkład błędów według cechy podpisu.
- Taksonomia: identyczne nazwy → ten sam zabieg ≥ 98%; przegląd 300 najczęstszych zabiegów i ich synonimów (ja).
- Etap 3: suchy przebieg 20 raportów — udział wierszy z ceną „ta sama” / „podobne”, zgodność mediany ze starym.
- Testy jednostkowe: normalizacja, równość podpisów (zbiory, nieokreślone), schodzenie poziomami.

## Ryzyka i jak je zamykamy
1. **Limity abonamentu Z.ai** (masowe użycie dozwolone — Alex, 29.09; 27.08 był epizod 429 na każdym
   wywołaniu) → runner z limitem dobowym i zatrzymaniem na pierwszym 429 (jak `taxonomy_backfill.py`), limity
   wpisane w panelu; zapas: MiniMax-M3 (cena niepotwierdzona w `services/ceny_modeli.py` — sprawdzić przed
   użyciem masowym).
2. **Niespójne wyciąganie** (ta sama oferta raz z metodą, raz bez) → schemat JSON, sprawdzenie „fraza jest
   w tekście”, drugi model tylko przy rozbieżnościach na próbie; różnice słów wyrównuje słownik.
3. **Słownik skleja za dużo („łydki” = „nogi”) albo za mało** → łączenie tylko decyzją tak/nie z ostrym progiem,
   wartości w obrębie zabiegu, przegląd 300 najczęstszych zabiegów; błąd poprawiany w jednym miejscu.
4. **Wartości domyślne** („Strzyżenie męskie” = nożyczki + maszynka?) są skrzywione, bo salony piszą cechę,
   gdy odbiega od normy → domyślna tylko po pytaniu „czy wynika wprost z nazwy zabiegu” (rozdzielało 0,8–0,94
   od 0,13–0,34 w pomiarze z 26.09).
5. **Ukryte różnice** („długie” = za ramiona w jednym salonie, za łopatki w drugim) → takie cechy zostają
   wyróżniające; gdzie 95% się nie da, wiersz idzie jako „podobne”, nie „ta sama”.
6. **Stronniczy pomiar**: ocenione pary pochodzą z kandydatów v13b/v14 → w sprawdzianie 7 osobna grupa par,
   które znalazł tylko podpis.

## Stan realizacji vs plan (29.09 wieczór, commit bagent e67cf20)
Zgodne z planem: oferta = usługa albo wariant; GLM z abonamentu (paczki po 12 — po teście); TypeSafe tylko
krótkie Score raz na klasę różnicy z pamięcią; model tej samej usługi bez zmian (dodatek w nazwie = podobna,
czas nie rozróżnia, produkty poza porównaniem); B0 → test paczek → ocena vs v14f na tych samych parach; w raporcie
zero wywołań modelu. Wynik kontrolny: 97,8% trafnych „ta sama”, odzysk 42% (1042 pary, zbiór użyty).
Odstępstwa (każde z pomiaru, do akceptacji Alexa):
1. Podpis porównuje ZBIÓR rdzeni słów, nie kanoniczne wartości per rola — model niepowtarzalny w przydziale ról
   (58%), zgodny co do słów (82%). Role zostają do doboru kontekstu, poziomu dopisku i pytań o klasy.
2. Słowo nieprzypisane NIE blokuje — wchodzi do zbioru (więcej słów = mniej fałszywych „ta sama”; blokada gubiła pary).
3. Deterministyczne ujednolicenie słów poza „prostym czyszczeniem”: bez polskich znaków, obcinanie ≤ 2 końcówek,
   krótka lista słów „nazwy bez treści” (combo, pakiet, premium…).
Jeszcze nie zrobione z planu: słownik synonimów (moduł gotowy, nieuruchomiony — główny ubytek odzysku: 224 pary),
kaskada (a) domyślne z danych, (b) opisowe z danych, (c) dopełnienie menu; `rodzaj_ceny`; sieci liczone jako
jeden salon; pamięć po tekście oferty (dziś po numerze paczki); sprawdzian 7; wpięcie w silnik i panel.
Nowe z pomiaru (poza planem, do zrobienia): kategoria rozkładana raz na salon z pierwszeństwem przed etykietą
Booksy (4 z 6 błędów to kontekst kategorii).

## Koszt planu
Etapy 1–2: < 1 USD (tak/nie TypeSafe), GLM 0 USD, MiniMax tylko mała próbka. Żadnych pętli ani cronów do
etapu 4; przed nim rachunek kosztu dobowego wg bramki kosztów automatyzacji.

## Stan 29.09 po raporcie testowym (commit bagent 1e85a7c, bez push)
Zrobione od e67cf20: słownik synonimów z danych (kandydaci z pisowni + pary ofert różniące się jednym słowem w różnych
salonach, negatyw „ten sam salon sprzedaje obie”, TypeSafe 4-stanowy, 46 scaleń, 0,02 USD); pytanie o klasę v2 (obie
oferty wprost — v1 przesądzało „ten sam zabieg” i przepuściło laserowe usuwanie tatuażu jako „Tatuaż”); kategoria
cennika rozkładana raz na tekst (kod gotowy, zbiór 1042 par w trakcie rozkładu).
Wynik na 1042 parach: 96,8% trafnych „ta sama”, odzysk 47%. Raport testowy na losowym salonie z prod (VEAN TATTOO,
Poznań, 25 konkurentów): 28 ofert → 10 z ceną „ta sama”, 9 tylko „podobne”, 9 bez porównania; 65/67 par „ta sama”
trafnych (oba błędy w wierszach bez ceny). Koszt: GLM 0 USD, TypeSafe < 1 centa.
Kierunek zgodny z planem; odstępstwa 1–3 bez zmian. Dalej: kontekst kategorii na 1042 parach → sprawdzian 7
(nowe salony, moja ocena, bilans z v14f) → kaskada (a)(b)(c), rodzaj_ceny, sieci, pamięć po tekście oferty.

## Stan 29.09 wieczór (bagent 5f4426a, bez push)
Dodane: uzupełnianie z kategorii wg ról (+ doprecyzowanie zabiegu z kategorii nazywającej jedną rzecz), zamiany słów
(różnica po obu stronach ≤2 słowa → relacja to samo / węższe / szersze / inne / nie wiadomo raz na klasę — to plan:
„czy X i Y to to samo przy zabiegu Z”), szum z nazwy wraca, gdy słowo jest cechą na rynku. 1042 pary: 97,7% / 54%.
Raport VEAN: 62/67 (93%). Sprawdzian 7: bez pełnego przebiegu v14f (~4 USD > budżet planu < 1 USD na etapy 1–2) —
odniesienie v14f ze zbiorów 5–6 + B0 na tych samych parach; próba warstwowa do mojej oceny. Dalej: słownik synonimów
z całego rynku (zbiór par + raport + sprawdzian 7), potem klasy/zamiany sprawdzianu, ocena, wynik ważony.

## Decyzje Alexa (29.09 wieczór, „ok”)
Przyjęte odstępstwa 1–7 (zbiór słów zamiast ról; słowa nieprzypisane i szum-z-treścią w zbiorze; ujednolicanie słów
w kodzie + krótkie listy + progi z rozumowania; skład z opisu poza porównaniem; rodzaj różnicy = wspólne słowa pary;
„nie wiadomo” w relacji słów; kategoria rozkładana osobno). Zgoda na ~4 USD: pełny przebieg v14f w sprawdzianie 7,
żeby bramka „pokrycie ≥ v14f w ≥ 8 z 9 branż” była mierzona na tych samych parach.

## Sprawdzian 7 — wynik (29.09 noc)
18 nowych salonów, 5345 par usług, v14f pełny (4,12 USD). Moja ocena: wszystkie 225 „ta sama” podpisu + 96 tylko v14f.
Podpis: 86,2% trafnych (97% poza depilacją), znajduje 80% znanych par; v14f: 71,2% / 65%. Pokrycie ≥ v14f w 8/9
branż ✓; trafność ≥ 95% ✗ przez jeden mechanizm: metoda tylko w nazwie salonu („LaserPoznań”, 25 z 31 błędów).
Poprawki w toku: nazwa salonu jako kontekst metody, szum z wariantu. Dowód: sprawdzian 8 na nowych salonach.

## Etap 3 — suchy przebieg (30.09, bagent 857bffa, bez zapisu na prod)
Pozycja z kategorii (szkolenia / vouchery / produkty poza porównaniem): sprawdzian 8 96,3% → 97,8%.
Stary silnik (lokalnie, tylko odczyt) vs podpis na 107 usługach sprawdzianów 7–8: stary daje cenę w 85% wierszy,
podpis „ta sama” w 19–35%, tylko „podobne” 26–31%, bez porównania 33–55%; tam, gdzie oba mają cenę, mediany ±5–9%.
Wiersze wyceniane tylko przez stary silnik: ~połowa to usługi autorskie i markowe (tożsamej nie ma — stary bierze
cudze), ~połowa ma 1–2 salony „ta sama” (odzysk ~65% prawdziwych par). Decyzja Alexa: wpinać teraz czy najpierw
kaskada (a) domyślne zabiegu, (b) opisowe, (c) dopełnienie menu (część tego planu, niezrobiona) i nowy sprawdzian.
