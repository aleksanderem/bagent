> Kopia (30.09.2026) dziennika pomiarów i decyzji — źródło: pamięć projektu `project_katalog_podpis_2026-09.md` (lokalna dla maszyny).


---
name: project_katalog_podpis_2026-09
description: "29.09 Alex zatwierdził nowy kierunek matchingu: katalog usług + podpis oferty (hybryda, GLM z abonamentu, TypeSafe tylko tak/nie raz na klasę różnicy). Plan, decyzje i poprawki z przeglądu."
metadata:
  node_type: memory
  type: project
  created: 2026-09-29
  last_verified: 2026-09-30
  originSessionId: 7e810d96-7166-4c44-a98e-b71a8aee880e
  modified: 2026-09-29T12:57:50.956Z
---

**Decyzja 29.09 (Alex zatwierdził plan `~/.claude/plans/precious-imagining-metcalfe.md`):** matching przechodzi
z drzewa TypeSafe (v14f) na **katalog usług + podpis oferty**. Powód: v14f kosztuje ~18,4 tys. tokenów na usługę,
95% z nich to menu opcji; decyzja „ta sama” w pytaniach na parach; warianty zlepione. Alex: „nie wierzę, że nie
możemy lepiej/taniej/szybciej … to ja Cię ograniczyłem”.

- Oferta = usługa albo wariant z własną ceną (decyzja Alexa). Cechy wyciąga GLM-5.3-flash (Z.ai; abonament,
  masowo wolno — Alex zna limity) raz na znormalizowany tekst oferty; JSON, frazy z tekstu.
- Modele: tylko Z.ai i MiniMax — **bez GPT** (Alex nie ma tam subskrypcji). TypeSafe = krótkie tak/nie/Score.
- „Zła para gorsza niż brak porównania”: cel „ta sama” ≥ 95%, luki = wiersz „ceny podobnych usług”.
- Hybryda (po przeglądzie): różnica jednostronna rozstrzygana RAZ na klasę (zabieg, cecha, wartość) kaskadą:
  domyślna zabiegu → opisowa → dopełnienie menu salonu → Score TypeSafe z pamięcią → podobna.
- Każde słowo nazwy/wariantu przypisane do cechy/szumu/słownika, inaczej brak „ta sama”; słownik 4-stanowy
  (to samo / węższe / szersze / inne); `rodzaj_ceny`; poziom stylisty poza podpisem; sieci liczone jako jeden salon.
- Kolejność: B0 bez modelu (identyczna nazwa: 179/189 par T) → test paczek GLM → słownik ~50 zabiegów →
  podpis vs hybryda vs v14f na 1042 ocenionych parach → sprawdzian 7 (dowód 95% = ~400 par przy ~97,5%).

**Pomiary etapu 1 (29.09, bagent `services/katalog_uslug/`, `scripts/katalog/`, dane `scripts/katalog/dane/2026-09-29/`):**
- B0 bez modelu (identyczna nazwa po normalizacji) na 1042 moich parach: 188 „ta sama”, 94,1% trafnych, odzysk 28%;
  bez rodzin wariantów 95,3% / 25%. Błędy: „Combo Premium”, kategoria „Usługi mobilne”, „dla dwojga”, skład z opisu.
- Test paczek GLM-5.3-flash (120 ofert, 0 USD, prompt v2): paczka 12 = jakość pojedynczych wywołań (zgodność z p1
  84% vs drugi pojedynczy przebieg 82%), paczka 20 gorsza (79%, błędy rekordów). **Model nie jest powtarzalny:**
  ten sam przydział słów do ról w dwóch przebiegach tylko 58%, ale ten sam ZBIÓR słów 82% → podpis porównuje zbiór
  rdzeni słów, role tylko do doboru źródeł (kontekst z kategorii gdy nazwa nie ma zabiegu / „dla kogo”) i pytań o klasy.
  ~2% odpowiedzi to uszkodzony JSON → jedno ponowienie. 4,6 s na pojedyncze wywołanie, 22 s na paczkę 12.

- Ocena na 1042 parach (zbiór UŻYTY do poprawek = kontrola, nie dowód; `scripts/katalog/ocena_podpisu.py --klasy`):
  podpis 276 „ta sama”, **97,8% trafnych, odzysk 42%** (B0 94,1% / 28%); zbiory 3–6: 98,0% / 40% vs v14f 87,9% / 59%.
  Klasy różnic TypeSafe: ~210 klas, łącznie ~0,015 USD. Wyciąganie 1456 ofert GLM: 0 USD, ~45 min (abonament
  dzielony z produkcją → ~32 oferty/min). Poprawki po drodze: zbiór słów zamiast ról, słowa nieprzypisane do zbioru,
  bez polskich znaków + 2 końcówki (12 par „rzes”/„rzęs”), „do/po/od” zostają, dopisek dzielony na poziomy
  (błąd: pytanie widziało tylko część dopisku), skład jednostronny = zawsze podobna (dodatek w nazwie).
  Zostałe błędy: 4× kontekst z kategorii („Usługi mobilne”, „dla dwojga”, etykieta Booksy zamiast kategorii
  „Fale radiowe”) + 2× moja niespójna ocena (Laser tulowy w zbiorze 3; skład z opisu). Główny ubytek odzysku:
  „różne słowa” (224 par T; 85 to słowo↔słowo: wąs/wąsik, przedłużanie/przedłużenie, kolor/malowanie).
  Następne: kategoria rozkładana raz (pierwszeństwo przed etykietą Booksy), słownik synonimów (kandydaci:
  pary ofert różniące się jednym słowem; negatyw: ten sam salon sprzedaje oba), potem sprawdzian 7 na nowych salonach.
- 29.09 wieczór (bagent 1e85a7c, bez push): słownik synonimów zrobiony (46 scaleń, 0,02 USD); pytanie o klasę v2 —
  v1 („inna oferta tego samego zabiegu”) przesądzało odpowiedź i przepuściło „Laserowe usuwanie tatuażu” = „Tatuaż”
  (0,83); v2 podaje obie oferty wprost, dopisek = tylko słowa różnicy, bez przykładów z testowanego przypadku.
  1042 pary: **96,8% trafnych, odzysk 47%**. Raport testowy na losowym salonie z prod (VEAN TATTOO, Poznań,
  `scripts/katalog/raport_testowy.py` + `raport_html.py`): 28 ofert → 10 z ceną „ta sama”, 9 tylko „podobne”,
  9 bez; 65/67 par „ta sama” trafnych. Artefakt: https://claude.ai/artifact/BjnqWbZQ4tM584FLiWmiQc
- 29.09 późno (bagent c1a0fbd, 5f4426a, bez push): (1) kategoria uzupełnia role, których nazwa nie podaje (obszar,
  poziom), i doprecyzowuje zabieg z kategorii nazywającej JEDNĄ rzecz; kategoria-lista nic nie dokłada; (2) ZAMIANY
  SŁÓW: różnica po obu stronach ≤2 słowa → TypeSafe Choice (to samo/węższe/szersze/inne/nie wiadomo) raz na klasę,
  „ta sama” tylko przy „to samo” ≥0,8 — v1 bez „nie wiadomo” wciskało nazwy własne w „to samo”; (3) szum z nazwy
  wraca do podpisu, gdy słowo jest cechą w innych ofertach rynku. 1042 pary: **97,7% / odzysk 54%** (zbiory 3–6:
  98,0% / 53% vs v14f 87,9% / 59%). Raport VEAN (piercing): 12/28 ofert z ceną, 62/67 trafnych (93%) — słabość:
  zamiana 2-słowna łączy synonim z nową cechą („Piercing Symetryczne” / „Przekłucie” → to samo); lekarstwo:
  słownik synonimów z całego rynku (dziś tylko ze zbioru 1042 par, bez piercingu). Sprawdzian 7 (18 nowych
  salonów, 8323 pary ofert) — `scripts/katalog/sprawdzian7.py`, wyciąganie w toku.

- 29.09 wieczór: Alex „ok” = PRZYJĘTE odstępstwa 1–7 (zbiór słów zamiast ról; nieprzypisane i szum-z-treścią
  w zbiorze; ujednolicanie słów + krótkie listy + progi z rozumowania; skład z opisu poza porównaniem; rodzaj różnicy =
  wspólne słowa pary; „nie wiadomo” w relacji słów; kategoria rozkładana osobno) + zgoda ~4 USD na pełny v14f
  w sprawdzianie 7 (szac. 3,97 USD, `dane/2026-09-28/v14_sprawdzian7`). Na 1042 parach bramka „pokrycie ≥ v14f
  w 8/9 branż” NIE przechodzi (podpis lepszy w 3, remis 1, v14f lepszy w 5; podologia 4 vs 21).

- SPRAWDZIAN 7 (29.09 noc, 18 NOWYCH salonów z 18 miast, 9 branż, 5345 par usług; v14f pełny 4,12 USD;
  `scripts/katalog/sprawdzian7.py`, dane `scripts/katalog/dane/2026-09-29/sprawdzian7/`, v14f `dane/2026-09-28/v14_sprawdzian7`):
  oceniłem WSZYSTKIE 225 „ta sama” podpisu + 96 tylko-v14f + 9 wariantów (330). Podpis **86,2% trafnych (97% poza
  depilacją), znajduje 80% znanych par**; v14f 71,2% / 65%. Bramka pokrycia ≥ v14f: 8/9 ✓; trafność ≥95%: ✗ —
  25/31 błędów z jednego salonu „LaserPoznań” (metoda tylko w nazwie salonu). Reszta błędów: opis mówi „tył ciała”
  / „Pedicure SPA” (skład/obszar z opisu poza porównaniem), szum z wariantu („Uzupełnienie”, „& BODY”), „(Podolog)”.
  Poprawki: nazwa salonu jako kontekst metody (tylko metoda, tylko gdy oferta i kategoria jej nie podają, nazwa nie
  jest listą), szum z etykiety wariantu wraca. Dowód po poprawkach wymaga sprawdzianu 8 (7 jest już użyty).

- 30.09 ~00:00 (bagent e2994c5, bez push): po poprawkach (nazwa salonu → metoda; kategoria/Booksy uzupełniają,
  gdy nazwa nie ma własnego ZABIEGU — rola metody bywa błędna; szum z wariantu; słownik z rynku 215 scaleń, 5 moich
  odrzuceń) sprawdzian 7 (UŻYTY): 98% trafnych, par tyle samo; zbiór 1042 bez zmian. Próby odrzucone po bilansie:
  kategoria dokładająca zabieg zawsze (−24 prawdziwe pary), lista „słów metod rynku” z ról (laser siedzi we frazie
  zabiegu → nie łapie). Sprawdzian 8 (DOWÓD, 18 nowych salonów, bez v14f): `sprawdzian7.py --wyjscie sprawdzian8`.

- SPRAWDZIAN 8 (30.09 noc, DOWÓD, 18 nowych salonów, reguły bez zmian po sprawdzianie 7; bagent c86df25): 812 par
  „ta sama”, moja ocena 50/branżę ważona + 150 losowych reszty → **96,3% trafnych** (bramka ≥95% ✓), odzysk ~65%
  (szacunek), warianty 98%. Branże ≥94% poza: Masaż 32% (oferta podmiotu = SZKOLENIE Kobido w kategorii „SZKOLENIA”
  porównane z masażami — brak rozpoznania „to nie usługa” z kategorii), Depilacja 6/7. Słownik rynku 313 scaleń
  (odrzucone przeze mnie m.in. zestaw≠pakiet, komplet≠zestaw). Dalej: kategoria mówi „szkolenia / vouchery /
  produkty” → pozycja poza porównaniem; potem etap 3 planu (wpięcie za przełącznikiem — wymaga zgody na migrację).

- 30.09 rano (bagent 1c1efb8, 857bffa, bez push): (1) pozycja z KATEGORII cennika (osobne pytanie GLM v2: usługi /
  szkolenie / voucher / produkt, fraza z nazwy) — v1 wyrzucało instruktorki i „Usługi dodatkowe”, więc bez „dodatek”
  na poziomie kategorii; produkt/voucher/dodatek/szkolenie obok usługi = INNA (też poza „podobnymi”). Sprawdzian 8:
  96,3% → 97,8% (odpadło 14 błędnych par szkolenia Kobido), reszta zbiorów bez zmian. (2) SUCHY PRZEBIEG (stary silnik
  lokalnie tylko odczyt vs podpis, 107 usług sprawdzianów 7–8): stary cena w 85% wierszy, podpis „ta sama” 19–35%,
  tylko „podobne” 26–31%, bez porównania 33–55%; gdzie oba — mediany ±5–9%. Wiersze tylko ze starym: ~½ to usługi
  autorskie/markowe (stary wycenia je z cudzych usług, np. 525 zł z 32 salonów), ~½ ma 1–2 salony „ta sama” (odzysk
  ~65%). Wniosek: przed wpięciem podnieść odzysk — zaplanowana kaskada (domyślne zabiegu, opisowe, dopełnienie menu).
- 30.09 południe (bagent f725de8, bez push; Alex: „Najpierw odzysk”): słowo nazwy wpisane przez GLM do frazy
  z etykiety Booksy/kategorii wraca jako własne (tylko gdy nie pokryte własną frazą — inaczej „bikini” udawało zabieg),
  różnica dwustronna ≤ 2+2 słowa rozkładana na dwa dopiski jednostronne (klasy TypeSafe), negatyw słownika tylko przy
  różnych cenach. Pokrycie „ta sama”: spr8 35→44%, spr7 19→21%, trafność 97,7/96,6%. ODRZUCONE po pomiarze:
  cecha opisowa i wartość domyślna ze statystyk rynku (na danych z samych kandydatów ratują tyle złych par co
  dobrych — wrócić przy pełnych cennikach), próg P(„nie zmienia”)>0,5 (spr7 → 93%). Sufit teraz: ~⅓ wierszy to
  usługi autorskie/zestawy (brak tożsamej = poprawnie), ~¼ ma 1–2 salony. W toku sprawdzian 9 (18 salonów, 71 usług).
- SPRAWDZIAN 9 (30.09 po południu, DOWÓD, 18 nowych salonów, reguły zamrożone; moja ocena na ślepo 657 par;
  bagent dd0ec13): **91,8% trafnych „ta sama” — bramka ≥95% ✗.** Branże: Barber 84, Brwi 100, Depilacja 90,
  Fryzjer 98, Masaż 98, Med-est 100, Paznokcie 100, Podologia 90, Salon Kosmetyczny 56. Wg mechanizmu: równe zbiory
  słów 98,5% (53% par), dopisek jednostronny 95,0% (35%), zamiana słowa 55% (8%), dopiski po obu stronach 52% (4%).
  Pokrycie wierszy: „ta sama” (≥3 salony) 42% (stary silnik: cena w 72%), 1–2 salony 23%, brak 35%. 77% ważonego
  błędu to dwie usługi: „Hybryda na stopy” vs „Pedicure hybrydowy” (zamiana pedicure↔stopy „to samo” 0,92; moja
  ocena: podobna — pedicure to też opracowanie stóp) i „Strzyżenie jedna długość maszynką” vs „Strzyżenie maszynką”.
  MECHANIZM: klasa różnicy (wspólne słowa, poziom, dopisek) rozstrzygana na JEDNEJ parze-przykładzie, a przykład
  bywał „brudny” — druga oferta miała dopisek innymi słowami („Combo - BuzzCut + Broda” → strzyżenie przy brodzie
  „nic nie zmienia”; „Maszynka(1długość)”; „Pedicure hybrydowy” jako oferta bez „stóp”). Odpowiedź trafna dla
  przykładu, pamięć uogólnia ją na pary, gdzie druga oferta naprawdę tego nie ma (broda ≠ strzyżenie + broda,
  hybryda na stopy ≠ hybryda na dłonie). Reszta błędów (~2%): metoda tylko w kategorii (SHR/IPL przy „laserowej”),
  „(do zabiegu)” = dodatek, „lub”, infuzja skóry głowy vs twarzy. Poprawka: przykład klasy tylko z różnicy
  jednostronnej, w której oferta bez dopisku nie ma go w żadnej postaci; dowód na sprawdzianie 10.
- 30.09 ~14:20 (bagent ef7b403): poprawka przykładów (czysty przykład, szukanie w parach dwóch innych salonów puli,
  pamięć klas v3 `klasy_p3.json`, 0,027 USD). DECYZJE ALEXA: (1) „Hybryda na stopy” = „Pedicure hybrydowy” — ta sama
  (moje 24 oceny P→T: 21 w spr9, 3 w v13; skrypt `diagnoza/decyzja_pedicure.py`; frezowanie / podeszwa / „pełny”
  zostają podobne); (2) wiersz z 1–2 salonami „ta sama” pokazuje ich ceny bez mediany rynku. Wyniki po decyzji:
  **spr9 na zamrożonych regułach 95,9% — bramka ✓** (Barber 84%); po poprawce (zbiory już użyte): spr9 98,3%, spr8
  97,9%, spr7 97,5%, 1042 pary 98,3% / odzysk 54%. Pokrycie wierszy „ta sama” ≥3 salony po poprawce: spr9 44%, spr8
  39%, spr7 19%. Sprawdzian 10 (nowe salony, dowód poprawki) — losowanie i wyciąganie w toku.
- 30.09 ~14:40: ODRZUCONE v4 — pierwszeństwo przykładu, w którym dopisek widać w nazwie oferty (pamięć
  `w2/klasy_p4_odrzucone.json`, 0,002 USD): trafność bez zmian (spr9 98,2%, spr8 98,0%), mniej par „ta sama”
  (spr9 773→717), pokrycie spr8 39→37%. Zmiana samego przykładu odwróciła 18 klas w obie strony („męskie” przy
  strzyżeniu 0,18→1,04, „całego ciała” przy drenażu 0,31→1,21) — odpowiedź na jednym przykładzie jest szumna;
  kierunek na później: kilka przykładów na klasę i średnia. Zostaje v3. Pokrycie wierszy wg decyzji Alexa
  (1–2 salony „ta sama” z cenami): spr9 44%+17% = 61%, spr8 39%+30% = 69%, spr7 19%+17% = 36%; tylko „podobne”
  14/9/25%, bez porównania 25/22/40% (stary silnik z ceną 72/85/85%, ale ~35% jego próbek to ta sama usługa).
  Reguły zamrożone na sprawdzian 10: v3 (bagent ef7b403) + decyzja pedicure w ocenach.
- SPRAWDZIAN 10 / TEST D (30.09 wieczór, DOWÓD, 18 nowych salonów z 18 miast, reguły v3; moja ocena na ślepo 540 par;
  bagent 36c1bb8): **89,5% — bramka 95% ✗.** Branże: Barber 86, Brwi 100, Depilacja 75 (n=4), Fryzjer 98, Masaż 70,
  Med-est 94, Paznokcie 96, Podologia 80, Salon Kosm. 100 (n=1). Mechanizmy: równe zbiory 92%, dopisek jednostronny 85%,
  zamiana 93%, dopiski po obu stronach 80%. 58% ważonego błędu = „Rekonstrukcja paznokcia”: podologiczna odbudowa płytki
  u stopy (50–220 zł) vs naprawa złamanego paznokcia przy manicure (10–30 zł) — różnica tylko w kontekście (opis „paznokci
  stóp”, typ salonu Podologia, kategoria „Manicure / Stylistka paznokci”); klasa „stylizacja” przy rekonstrukcji uznana
  za nieistotną na przykładzie z manicure. Odzysk ~35% (synonimy wąs/wąsik, ombre/babyboomer, keratyna/prostowanie
  keratynowe; pozycja dodatek/zabieg niespójna przy Olaplex). Pokrycie wierszy: „ta sama” ≥3 salony 34% + 1–2 salony 19%
  = 53%, tylko podobne 21%, brak 26% (stary silnik z ceną 87%). Sygnał ceny (testy A–D, oceny „ta sama”): różnica cen ≥5×
  w 2/1076 par prawdziwych (0,2%) i 21/93 błędnych (23%); ≥3× — 1,6% vs 34%.
- 30.09 ~17:00 (bagent be0f4ca): DECYZJA ALEXA „od 5×” — strażnik ceny (`dopasowanie.straznik_ceny`, PROG_CENY 5,0):
  para z ceną różną ≥ 5× nigdy „ta sama” (→ podobne), w wycenie raportu i w pomiarach. Test D 89,5% → 91,8%; A–C bez
  strat (97,5 / 98,0 / 98,3%); 1042 pary 98,3%. Pokrycie wierszy bez zmian (D: 34% + 19%).
- 30.09 ~17:40: ODRZUCONE — „dziedzina z kontekstu” (poziom 1 modelu z działu cennika albo typu salonu; TypeSafe Score
  raz na klasę: najpierw „czy sama nazwa wskazuje część ciała”, potem dla nazw wieloznacznych „czy miejsca w cenniku
  wskazują inną część ciała”; 4 wersje pytań po próbach po 40 klas, razem ~0,02 USD). Bilans na testach A–D (ważony,
  te same pary, słowa bez zmian): usuwa 18,6 błędnych par, ale 66,1 prawdziwych (3,5 : 1); wariant „weto tylko przy
  wyraźnie innej części ciała” — nic nie usuwa. Przyczyna: TypeSafe waha się między „nie wiadomo” a „inna” także przy
  oczywistych parach („Peeling kawitacyjny” w dziale „Kosmetyka” vs „Pielęgnacja twarzy”, „Hybryda na stopy” w salonie
  kosmetycznym vs dział „Pedicure”), a salon z masażem i rekonstrukcją paznokcia za 70 zł jest niejednoznaczny z samej
  natury. Kod: `docs/katalog_uslug/odrzucone/dziedzina_z_kontekstu_2026-09-30.patch` (git apply), odpowiedzi:
  `dane/2026-09-29/w2/{dziedziny,jednoznacznosc}_p*_odrzucone.json`. Nie wracać bez nowego sygnału (np. pełne menu salonu).
  Odrzucone też (pomiar, bez modelu): „zestaw ≠ pojedynczy zabieg” (C: −59 prawdziwych — GLM niespójny w pozycji
  zestaw/zabieg) i „nazwa z + wobec nazwy bez połączenia” (5,8 błędnych za 43 prawdziwe).
- 30.09 ~17:50: PRZYJĘTE — „lub/albo” wobec „+ / i / oraz / z” (`podpis._laczenie`, przeszkoda przed porównaniem
  słów): „Depilacja uszu lub nosa” (jedno w cenie) ≠ „Depilacja uszu + nosa” (oba) — poziom „gdzie i ile”, zbiór słów
  tego nie widzi; przecinek i ukośnik bez rozstrzygnięcia. Bilans A–D: 3 błędne pary usunięte, zero prawdziwych.
  Wyniki: A 97,5% (201), B 98,3% (804), C 98,6% (771), D 92,0% (579); łącznie A–D 96,8%; 1042 pary 98,3% / odzysk 54%.
  Pokrycie wierszy bez zmian. Reguły zamrożone na test E (sprawdzian 11, nowe salony).
- TEST E / SPRAWDZIAN 11 (30.09 wieczór, DOWÓD, 18 nowych salonów z 18 miast, reguły zamrożone bagent 502562e; rozbiór
  GLM 7032/7065 ofert, klasy + zamiany TypeSafe 0,057 USD; moja ocena na ślepo 716 par): **95,9% trafnych „ta sama”
  (519/541) — bramka 95% ✓.** Branże: Paznokcie 99,3 (139), Salon Kosm. 98,9 (90), Fryzjer 97 (100), Depilacja 94 (67),
  Masaż 91,9 (111), Brwi 89,5 (19), Barber 100 (7), Med-est 100 (4), Podologia 50 (4). Warianty 88% (25). Błędy (22,
  rozproszone): masaż — dopiski „Relaks”, „Rytuał”, „Twój pierwszy”, „z elementami” uznane za nieistotne przy Kobido,
  bańka chińska z innym zakresem w opisie; lasery różnego typu tylko w dziale (DEKA CO2 / Fotona Er:YAG); „do zabiegu”;
  półdługie ≈ średnie (zamiana „to samo”); wariant „Farbowanie rzęs” usługi „brwi i/lub rzęs”; pedicure w dziale
  „Podologia”. Odzysk ~45% (reszta: 11,3% prawdziwych w próbie 150/5574). Pokrycie wierszy: „ta sama” ≥3 salony 31% +
  1–2 salony 13% = 44%, tylko podobne 15%, brak 41% (stary silnik z ceną 61%); mediany ±5% tam, gdzie oba mają cenę.
  Łącznie A–E: 96,6% (2896 par „ta sama”).

- 1.10 (bagent a4a31e3, 93e8e9f) — ODZYSK, każda zmiana bilansem na A–E (wszystkie pary, które zmieniły werdykt,
  ocenione przeze mnie; oceny spoza próby w `ocena_claude_dodatkowe.json`): (1) pisownia — NFKC (ozdobne litery Booksy),
  zakres „2-3D” = „2D 3D”: +30 trafnych / +2 błędne; (2) słownik z całego rynku: 19 359 nowych par (0,56 USD — szacunek
  skryptu był 2× za niski, stała poprawiona na zmierzone ~700 tok./parę), mój przegląd 304 scaleń, 11 odrzuconych
  z powodem (np. „wąski” ≠ woskowanie, „farbka” ≠ barwienie, „komplet” ≠ całe — to obchodziło ochronę pustej nazwy):
  +73 / +3; (3) „20 ml” = „20ml”: +2 / 0; (4) słowo wykonawcy (≥ 80% głosów rynku „specjalista”: top, senior, junior,
  master — rozkład dwubiegunowy, poziomy usługi ≤ 15%) poza podpisem, plan p. 5: +128 / 0. Naprawy pomiaru i pamięci:
  pamięć klas i zamian przenoszona do form głównych przy przebudowie słownika (ta sama klasa pytana ponownie dała
  0,48 → 0,51 na progu i 36 par „Ściągnięcie rzęs” ginęło — zasada „klasa raz”); walidacja paczek rozbioru na wszystkich
  ofertach zbioru (przesunięcie listy gubiło całe paczki — w E ~990 par); test pustej nazwy na rdzeniach przed słownikiem.
  ODRZUCONE po pomiarze: „liczba w dopisku = różnica” (−8 trafnych za −3 błędne); zakazy łączenia grup z każdej
  odpowiedzi „inne/węższe” i negatywów (rozcinały prawdziwe grupy odmian: uzupełnienie/uzupełnianie, męskie/mężczyzn,
  żel/żelowe) — zostały tylko moje odrzucenia. Wynik A–E: trafność 96,8% (A 98,1 / B 98,4 / C 98,9 / D 92,4 / E 95,7;
  64% par „ta sama” ocenionych wprost), pokrycie wierszy „ta sama” 53,6% (było 52,4%): par przybyło ~230, ale głównie
  w wierszach, które już miały cenę. Przegląd 53 wierszy „tylko podobne”: prawdziwa „ta sama” wśród kandydatów w ~15
  (literówki, „średnie (do ramion)”, poziom wykonawcy, „20 ml”), reszta to poprawne „podobne” (inny zakres, dodatek,
  pakiet). Błąd D nadal skupiony w „Rekonstrukcji paznokcia” (podologia vs manicure — sam kontekst salonu).

- TEST F / SPRAWDZIAN 12 (1.10 wieczór, DOWÓD, 18 nowych salonów z 18 miast, ziarno 20261012, reguły zamrożone
  bagent 8b30f64; rozbiór GLM 7134/7208 ofert, klasy + zamiany TypeSafe 0,07 USD; moja ocena na ślepo 543 par; bagent
  7c8e447): **96,9% trafnych „ta sama” (317/327) — bramka 95% ✓.** Branże: Barber 100 (34), Brwi 100 (51), Depilacja
  100 (1), Fryzjer 96 (27), Masaż 95 (55), Med-est 92 (59), Paznokcie 100 (23), Podologia 98 (52), Salon kosm. 100 (25);
  warianty 66/66. Błędy (10): 6 granicznych przy ostrej ocenie („bikini pogłębione” = „głębokie” jako zamiana słów,
  masaż „regeneracyjny” = „leczniczy” wg działu salonu, przedszkolak w dziale damskim i męskim, drenaż w dziale
  „Endermologia”), 2 prawdziwe — ombre brwi (makijaż permanentny) uznane za ombre włosów: klasa dopisku liczona tylko
  na wspólnym słowie „ombre”, bez dziedziny (por. odrzucona „dziedzina z kontekstu”), 1 kwas migdałowy wobec „kwasów”.
  Odzysk ~32% (próba reszty 150/6675). **Pokrycie wierszy 46,5% (≥3 salony 34% + 1–2 salony 13%) — poniżej celu ~50%;**
  tylko podobne 17%, brak 37% (Depilacja 6/8 bez ceny — laser „broda”, „palce u nóg” w małym mieście); stary silnik
  z ceną 73%, różnica median tam, gdzie oba mają cenę, 15% (n=23). Łącznie testy A–F: pokrycie 52,3% (203/388 wierszy).

- RAPORT 279 — PRZEBIEG PRÓBNY ETAPU 3 (1.10 wieczór, decyzja Alexa „Podpinamy”; tylko odczyt prod, zapis lokalny;
  reguły jak w teście F, bagent e6a18b1, dane 4d51c69): klinika medycyny estetycznej, Warszawa — 245 usług / 419 ofert
  (194 to warianty-pakiety 3/4/6/10 zabiegów), pula 2801 salonów, 26,7 tys. par. Rozbiór GLM 15 132/15 444 ofert (98%,
  ~2,5 h), salony 2790/2801, działy 2229 (3 poza usługami: sety, kursy, karnety); klasy + zamiany TypeSafe 0,18 USD
  (2686 klas, 1320 zamian). Oferty: „ta sama” ≥3 salony 13%, 1–2 salony 9%, tylko podobne 26%, brak 53% (117 z 220
  „brak” to pakiety); bez pakietów „ta sama” 37%. Raport, który widziała klientka: cena w 72/222 pozycji (32%); cennik
  zmienił się od raportu, zgodnych nazwą i ceną 106 pozycji: stary 58%, podpis „ta sama” 44% (33% + 11%), mediany ±21%
  (n=32). **Moja ocena na ślepo 80 par (50 „ta sama”, po jednej z losowej pozycji, + 30 pozostałych): 78% trafnych
  „ta sama” (39/50) — poniżej bramki.** Mechanizm: 9 z 11 błędów w jednym dziale „Usuwanie kurzajek i zmian skórnych
  (laser CO2)” — metoda jest tylko w nagłówku działu, a nagłówek wymienia dwie rzeczy, więc reguła z 29.09
  (kategoria-lista nie mówi, której pozycji dotyczy) odrzuca też metodę wspólną dla całego działu; usuwanie ręczne,
  elektrokoagulacją i w gabinecie podologii przechodzi jako „ta sama” (14 pozycji z ceną, 9 z medianą, np. prosak 33 zł
  wobec 199 zł). Pozostałe 2: Aquashine wobec Aquashine PTX (klasa „nieistotne” od TypeSafe), pakiet 3 zabiegów tylko
  w nagłówku działu konkurenta. Bez tego działu 37/39 (95%); 4 z 11 błędów sporne. Zgubiona 1 z 30 (botoks „1 okolica”
  / „lwia zmarszczka”). Wniosek: przełącznika nie włączać; dalej metoda z nagłówka działu wspólna dla jego pozycji
  (raz na dział), bilans na A–F i dowód na nowych salonach per branża, potem ponownie raport 279. Dane: `raport_279/`
  (`probka_oceny.json`, `ocena_claude.json`, `porownanie_raportu.json`, `bez_porownania.json`).

- 1.10 ~22:30 DECYZJA ALEXA („tak”, po uwadze „marnujesz tokeny na pojedyncze przypadki”): zamiast reguł, które słowa
  nagłówka działu i nazwy salonu doklejać do oferty — jeden mechanizm, w którym kontekst rozstrzyga model.
  Wersja 1 (bagent 6e54fa4, `dzialy.py`): GLM czyta dział naraz i sam dopisuje frazy nagłówka. Próba 40 ofert dobra
  (laser CO2 trafia do każdej usługi działu), ale test F: −68 prawdziwych par za −3 błędne — ten sam nagłówek u dwóch
  salonów dostawał raz „kobiety”, raz „pastą cukrową”. ODRZUCONA (niespójność, nie treść).
  Wersja 2 (bagent a97d7d9, 429e3fe, `stosowalnosc.py`): części kontekstu STAŁE (nagłówek, nazwa salonu i etykieta
  Booksy rozłożone raz na tekst), a TypeSafe Noul rozstrzyga raz na (część, nazwa oferty) z pamięcią: „czy ta fraza
  dopowiada coś, co odróżnia usługę — zabieg (gdy nazwa go nie mówi), metodę, obszar, dla kogo, ile zabiegów”; zabieg
  z kontekstu tylko, gdy TypeSafe uzna, że sama nazwa nie mówi, co to za zabieg. Pytanie v1 („czy mówi coś prawdziwego”)
  w próbie dopisywało „Medycyna Estetyczna”, „Kosmetologia”, „Hair” — zawężone. Przy okazji wyszła wada starego
  mechanizmu: różnica po obu stronach rozstrzygana dwiema osobnymi zgodami stron („brwi” przy ombre — nie zmienia,
  „koloryzacja” przy ombre — nie zmienia ⇒ makijaż permanentny = farbowanie włosów); dotąd maskowały ją słowa
  doklejane regułami. Teraz różnica po obu stronach tylko przez wspólną ocenę (zamiana), domyślnie.
  Wynik (moja ocena na ślepo, pary ze zmienionym werdyktem + nowa próba 279): test F trafność 97,4% → 96,2% (382
  trafnych w obu; błędów 10 → 15, z tego 6 spornych: typ lasera tylko w opisie, usługa z dojazdem), wiersze z ceną
  „ta sama” 47% → 55% (≥3 salony 34% bez zmian, 1–2 salony 13% → 21%); raport 279 trafność 78% → ok. 97% (71/73
  ocenionych), oferty z ceną „ta sama” 22% → 21% (znika dział laserowy porównywany z usuwaniem ręcznym). Koszt nocy
  TypeSafe ~1,1 USD (próby, oba zbiory, klasy), GLM w abonamencie. F i 279 posłużyły do poprawek — dowód: test G
  (sprawdzian 13, 18 nowych salonów, ziarno 20261013), rozbiór w toku.

**Why:** koszt i jakość v14f utknęły (87–94%, 3/4 par) na ograniczeniach metody, nie na strojeniu.
**How to apply:** nowa praca nad matchingiem idzie tym planem; v14f zostaje punktem odniesienia w pomiarach.
Model tej samej usługi ([[feedback_model_tej_samej_uslugi]]) bez zmian. Poprzednie ustalenie drzewa:
[[feedback_typesafe_cookbook_drzewo]]. Pomiary: [[project_trafnosc_matchingu_2026-09]].
