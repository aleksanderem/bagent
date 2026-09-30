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

**Why:** koszt i jakość v14f utknęły (87–94%, 3/4 par) na ograniczeniach metody, nie na strojeniu.
**How to apply:** nowa praca nad matchingiem idzie tym planem; v14f zostaje punktem odniesienia w pomiarach.
Model tej samej usługi ([[feedback_model_tej_samej_uslugi]]) bez zmian. Poprzednie ustalenie drzewa:
[[feedback_typesafe_cookbook_drzewo]]. Pomiary: [[project_trafnosc_matchingu_2026-09]].
