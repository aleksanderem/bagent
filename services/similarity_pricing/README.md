# Silnik cenowy similarity-first + test tożsamości

> Moduł `bagent/services/similarity_pricing/`. Zastępuje stary, klasyfikacja-first
> pipeline cenowy (`_compute_pricing_comparisons` z tierami variant/method/structured
> + brand_marker regex). S0078, 2026-06-22.

## 1. Po co to istnieje (paradygmat)

**Cel:** dla usługi salonu-klienta („subject") znaleźć TOŻSAME usługi u konkurentów —
mimo różnych nazw i różnych natywnych taksonomii — i z tożsamego zbioru policzyć
cenę rynkową.

**Cena jest WYNIKIEM, nie kryterium.** Najpierw ustalamy „czy to ta sama usługa",
dopiero potem liczymy cenę z tego, co przeszło test tożsamości.

**Dlaczego nie klasyfikacja.** Stary pipeline klasyfikował usługi do taksonomii
(`treatment_method`/`variant_id`), a 78% usług lądowało w samogenerującym się
śmietniku `llm_inferred` (LLM tworzył nową metodę gdy alias nie trafił). Stąd
absurdy: presoterapia porównywana do endermologii, botoks-na-włosy do toksyny.
Osiem warstw starego pipeline to były podpórki pod tę zepsutą klasyfikację.

**Similarity-first.** Embedding nazwy (`name_embedding`, OpenAI text-embedding-3-small)
grupuje usługi semantycznie — czysto i uniwersalnie. To pierwszy sygnał tożsamości.
Test tożsamości (6 osi) odsiewa fałszywych bliźniaków. Cena wypada z reszty.

## 2. Architektura (przepływ)

```
subject service ─┐
                 ├─► fn_find_related_v2 (SQL, embedding ANN) ─► raw_samples (bliźniacy)
competitors ─────┘                                                     │
                                                                       ▼
                              ┌────────────────── engine.compute_market_price ──────────────────┐
                              │  1. DRUGIE PRAWO: adaptive_identity_filter (test tożsamości)      │
                              │     → odsiewa nie-tożsame, dobiera surowość per usługa           │
                              │  2. dedup_by_salon       → 1 reprezentant per salon              │
                              │  3. assess_sufficiency   → sufficient / thin / insufficient      │
                              │  4. normalize_unit       → cena rynkowa (bez pakietów, zł/min)   │
                              └─────────────────────────────────────────────────────────────────┘
                                                                       │
                                                                       ▼
                                                            MarketResult → wiersz competitor_pricing_comparisons
```

## 3. Warstwa danych — `fn_find_related_v2` (SQL, mig 144)

`fn_find_related_competitor_services_v2(subject_ids[], competitor_booksy_ids[], limit, min_similarity)`

Dla każdej usługi subject zwraca top-N bliźniaków (embedding cosine ≥ próg, domyślnie
0.82) wśród WYBRANYCH konkurentów raportu. Każdy wiersz niesie pola których potrzebuje
test tożsamości: `service_name`, `price_grosze`, `duration_minutes`, `category_name`,
`is_package`, `similarity` + metadane subjectu (`subject_*`).

- Filtr bazowy: `is_chain_head`, `is_active`, `price_grosze IS NOT NULL`, w puli konkurentów.
- Similarity liczone TYLKO względem konkurentów raportu (nie całej bazy) — to porównanie
  z wybranymi konkurentami, nie z całą Polską.
- **Perf:** bez indeksu = full-scan po usługach konkurentów. Dla raportu (15-50 konkurentów,
  ~1-15k usług) to akceptowalne. IVFFlat na całej bazie (7.36M) NIE wszedł — build-temp
  przekroczył dysk (patrz §11).

## 4. Test tożsamości — 6 osi (`layer_identity.py`)

Każda oś głosuje per para (subject, bliźniak): `for` / `against` / `abstain`.
**`abstain` = brak sygnału** (pusty/nieznany) — nigdy nie wycina; „nie wiem" ≠ „obce".

| Oś | Rola | Kierunek | Twarde weto? |
|---|---|---|---|
| `params` | ilość produktu (ml, liczba okolic/paznokci) | zgodne→for, sprzeczne→against | **TAK** |
| `package` | pakiet vs pojedyncza | różny→against, zgodny→abstain | **TAK** |
| `body_area` | zakres obszarów ciała (twarz vs twarz+szyja+dekolt) | różny zestaw→against | **TAK** |
| `price` | rozrzut ceny rzędu wielkości (zł/min ≥ 4×) | tylko against; **NIGDY for** | nie (miękka, waga 1.5) |
| `category` | kategoria właściciela | zgodna→for, rozłączna→against | nie (waga dynamiczna) |
| `duration` | skrajna różnica czasu (≥3×) | tylko against | nie (waga 0.5) |

**Twarde weta** (`params`, `package`, `body_area`): sprzeczność = definitywnie inna
usługa, niezależnie od surowości. To strukturalna tożsamość.

**Oś `price` jest asymetryczna:** nigdy nie głosuje „za" tożsamością (podobna cena nie
buduje klastra — byłaby kryterium ceny), tylko skrajny rozjazd (rząd wielkości przy
zgodnej nazwie) przeczy. Łapie „laser nieablacyjny 600 vs frakcyjny 3667". Per zł/min,
próg `PRICE_RATIO_AGAINST=4.0`.

**Waga `category` jest dynamiczna** (`_category_weight_for`): dla nazwy GENERYCZNEJ
(„Konsultacja") kategoria DECYDUJE (waga 2.0 — sama nazwa nie niesie tożsamości); dla
SPECYFICZNEJ („Presoterapia drenaż") kategoria waży mało (0.4 — embedding nazwy wystarcza,
różna kategoria to zwykle niespójność właściciela, nie inna usługa). To rozdziela
„konsultacja podologiczna vs estetyczna" (różne), zachowując „presoterapia w Pielęgnacji
ciała vs Modelowaniu sylwetki" (tożsama).

**Margines i surowość:** `identity_margin` = Σ(for·waga) − Σ(against·waga) dla osi miękkich.
`is_identity_match` = (brak twardego weta) AND (margines ≥ cutoff(surowość)). cutoff od
−1.5 (surowość 0, liberalny) do +0.1 (surowość 1, surowy).

### Drugie prawo — `adaptive_identity_filter`

Nie ma globalnej surowości. Dla każdej usługi silnik dobiera **najmniejszą surowość,
która czyni klaster wystarczająco TOŻSAMYM** (czystość ≥ `purity_target`) zachowując dość
salonów (≥ `min_salons`). „Zły wpływ" wykrywany operacyjnie: jeśli wyższa surowość tnie
salony, ale nie podnosi czystości — to ślepe cięcie, nie wybieramy go. Przy konflikcie
czystość vs wystarczalność decyduje `prefer` (sufficiency/purity).

`cluster_identity_purity`: udział bliźniaków tożsamych przy surowości referencyjnej (0.6,
pełna logika) — nie myli nazw specyficznych z różną kategorią.

## 5. Warstwy silnika (po teście tożsamości)

- **`dedup_by_salon`** (`layer_dedup.py`): jeden reprezentant per `booksy_id`. Siła rynku =
  liczba UNIKALNYCH salonów, nie usług (jeden salon z 24 wariantami nie zawyża).
- **`assess_sufficiency`** (`layer_sufficiency.py`): `sufficient` (≥5 salonów) / `thin`
  (≥3) / `insufficient` (<3). Insufficient → NIE pokazuj ceny („za mało danych" > zgadywanie).
  **„Thin" to feature, nie bug:** specyficzna usługa („Botox 2 okolice") z natury ma mało
  prawdziwych bliźniaków. Stary silnik to ukrywał, mieszając nieporównywalne.
- **`normalize_unit`** (`layer_unit.py`): wyklucza pakiety, liczy zł/min, cena rynkowa =
  median(zł/min na nie-pakietach) × czas subjectu. Percentyle, deviation.

## 6. Kategorie neutralne (`layer_neutral.py`)

Niektóre kategorie nie wskazują domeny i wstrzymują oś `category` (→ abstain):
- **`service`** (neutralne usługowo): „Konsultacja", „Pakiety", „Voucher", „Diagnostyka" —
  zbierają tę samą usługę z różnych domen. Neutralne TYLKO gdy gołe; „Konsultacja
  kosmetologiczna" ma domenę → głosuje normalnie.
- **`event`** (neutralne eventowo): „Promocje", „Oferta specjalna", „Bestsellery", „Nowości" —
  z natury mieszają różne usługi. Mocny marker bije domenę; słaby („dla niej") tylko gdy goły.

## 7. Wynik — `MarketResult` (`engine.py`)

`market_price_grosze` (None gdy insufficient), `status`, `n_unique_salons`, `deviation_pct`,
`p25/p50/p75_grosze`, `zl_per_min_median`, `identity_strictness` (dobrana adaptacyjnie),
`identity_purity`, `subject_generic`, `n_raw_samples`, `n_identity_kept`, `provenance`
(meta każdej warstwy — pełny ślad do debugowania i UI).

## 8. Kontrakt wyjściowy → `competitor_pricing_comparisons`

Frontend (`convex/audit/competitorReport.ts`) czyta te kolumny — silnik je wypełnia:

| Kolumna | Źródło z MarketResult |
|---|---|
| `treatment_name` | nazwa usługi subject |
| `subject_price_grosze`, `subject_duration_minutes` | z subjectu |
| `market_min/median/max_grosze`, `market_p25/p75_grosze` | percentyle z `normalize_unit` |
| `deviation_pct`, `deviation_pct_per_min` | z `normalize_unit` |
| `sample_size` | `n_unique_salons` |
| `recommended_action` | z deviation (raise/lower/hold) |
| `verification_status` | z `status` (+ extreme flag) |
| `comparison_tier` | `"identity"` |
| `competitor_samples` (JSONB) | tożsamy klaster (per-salon) — drill-down |

Ziarno: **wiersz per usługa subjectu** (nie per variant taksonomii jak stary). Każda
realna usługa dostaje porównanie ze swojego tożsamego klastra.

## 9. Pakiety i drill-down (PUNKT ROZSZERZENIA)

**Stan obecny:** pakiety (`is_package=TRUE`) są:
1. Wykrywane przy ingest (`scripts/ingest_salon_jsons.py::_detect_service_is_package` +
   mig 142/143 z `salon_scrape_service_variants`).
2. Wycinane z głównego klastra przez oś `package` (twarde weto — pakiet ≠ pojedyncza usługa),
   żeby „Presoterapia 5 zabiegów 700zł" nie zawyżała ceny pojedynczej presoterapii.

**Jak dodać wyświetlanie cen pakietowych po kliknięciu (to czego potrzebujesz):**
Pakiety NIE giną — `fn_find_related_v2` zwraca je z flagą `is_package=TRUE` w surowym
klastrze (`raw_samples`). Są tylko odfiltrowane z liczenia ceny. Żeby pokazać je w drill-down:

1. W miejscu wołania silnika zbierz pakietowych bliźniaków z `raw_samples` PRZED filtrem
   tożsamości: `package_samples = [s for s in raw_samples if s["is_package"]]`.
2. Zapisz je do osobnego pola (np. nowa kolumna JSONB `package_samples` na
   `competitor_pricing_comparisons`, analogicznie do `competitor_samples`).
3. Frontend renderuje je w rozwijanym panelu „ceny pakietowe u konkurencji".

`raw_samples` ma wszystko czego potrzeba per pakiet: `salon_name`, `service_name`,
`price_grosze`, `duration_minutes`, `similarity`. Nie trzeba dodatkowego zapytania.

> UWAGA przy rozszerzaniu: NIE usuwaj osi `package` z testu tożsamości (to ona chroni
> główną cenę przed zawyżeniem). Pakiety to OSOBNY widok, nie część głównego porównania.

## 10. Konfiguracja (meta-pokrętła — `DEFAULT_CONFIG`)

| Klucz | Domyślnie | Znaczenie |
|---|---|---|
| `dedup_strategy` | `"closest"` | reprezentant salonu: max similarity / median ceny |
| `min_salons_sufficient` | 5 | próg `sufficient` |
| `min_salons_thin` | 3 | próg `thin` |
| `identity_purity_target` | 0.9 | jak tożsamy ma być klaster |
| `identity_prefer` | `"sufficiency"` | konflikt: broń liczby salonów vs czystości |

Stałe osi w `layer_identity.py`: `PRICE_RATIO_AGAINST=4.0`, `CAT_WEIGHT_GENERIC=2.0`,
`CAT_WEIGHT_SPECIFIC=0.4`, `AXIS_WEIGHTS`.

## 11. Pliki, testy, narzędzia, ograniczenia

**Pliki:** `engine.py`, `layer_identity.py` (6 osi + drugie prawo), `layer_dedup.py`,
`layer_sufficiency.py`, `layer_unit.py`, `layer_category.py` (generic detection), `layer_neutral.py`
(kategorie neutralne). Słownik obszarów: `services/body_area_taxonomy.py` (współdzielony).

**Testy:** 192 (pytest, `services/similarity_pricing/test_*.py`). `python -m pytest services/similarity_pricing/ -q`.

**Narzędzia (read-only):**
- `scripts/similarity_pricing_preview.py` — zestawienia na próbce + pula konkurentów.
- `scripts/ab_pricing_compare.py [report_id]` — A/B stary (zapisane) vs nowy silnik na realnym raporcie.

**Znane ograniczenia:**
- „Thin" częste dla specyficznych usług — to UCZCIWOŚĆ (mało tożsamych), nie bug.
- Warianty premium (ten sam obszar/czas, 2-4× cena) — świadomie nierozstrzygnięte (oś cena
  ich nie łapie poniżej 4×; „premium" bywa marketingiem tej samej usługi).
- IVFFlat na całej bazie nie wszedł (build-temp k-means 7.36M×1536 > wolny dysk). Produkcja
  używa full-scan na małej puli konkurentów. Indeks do rozważenia: partial chain-head / więcej dysku.
- Słownik obszarów ma luki w odmianach („stóp" dopełniacz nie matchuje `stop[ya]`).
