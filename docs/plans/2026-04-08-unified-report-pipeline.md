# Plan: Unified Report Pipeline + Free Tier Snapshot

Data: 2026-04-08
Status: planning → Etap 0 in progress
Branch: main

## Problem

BAGENT #1 (raport) produkuje diagnostykę i drafty nazw/opisów per usługa, ale:

1. Agenty naming/description mają escape "Pomiń usługi które mają już dobre", coverage ~35% per agent (nie wzrosło bez mechanicznej sanityzacji)
2. `topIssues` z raportu i `transformations` nie są ze sobą połączone — user widzi "10 problemów" i "131 zmian" ale nie wie która zmiana rozwiązuje który problem
3. Restrukturyzacja kategorii żyje w BAGENT #2 (cennik.py:292-375) — osobny AI call który nie widzi wniosków z naming/description agent z BAGENT #1, ma dostęp tylko do `dimension=="structure"` issues
4. MiniMax flakiness w agent loop dla kategorii sprawia że user czeka 5 min na tab "Kategorie" w wizardzie
5. Raport wygląda inteligentnie, cennik wygląda mechanicznie — split-brain między diagnozą a fixem

## Cel docelowy

BAGENT #1 (raport) staje się autorytatywnym produkujementem WSZYSTKICH decyzji AI:
- diagnostyka (score, issues, quickWins, missingSeoKeywords)
- drafty nazw i opisów (z pełną coverage, bez escape)
- category mapping (przeniesione z BAGENT #2)
- traceability: każda transformacja wskazuje issue który rozwiązuje

BAGENT #2 (cennik) staje się czysto deterministyczną finalizacją:
- load report z Supabase
- mechanicznie zaaplikuj transformations
- zaaplikuj categoryMapping (już gotowy w raporcie)
- sanitize_text (emoji, caps)
- compute diff, save do optimized_pricelists
- brak nowych AI calls, czas pipeline ~2-3s
- brak flakiness bo brak zewnętrznych API calls w krytycznej ścieżce

Przy okazji: zachować obecny behavior BAGENT #1 jako frozen snapshot pod endpointem `free_report` — żeby w przyszłości uruchomić darmowy tier bez ryzyka że ewolucja głównego pipeline'u go zepsuje.

## Etap 0: Wydzielenie free_report jako snapshot BAGENT #1

**Cel:** Frozen copy obecnej logiki BAGENT #1 jako osobny pipeline + endpoint dostępny pod `/api/audit/free_report`. Snapshot ma być byte-compatible z obecnym `/api/audit/report` ale żyć w osobnym pliku żeby nie dostawał żadnych zmian z etapów 1-3.

**Zmiany w kodzie:**
- `pipelines/free_report.py` — NEW — kopia `pipelines/report.py` z przemianowaną funkcją `run_audit_pipeline` → `run_free_report_pipeline` + nagłówek "FROZEN — do not modify, mirror of BAGENT #1 as of commit X for future free tier"
- `server.py` — dodać route `POST /api/audit/free_report` który przyjmuje ten sam payload co `/api/audit/report` (auditId, scrapedData) i wywołuje `run_free_report_pipeline` przez background task. Webhook paths bez zmian (`/api/audit/report/progress` itd. — Convex nie rozróżnia tier'a)
- `tests/test_free_report.py` — NEW — smoke test: endpoint przyjmuje payload, background task startuje, funkcja jest importowalna

**Dokumentacja:**
- `docs/free_report.md` — NEW — kontrakt API (request/response), lista co pipeline produkuje (score, transformations, topIssues, quickWins, missingSeoKeywords), czego NIE produkuje (categoryMapping, traceability), stability commitment "frozen"
- Update `CLAUDE.md` — dodać sekcję "Free tier snapshot" z linkiem do doc

**Deploy + verify:**
- Commit → push → deploy na tytan
- curl test: `POST /api/audit/free_report` z dummy payloadem → odpowiedź 202 Accepted + job ID
- tests: `pytest tests/test_free_report.py`

**Nie robimy:**
- Nowych webhook paths dla free_report (reuse existing)
- Wpięcia w Convex (to osobna decyzja produktowa na później)
- Osobnej tabeli Supabase dla free vs premium (na razie współdzielą)
- Flag w audit_reports — do rozważenia w przyszłości

## Etap 1: Category restructuring z cennik do report

**Cel:** Przesunąć agent loop kategorii z `pipelines/cennik.py` do `pipelines/report.py`, żeby BAGENT #2 stał się pure executorem.

**Zmiany w kodzie:**
- `pipelines/category_restructure.py` — NEW — ekstrakcja logiki z cennik.py:292-375. Funkcja `async def restructure_categories(client, transformed_pricelist, top_issues, audit_id, progress) -> tuple[dict[str, str], list[dict]]` zwraca `(category_mapping, category_changes)`. Input to już POCZYSZCZONY pricelist (po name+desc transformacjach) — agent widzi najlepszy kontekst.
- `pipelines/report.py` — dodać krok po `_analyze_structure`: zbudować transformed_pricelist z naming+description transformations zaaplikowanymi mechanicznie (ten sam kod co w cennik.py Step 2), następnie wywołać `restructure_categories`, dodać wyniki do `report` dict jako `categoryMapping` + `categoryChanges`
- `services/supabase.py:save_report` — dodać persystencję `category_mapping JSONB` + `category_changes` do audit_reports. Jeśli audit_reports nie ma tych kolumn, schema migration: `ALTER TABLE audit_reports ADD COLUMN category_mapping JSONB, ADD COLUMN category_changes JSONB`
- Supabase RPC `get_audit_report` — update żeby zwracał `category_mapping` + `category_changes`
- `pipelines/cennik.py` — wywalić Step 3 (category agent loop, linie 292-375). Zastąpić: `category_mapping = report_data.get("categoryMapping", {})`, `category_changes = report_data.get("categoryChanges", [])`. Reszta pipeline (Step 4 finalize, Step 5 save) bez zmian.

**Testy:**
- Regen Beauty4ever cennik przez `triggerCennikGeneration` → verify pipeline time <10s (zamiast 5 min)
- Verify że optimized_pricelists id=X ma identyczną strukturę co poprzednio (te same kategorie, te same usługi w nich)
- Smoke test jednostkowy: `restructure_categories` jako samodzielna funkcja

**Deploy:** commit → push → deploy → retrigger cennik Beauty4ever

## Etap 2: Traceability issues ↔ transformations

**Cel:** Każda transformacja pokazuje issue który rozwiązuje. Każdy issue pokazuje listę transformacji które go rozwiązują.

**Schema migration Supabase:**
```sql
ALTER TABLE audit_transformations
  ADD COLUMN caused_by_issue_id UUID NULL REFERENCES audit_issues(id) ON DELETE SET NULL;
CREATE INDEX idx_audit_transformations_caused_by ON audit_transformations(caused_by_issue_id);

ALTER TABLE audit_issues
  ADD COLUMN resolved_by_transformation_ids UUID[] NULL;
```

**Zmiany promptów:**
- `prompts/naming_agent.txt` — dodać sekcję "KONTEKST: aktywne problemy z raportu" + listę `topIssues` filtrowanych po `dimension in ["naming","structure"]` — każdy z `issueId`. Instrukcja: "dla każdej transformacji wskaż issueId który rozwiązujesz w polu causedByIssueId (lub null jeśli to poprawka niezwiązana z żadnym raportowanym issue)"
- `prompts/descriptions_agent.txt` — analogicznie dla `dimension in ["descriptions","seo"]`

**Zmiany tools:**
- `agent/tools.py` — NAMING_TOOL schema dostaje opcjonalne pole `causedByIssueId: string | null` w items.transformations. DESCRIPTION_TOOL analogicznie.

**Zmiany pipeline:**
- `pipelines/report.py` — kolejność kroków: najpierw `_analyze_structure` (żeby mieć issues), potem `_analyze_naming` i `_analyze_descriptions` WIEDZĄCE o issues. Naming i description agents dostają `issues_text` jako dodatkowy kontekst w prompt.
- Parse `causedByIssueId` z tool call output i propagate do transformation dict

**Zmiany persistence:**
- `services/supabase.py:save_report`:
  - Insert audit_issues FIRST, zbierz mapę `client_issue_id → db_uuid` (temporary mapping z indeksu)
  - Insert audit_transformations z `caused_by_issue_id` resolved z mapy
  - Po insercie update audit_issues.resolved_by_transformation_ids z listą UUID transformacji które je wskazują

**Convex side:**
- `convex/schema.ts` — jeśli jest tabela `auditTransformations` (trzeba zweryfikować, może używa się tylko Supabase) — dodać pole causedByIssueId
- `convex/auditReportStorage.ts` / podobne — update query żeby eksponowała pole

**Frontend:**
- `components/results/IssueCard.tsx` (lub odpowiednik) — counter "rozwiązane przez N transformacji" + expand list
- `components/results/TransformationCard.tsx` — tooltip / link do powiązanego issue

**Testy:**
- Full audit na nowym salonie → verify że audit_transformations mają caused_by_issue_id set
- Query "dla issue X pokaż transformations" zwraca niepusty wynik
- Frontend render testy dla nowych pól

## Etap 3: Agresywne coverage — bez "pomiń dobre"

**Cel:** Agenty naming i description iterują po WSZYSTKICH usługach, nie tylko tych wyglądających na złe. User widzi explicit report "sprawdzono 273, poprawiono 195, uznano 78 za optymalne".

**Zmiany promptów:**
- `prompts/naming_agent.txt` — wywalić linię "Pomiń usługi które mają już dobre nazwy". Dodać: "dla KAŻDEJ usługi z cennika zwróć wynik przez submit_naming_results. Pole `already_optimal: true` jeśli nazwa nie wymaga poprawy (wtedy `improved` może być == `name`). Pole `already_optimal: false` jeśli proponujesz zmianę."
- `prompts/descriptions_agent.txt` — analogicznie

**Zmiany tools:**
- `agent/tools.py` — NAMING_TOOL items.transformations: dodać pole `already_optimal: boolean`. DESCRIPTION_TOOL analogicznie.

**Zmiany pipeline:**
- `pipelines/report.py` `_analyze_naming` — parse `already_optimal`. Transformacja tworzona TYLKO gdy `already_optimal == false` AND `improved != original` AND passes validation. Zliczanie: `total_checked`, `optimized_count`, `already_optimal_count`, `rejected_count`.
- `_analyze_descriptions` — analogicznie
- Return `coverage_stats` w wynikach obu funkcji. Dołącz do `report["stats"]["naming_coverage"]` i `report["stats"]["descriptions_coverage"]`.

**Frontend:**
- `components/results/NamingScoreCard.tsx` — pokazać "Agent sprawdził X usług: poprawił Y, uznał Z za optymalne, odrzucił W nieudanych prób"
- Tak samo dla descriptions

**Testy:**
- Regen raport na Beauty4ever → verify `total_checked == 273` dla obu agentów, verify `optimized + already_optimal == total_checked`, verify że agent loop nie skończył się wcześniej niż po przetworzeniu całości

**Uwaga cost:** Etap 3 zwiększy tokeny per audyt bo agent musi każdą usługę uzasadnić. Estymata: +50% tokenów w BAGENT #1 naming + descriptions (z ~35% coverage do 100%). Trzeba zweryfikować w praktyce i ewentualnie batchować agresywniej (30 usług per tool_call zamiast 15).

## Kolejność wykonania i dependencies

```
Etap 0 (extraction) — niezależny, robimy pierwszy
  ↓
Etap 1 (category restructuring → report) — zmienia report.py, ale w sposób który nie dotyka free_report.py (bo free_report to frozen copy sprzed Etap 1)
  ↓
Etap 2 (traceability) — zmienia report.py, tools, schema, frontend. Zależy od Etap 1 bo operuje na report w którym jest już categoryMapping
  ↓
Etap 3 (coverage) — zmienia prompty, tools, report.py. Niezależny od Etap 2 (można robić równolegle), ale najlepiej po Etap 2 żeby agenty już wiedziały o issues przy wyrównywaniu coverage
```

Każdy etap to osobny commit (lub seria commits) + deploy + regresja na Beauty4ever przed przejściem do następnego.

## Kryteria sukcesu

- **Etap 0:** `curl -X POST https://bagent.booksyaudit.pl/api/audit/free_report` działa, testy pass, `docs/free_report.md` istnieje, CLAUDE.md linkuje do niego.
- **Etap 1:** Cennik pipeline dla Beauty4ever trwa <10s. Retrigger kategorii jest idempotentny i nie wymaga nowych AI calls. Output jest equivalent do poprzedniej wersji.
- **Etap 2:** Query "SELECT * FROM audit_transformations WHERE caused_by_issue_id IS NOT NULL" zwraca >0 wierszy dla nowego audytu. Frontend Raport tab pokazuje "rozwiązane przez N transformacji" na issue cards.
- **Etap 3:** Dla Beauty4ever naming agent raportuje `total_checked == 273`. Coverage_stats.already_optimal + coverage_stats.optimized == 273.
