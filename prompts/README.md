# prompts/ — status plików (FINDINGS P2, 2026-06-11)

Każdy plik ładowany jest przez `_load_prompt(name)` w pipeline'ach. Zanim
edytujesz prompt, sprawdź kolumnę "używany przez" — dwa pliki są MARTWE,
a trzy żyją wyłącznie w zimnym kodzie `pipelines/_premium/`.

| Plik | Używany przez | Status |
|---|---|---|
| naming_score.txt | pipelines/report.py `_score_naming` | aktywny (BAGENT #1) |
| descriptions_score.txt | pipelines/report.py `_score_descriptions` | aktywny (BAGENT #1) |
| structure.txt | pipelines/report.py `_analyze_structure` | aktywny (BAGENT #1) |
| summary.txt | pipelines/report.py `_generate_summary` | aktywny (BAGENT #1) |
| naming_agent.txt | pipelines/report.py `_agent_naming` | aktywny (BAGENT #1) |
| descriptions_agent.txt | pipelines/report.py `_agent_descriptions` | aktywny (BAGENT #1) |
| optimization_categories.txt | pipelines/category_restructure.py | aktywny (BAGENT #2) |
| optimization_services.txt | pipelines/cennik.py | aktywny (BAGENT #2) |
| audit_summary_applied.txt | pipelines/summary.py | aktywny (BAGENT #3) |
| audit_summary_pending.txt | pipelines/summary.py | aktywny (BAGENT #3) |
| competitor_synthesis.txt | pipelines/competitor_synthesis.py | aktywny (raport konkurencji) |
| competitor_market.txt | TYLKO pipelines/_premium/competitor.py | zimny — żaden aktywny endpoint nie woła |
| competitor_swot.txt | TYLKO pipelines/_premium/competitor.py | zimny — j.w. |
| competitor_strategy.txt | TYLKO pipelines/_premium/competitor.py | zimny — j.w. |
| optimization_seo.txt | NIC (zweryfikowane grep 2026-06-11) | MARTWY — kandydat do usunięcia |
| optimization_content.txt | NIC (zweryfikowane grep 2026-06-11) | MARTWY — kandydat do usunięcia |

Martwych plików celowo nie usuwamy bez zgody właściciela — mogą wrócić
przy reaktywacji `_premium/`. Jeśli reaktywujesz `_premium/`, zaktualizuj
tę tabelę.

Reguły wspólne dla promptów AI (po incydentach z 2026-06-11):
- zakaz prostych cudzysłowów `"` w treści generowanej (psują JSON) — pisać «...»,
- wyłącznie polski (MiniMax M2.7 potrafi wtrącać chińskie tokeny),
- liczby tylko z danych wejściowych (anty-halucynacja),
- odpowiedzi JSON przechodzą przez `services/json_repair.py` — ale prompt
  ma nie polegać na naprawie.
