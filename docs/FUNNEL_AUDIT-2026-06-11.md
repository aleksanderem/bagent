# bagent — Audyt lejka sprzedażowego (2026-06-11)

> **STATUS WDROŻENIA (2026-06-11, ta sama sesja):** ✅ R1 (services/product_context.py:
> wierzchołki z Supabase + blok promptowy; wpięty w BAGENT #3 summary i syntezę
> konkurencji przez placeholder {product_context}), ✅ R2 (stopka next-step w
> run_free_report_task — wrapper, zamrożony pipeline nietknięty), ✅ R3 (CTA w BAGENT #3
> wybiera BRAKUJĄCY wierzchołek; komplet → konsultacja; wcześniej hardcodowany upsell
> raportu nawet dla jego posiadaczy), ✅ R4 (outreach: salon_name/salon_city/desc_gap_pct
> dociągane z salons→salon_scrapes→services, best-effort), ✅ P1-3 przy okazji (wzór
> obowiązkowy dla estimatedRevenueImpactGrosze + zakaz prostych cudzysłowów + zero
> powtórzeń między sekcjami w competitor_synthesis.txt), ✅ R5 (2026-06-12:
> campaign_setup generuje copy reklam z wniosków audytu — extract_audit_insights +
> 1 call LLM per proposal, copySource audit_llm|template per kreacja, HEALTH zawsze
> na szablonie compliance-safe, fallback na szablon przy każdej porażce).
> Otwarte: R6 (funnel_events).

> Zasada nadrzędna (właściciel, 2026-06-11): BooksyAudit to JEDNA maszyna, a model sprzedażowy
> to **TRÓJKĄT trzech RÓWNOWAŻNYCH produktów** — (1) audyt profilu z optymalizacją AI,
> (2) raport konkurencji z wnioskami, (3) monitoring konkurencji. **Nie ma kolejności ani
> hierarchii**: każdy wierzchołek może być pierwszym zakupem i każdy można dosprzedać z każdego
> innego, w obu kierunkach. Każda generowana treść ma być świadoma pozostałych wierzchołków:
> referować zamiast powtarzać, komunikować brakujące wierzchołki merytorycznie (przy konkretnym
> wniosku), a całość prowadzi do CENTRUM trójkąta — PŁATNEJ KONSULTACJI, docelowo do usług
> marketingu internetowego właściciela. (W produkcie istnieje już ten język: UpsellLadder
> renderuje „brakujące wierzchołki trójkąta" — komentarz w BEAUTY_AUDIT convex/schema.ts.)

## 1. Stan obecny: gdzie system generuje treść dla klienta/leada i czy zna resztę maszyny

| Miejsce | Świadomość innych modułów | Upsell możliwości | CTA | Ocena |
|---|---|---|---|---|
| naming/descriptions/structure prompts (audyt) | brak | brak | brak | 🔴 silos |
| summary.txt (podsumowanie audytu, 2-3 zdania) | brak | brak | brak | 🔴 silos |
| audit_summary_applied/pending.txt (BAGENT #3) | częściowa (wspomina cennik + analizę konkurencji) | sugeruje, bez konkretu | miękkie, bez linku | 🟡 |
| competitor_synthesis.txt (raport konkurencji) | brak (nie wie o audycie/monitoringu/kampaniach) | brak | brak | 🔴 silos |
| deterministyczne sekcje raportu konkurencji (actionPlan 14 dni, short/longStrategy) | brak | brak | brak | 🔴 |
| alerty monitoringu (scrape_refresh → Convex ingest) | brak (czyste fakty; interpretacja AI jest już PO STRONIE BEAUTY_AUDIT od S0063) | brak | brak | 🟡 (naprawiane z drugiej strony) |
| outreach (szablony e-mail z repo BEAUTY_AUDIT) | w gestii copywritera; ZMIENNE personalizacji salon_name/salon_city/desc_gap_pct są dziś PUSTE (TODO w outreach_orchestrator) | j.w. | j.w. | 🟠 zepsuta personalizacja |
| Meta Ads copy (campaign_setup: _build_headline/_build_body) | brak — szablon "Zarezerwuj X w Y. Już od Z zł." | nie dotyczy (treść dla klientek salonu) | "Zarezerwuj na Booksy" | 🟡 nie wykorzystuje wniosków audytu |
| kreacje graficzne (openai_image + brand overlay) | n/d | n/d | CTA pill | 🟢 |
| free report (zamrożony) | brak | brak | brak | 🔴 lead magnet bez następnego kroku! |

**Diagnoza:** każdy moduł mówi osobno. Lead magnet (free report) i oba płatne raporty nie prowadzą
nigdzie dalej; outreach wysyła maile z pustymi zmiennymi personalizacji; kampanie Meta nie znają
wniosków audytu, za który klient zapłacił.

## 2. Rekomendacje (warstwa bagent)

### R1. Wspólny „kontekst maszyny" wstrzykiwany do KAŻDEGO promptu generującego treść
Jeden moduł (np. `services/product_context.py`) budujący krótki blok: jakie moduły klient MA
(audyt? raport konkurencji? monitoring? optymalizacja zastosowana?), jakich NIE MA, plus reguły:
- nie powtarzaj wniosków modułu, który klient ma — REFERUJ ("szczegóły w raporcie konkurencji"),
- gdy precyzję dałby moduł niedokupiony — powiedz to przy konkretnym wniosku (wzorzec banerów
  strategii w BEAUTY_AUDIT S0063: merytorycznie, przy temacie, nie nachalnie),
- każdy deliverable kończy się 1 krokiem „co dalej" prowadzącym w dół lejka.
Wejście do promptów: summary.txt, audit_summary_*, competitor_synthesis.txt (+ structure.txt quickWins).

### R2. Free report = wejście do trójkąta, nie ślepa uliczka
Zamrożony pipeline pozostaje zamrożony — ale finalny assembling może dokładać DETERMINISTYCZNĄ
stopkę (poza zamrożonym plikiem, na etapie zapisu/prezentacji), dobraną do znalezisk:
problemy profilu → „Pełny audyt + optymalizacja AI naprawia je za Ciebie"; sygnały rynkowe
(ceny odstające, luki vs okolica) → raport konkurencji; dynamiczny rynek → monitoring.
Zero zmian w free_report.py.

### R3. Trójkąt cross-sell (NIE drabina) + konsultacja w centrum
Trzy produkty są równoważne — komunikacja per deliverable:
- **z audytu**: tam, gdzie wniosek dotyczy rynku/cen → wskaż raport konkurencji; tam, gdzie
  dotyczy zmienności (ceny/oferta konkurentów w czasie) → wskaż monitoring;
- **z raportu konkurencji**: wnioski o brakach profilu → audyt z optymalizacją; wnioski
  „rynek się rusza" → monitoring;
- **z monitoringu**: alerty cenowe/ofertowe → raport konkurencji (pełny obraz rynku); alerty
  o słabościach własnego profilu względem ruchów konkurencji → audyt.
Zawsze promuj WIERZCHOŁKI BRAKUJĄCE temu klientowi (product_context z R1), przy konkretnym
wniosku, bez sugerowania kolejności. **Centrum trójkąta = płatna konsultacja** — CTA domyślne
w: podsumowaniu audytu (BAGENT #3), rekomendacjach raportu konkurencji, digestach monitoringu
(po stronie BEAUTY_AUDIT insight już jest — dodać CTA konsultacji przy priorytecie act),
a zwłaszcza gdy klient ma KOMPLET wierzchołków (wtedy jedyny upsell to konsultacja/marketing).
Free report prowadzi do dowolnego wierzchołka zależnie od znalezisk (nie tylko do audytu).

### R4. Napraw personalizację outreach (przedwarunek skuteczności lejka)
TODO w outreach_orchestrator (join salon_name/salon_city/desc_gap_pct z salons + diff snapshot) —
bez tego maile cold wychodzą generyczne. To pojedynczy join + cache na contact.attributes.

### R5. Meta Ads copy z wniosków audytu
`campaign_setup.setup_campaign_proposal` przyjmuje audit_id, ale go ignoruje. Podpiąć: insighty
audytu (pozycja cenowa, wyróżniki, brakujące usługi) → generacja 2-3 wariantów copy przez LLM
zamiast szablonu. Kampania staje się przedłużeniem raportu, za który klient zapłacił —
i naturalnym pomostem do usług marketingowych właściciela.

### R6. Pomiar lejka
Jednolite UTM-y już są (outreach_utm_source, kampanie). Brakuje: zdarzeń przejść między modułami
(free→audyt, audyt→raport, raport→monitoring, →konsultacja) w jednym miejscu. Minimalnie: tabela
funnel_events w Supabase zasilana z Convex (zakupy) + outreach (kliknięcia) + kampanie (atrybucje).

## 3. Granica odpowiedzialności
Interpretacja alertów, Strategia Salonu, banery „czego nie masz" w UI — już wdrożone PO STRONIE
BEAUTY_AUDIT (S0063). Ten dokument dotyczy warstwy bagent: promptów raportów, free reportu,
outreach i kampanii. Wdrożenie R1-R6 wymaga zmian w TYM repo (osobny deploy).
