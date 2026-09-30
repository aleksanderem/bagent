> Kopia (30.09.2026) — źródło wstrzykiwane hookiem: `BEAUTY_AUDIT/.claude/zasady-przed-zadaniem.md`. Ustalenia z Alexem dla matchingu.


# ZASADY PRZED KAŻDYM ZADANIEM — sam sprawdź plan, zanim cokolwiek zaczniesz

Wstrzykuje to hook przy każdej wiadomości Alexa i po każdym starcie sesji. Cel (Alex, 28.09.2026): masz SAM
wyłapać odstępstwo od ustaleń, zanim zaczniesz — nie czekać, aż Alex je wytknie, i nie przerzucać na niego
sprawdzania pytaniami. Twarde bramki z CLAUDE.md obowiązują bez zmian.

## 1. Kontrola przed startem — każde zadanie i każdy nowy krok
1. Przeczytaj TERAZ z plików ustalenia, których krok dotyczy (p. 4) — nie odtwarzaj ich z pamięci.
2. Rozpisz plan na kroki i przyłóż każdy krok do ustaleń (p. 3) i do listy moich odchyleń (p. 2).
3. Krok niezgodny → POPRAW go sam tak, żeby był zgodny, i dopiero wtedy ruszaj. Nie pytaj o to Alexa.
4. Alexa pytasz tylko, gdy zgodnego kroku nie da się zrobić bez zmiany samego ustalenia, albo przy:
   decyzji produktowej (co jest „tą samą usługą”), koszcie > 5 USD za przebieg, zmianie celu, destrukcji
   lub zapisie na produkcji. Pytanie bez odpowiedzi = nie robisz tego, o co pytałeś; robisz wersję zgodną.
5. Przed oddaniem przyłóż wynik jeszcze raz do ustaleń. Odstępstwo popraw przed raportem; jeśli się nie da,
   wypisz je wprost na początku raportu.

## 2. Moje odchylenia z ostatnich dni — jeśli plan zawiera któreś, to dryf
- własna lista (dziedziny, kategorie, opcje) zamiast etykiet wybranych z danych Booksy;
- kategorie Booksy jako poziom drzewa albo podstawa podziału;
- podział jednego poziomu według dwóch zasad naraz (czynność i część ciała, np. „masaż” obok „twarz”) —
  rodzeństwo zaczyna się nakładać: 28.09 moja lista dziedzin rozcięła 68 z 206 zabiegów na kilka gałęzi;
- coś zamiast drzewa: równoległe pytania, reguła w kodzie, regex, próg dopasowany do danych, reguła na nazwach;
- nowy poziom, wymiar albo inna kolejność poziomów niż w modelu Alexa;
- łatka pod jedną branżę zamiast poprawy węzła drzewa;
- inny cel niż ustalony: cena zamiast pokrycia taksonomii; raport przed destylacją i matchingiem;
- myślenie jak z LLM: jedno szerokie pytanie, „etykieta z listy do porównania”, HTTP zamiast SDK,
  pytania TypeSafe pisane bez przeczytania pasującego cookbooka;
- pomiar na danych już użytych, wąska sieć kandydatów, brak bilansu z poprzednią wersją na tych samych salonach;
- przerzucanie oceny wyników na Alexa („przejrzyj próbkę”, „oznacz złe werdykty”) — pary oceniam SAM według
  modelu tej samej usługi i podaję liczby (Alex, 28.09); Alex rozstrzyga tylko decyzje produktowe;
- pełny przebieg bez próby na ≤ 40 usługach obejrzanej oczami;
- wniosek z liczb, których pochodzenia nie sprawdziłem (28.09: „TypeSafe uznał masaże za różne” z zastępczych
  zer nieudanych pytań) — najpierw sprawdź, czy wynik jest prawdziwą odpowiedzią i czy przebieg jest powtarzalny.

## 3. Matching — ustalenia niezmienne (skrót)
- NOWY KIERUNEK ZA ZGODĄ ALEXA (29.09, zatwierdzony plan `~/.claude/plans/precious-imagining-metcalfe.md`):
  katalog usług + podpis oferty (hybryda). Oferta = usługa albo wariant z własną ceną; cechy wyciąga GLM (Z.ai,
  abonament) raz na znormalizowany tekst oferty, w JSON, frazy z tekstu; słownik cech z danych (grupy + TypeSafe,
  relacja to samo / węższe / szersze / inne); „ta sama” = równy podpis, różnica jednostronna rozstrzygana RAZ na
  klasę różnicy (zabieg, cecha, wartość) przez TypeSafe z pamięcią; w raporcie zero wywołań modelu. Dozwolone:
  proste czyszczenie tekstu w kluczu, paczki ofert dla GLM (po teście), zabieg Booksy jako kontekst. Modele:
  tylko Z.ai i MiniMax (bez GPT), TypeSafe do krótkich tak/nie/Score. Cel „ta sama” ≥ 95% trafności.
  Poniższe punkty o drzewie TypeSafe dotyczą v14f, który zostaje punktem odniesienia w pomiarach.
- ODSTĘPSTWA PRZYJĘTE PRZEZ ALEXA (29.09 wieczór, „ok”): (1) „ta sama” porównuje zbiór słów oferty, nie wartości
  ról; (2) słowo nieprzypisane albo oddane do szumu (gdy na rynku jest cechą) wchodzi do zbioru; (3) ujednolicanie
  słów w kodzie (bez polskich znaków, ≤ 2 końcówki) + krótkie listy (słowa bez treści, znaki listy w kategorii),
  progi z rozumowania (≤ 2 słowa w zamianie, 0,8 „to samo”); (4) skład z opisu się nie liczy (tylko wyłączenia
  i liczby); (5) rodzaj różnicy = wszystkie wspólne słowa pary; (6) relacja słów ma piątą odpowiedź „nie wiadomo”;
  (7) kategoria cennika rozkładana osobno, raz na tekst. Zgoda na ~4 USD: pełny przebieg v14f w sprawdzianie 7.
- DECYZJA ALEXA (30.09, „Najpierw odzysk”): suchy przebieg — podpis „ta sama” w 19–35% wierszy vs stary silnik 85%.
  Przed wpięciem do raportów: kaskada z planu (domyślne zabiegu, opisowe, dopełnienie menu), cel ~50% wierszy
  z ceną „ta sama” przy ≥ 95% trafności, dowód na NOWYCH salonach (sprawdzian 9); wpięcie dopiero potem.
- Drzewo jak w cookbooku TypeSafe hierarchical_classification DO POZIOMU ZABIEGU: każdy poziom to wybór spośród
  dzieci poprzedniego, rodzeństwo się nie nakłada, wiązka 3 ścieżek, średnia geometryczna krawędzi; zabieg może
  mieć kilku rodziców (graf jak MeSH w cookbooku).
- ZMIANA ZA ZGODĄ ALEXA (28.09, warunkowa): poniżej zabiegu (metoda, gdzie i ile, etap, skład) NIE wybieramy z listy
  opcji — porównujemy wprost frazy obu usług wyciągnięte przez TypeSafe z ich własnych słów; czy dwie różne frazy
  znaczą to samo, rozstrzyga TypeSafe tak/nie. Warunek Alexa: ma działać uniwersalnie, poprawnie wyciągać
  taksonomię i poprawnie dopasowywać — jeśli pomiar tego nie pokaże, mówię to wprost.
- Etykiety węzłów pochodzą z danych Booksy — TypeSafe wybiera, nie wymyśla; liczby liczy kod.
- „Ta sama / podobna / inna” rozstrzyga drzewo: ten sam liść / wspólny przodek / różne gałęzie.
- Model tej samej usługi: dziedzina → zabieg → metoda → gdzie i ile → etap, plus skład (zestaw = zbiór).
  Pełny kontekst z Booksy: kategoria, nazwa, opis, warianty, zabieg Booksy, salon. Czas trwania nie
  rozróżnia. Dodatek w nazwie = podobna. Produktów i dodatków rezerwowanych obok nie porównujemy.
- Sędzia par wyłączony; miary: pokrycie, spójność i MOJA ocena próbki par według modelu (Alex, 28.09 — nie jego przegląd). Jedna usługa na wywołanie modelu.
- Błędy liczy się na węzłach we wszystkich branżach naraz; o zmianie węzła decyduje TypeSafe, nie łatka.
- Wektory tylko podsuwają kandydatów, szeroko; decyduje drzewo.

## 4. Gdzie są pełne ustalenia
- Matching: memory `feedback_typesafe_cookbook_drzewo.md`, `feedback_model_tej_samej_uslugi.md`; dokument
  „Matching usług — stan”: Zasada niezmienna, Model tej samej usługi, Założenia i stan, Decyzje, Odrzucone.
- TypeSafe: skill `typesafe:typesafe-ai`, `reference_typesafe_design_rules.md`, `feedback_typesafe_myslenie.md`.
- Kolejność prac: `feedback_typesafe_priorytety.md`. Tempo: `feedback_dzialaj_bez_pytania.md` — w zatwierdzonym
  kierunku działasz sam do końca planu.

## 5. Płatne uruchomienia (TypeSafe)
Hook `scripts/hooks/typesafe-platne-gate.sh` zatrzymuje każde uruchomienie. Przejście: kontrola z p. 1–2,
jedna linijka w `.claude/preflight.log` (co, koszt z pomiaru i limit w komendzie, sprawdzone ustalenia,
odstępstwa: brak), potem komenda z `ZASADY_OK=1` na początku. Hook przepuszcza tylko przy wpisie młodszym
niż 15 minut.

## Utrzymanie
Nowe ustalenie z Alexem albo odchylenie, które wyłapał → dopisz tutaj w tej samej turze.
