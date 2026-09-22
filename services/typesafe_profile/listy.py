"""Kanoniczne listy dla badania usługi (services/typesafe_profile/pytania.py).

Okolice ciała: rozłączne części (każda to osobne pytanie tak/nie). Metody:
lista technik per branża salonu — jedyne miejsce, gdzie pytamy o wybór z listy.

Pochodzenie (21.09.2026): rozkład wartości, które GLM wpisał do service_taxonomy
dla 6 branż holdoutu (mig 188), plus Medycyna Estetyczna i Makijaż dopisane
z wiedzy dziedzinowej PRZED obejrzeniem etykiet partii walidacyjnej.
Zasada: scalane są tylko warianty NAZWY tej samej techniki; techniki różne
cenowo zostają osobno (balejaż ≠ farbowanie, keratyna ≠ nanoplastia), bo weto
ma rozdzielać usługi w różnych cenach.

Zmiana treści listy zmienia odpowiedzi modelu — podbij WERSJA w pytania.py,
żeby stare profile nie mieszały się z nowymi.
"""

from __future__ import annotations

# Części ROZŁĄCZNE (v2, 21.09): w v1 „twarz” wychodziła ~0,88 przy każdej usłudze
# koło oczu, więc rzęsy i brwi zawsze miały część wspólną i weto na okolicach nie
# działało. Tam, gdzie części sąsiadują, druga wartość krotki mówi, czego NIE liczyć
# (trafia do kryterium „false” pytania tak/nie).
OBSZAR: dict[str, str | tuple[str, str]] = {
    "wlosy": ("włosy na całej głowie", "tylko boki lub tył głowy; tylko broda"),
    "boki_glowy": ("tylko boki lub tył głowy, np. podgolenie boków", "strzyżenie całej głowy"),
    "skora_glowy": ("skóra głowy, np. peeling skóry głowy, trychologia", "zwykłe strzyżenie, farbowanie lub stylizacja włosów"),
    "broda": "broda lub zarost",
    "skora_twarzy": ("skóra całej twarzy, np. oczyszczanie, peeling, masaż twarzy", "zabieg tylko na brwiach, rzęsach, ustach, nosie lub uszach"),
    "okolice_oczu": ("skóra wokół oczu lub powieki", "same rzęsy albo same brwi"),
    "usta": "usta lub wargi",
    "nos": "nos",
    "uszy": "uszy lub płatki uszu",
    "szyja": "szyja lub kark",
    "dekolt": "dekolt",
    "brwi": "brwi",
    "rzesy": "rzęsy",
    "plecy": "plecy",
    "brzuch": "brzuch lub boczki",
    "rece": ("ramiona lub przedramiona", "same dłonie albo same paznokcie"),
    "dlonie": ("skóra dłoni, np. zabieg na dłonie, parafina", "same paznokcie dłoni"),
    "paznokcie_dloni": "paznokcie dłoni",
    "paznokcie_stop": "paznokcie stóp",
    "stopy": ("skóra stóp lub pięty", "same paznokcie stóp"),
    "nogi": "nogi lub uda",
    "posladki": "pośladki",
    "pachy": "pachy",
    "bikini": "bikini lub okolice intymne",
    "cale_cialo": "całe ciało",
    "jezyk": "język",
    "pepek": "pępek",
    "sutki": "sutki",
    "zeby": "zęby",
    "siersc_zwierzecia": "sierść zwierzęcia",
}

METODA: dict[str, dict[str, str]] = {
    "Barber shop": {
        "strzyzenie_wlosow": "strzyżenie włosów (nożyczkami lub maszynką)",
        "strzyzenie_brody": "strzyżenie, trymowanie lub konturowanie brody",
        "golenie": "golenie brzytwą lub maszynką na gładko (głowy albo twarzy)",
        "odsiwianie": "odsiwianie, repigmentacja lub kamuflaż siwizny",
        "koloryzacja": "koloryzacja lub farbowanie włosów",
        "modelowanie": "modelowanie lub stylizacja fryzury",
        "pielegnacja": "mycie, pielęgnacja lub masaż głowy",
        "depilacja_woskiem": "depilacja woskiem (nos, uszy, twarz)",
        "depilacja_laserowa": "depilacja laserowa",
        # v2: barberzy robią też usługi fryzjerskie; bez nich keratyna i trwała
        # wpadały obie w „inna” i weto na metodzie milczało.
        "keratyna": "keratynowe prostowanie",
        "trwala": "trwała ondulacja",
        "balejaz_ombre": "balejaż, ombre lub air touch",
        "pasemka_refleksy": "pasemka lub refleksy",
        "dekoloryzacja": "dekoloryzacja lub rozjaśnianie",
        "regeneracja": "regeneracja lub kuracja włosów",
        "przedluzanie_wlosow": "przedłużanie włosów",
    },
    "Fryzjer": {
        "strzyzenie": "strzyżenie włosów",
        "koloryzacja_globalna": "koloryzacja lub farbowanie całych włosów",
        "odrost": "farbowanie samego odrostu",
        "balejaz_ombre": "balejaż, ombre, sombre lub air touch",
        "pasemka_refleksy": "pasemka lub refleksy",
        "tonowanie": "tonowanie",
        "dekoloryzacja": "dekoloryzacja lub rozjaśnianie",
        "modelowanie": "modelowanie, suszenie lub stylizacja",
        "upiecie": "upięcie lub fryzura okolicznościowa",
        "trwala": "trwała ondulacja",
        "keratyna": "keratynowe prostowanie",
        "nanoplastia": "nanoplastia",
        "botoks_wlosy": "botoks na włosy",
        "regeneracja": "regeneracja, kuracja lub pielęgnacja włosów",
        "przedluzanie_wlosow": "przedłużanie lub zagęszczanie włosów",
        "strzyzenie_brody": "strzyżenie lub trymowanie brody",
        "pielegnacja_zwierzat": "strzyżenie, kąpiel lub pielęgnacja zwierzęcia",
    },
    "Paznokcie": {
        "manicure_klasyczny": "manicure klasyczny",
        "manicure_hybrydowy": "manicure hybrydowy",
        "stylizacja_zelowa": "stylizacja żelowa, akrylowa lub akrylożelowa",
        "wzmocnienie_plytki": "wzmocnienie naturalnej płytki (żel lub baza)",
        "pedicure_klasyczny": "pedicure klasyczny",
        "pedicure_hybrydowy": "pedicure hybrydowy",
        "podologia": "pedicure podologiczny lub zabieg podologiczny",
        "zdobienie": "zdobienie paznokci",
        "naprawa": "naprawa paznokcia",
        "zabieg_na_dlonie": "zabieg pielęgnacyjny na dłonie (np. parafina)",
        "przedluzanie_rzes": "przedłużanie rzęs",
        "stylizacja_brwi": "henna, laminacja lub regulacja brwi",
    },
    "Salon Kosmetyczny": {
        "peeling_chemiczny": "peeling chemiczny lub kwasy",
        "peeling_inny": "peeling kawitacyjny, enzymatyczny lub mechaniczny",
        "oczyszczanie": "oczyszczanie twarzy (manualne, wodorowe)",
        "mezoterapia_iglowa": "mezoterapia igłowa",
        "mezoterapia_mikroiglowa": "mezoterapia mikroigłowa (np. Dermapen)",
        "radiofrekwencja": "radiofrekwencja lub fala radiowa (także mikroigłowa)",
        "stymulator_tkankowy": "stymulator tkankowy",
        "toksyna_botulinowa": "toksyna botulinowa",
        "kwas_hialuronowy": "wypełniacz z kwasem hialuronowym",
        "laser": "zabieg laserowy (frakcyjny, naczynka, fotoodmładzanie)",
        "depilacja_laserowa": "depilacja laserowa",
        "depilacja_woskiem": "depilacja woskiem lub pastą cukrową",
        "przedluzanie_rzes": "przedłużanie rzęs",
        "laminacja": "laminacja rzęs lub brwi",
        "henna_regulacja": "henna lub regulacja brwi",
        "makijaz_permanentny": "makijaż permanentny",
        "zabieg_pielegnacyjny": "zabieg pielęgnacyjny lub nawilżający na twarz",
        "masaz": "masaż",
        "manicure_pedicure": "manicure lub pedicure",
        "modelowanie_sylwetki": "modelowanie sylwetki (endermologia, kriolipoliza, HIFU na ciało)",
    },
    "Tatuaż i Piercing": {
        "tatuaz": "wykonanie tatuażu",
        "usuwanie_tatuazu": "usuwanie tatuażu",
        "przeklucie": "przekłucie (piercing)",
        "bizuteria": "wymiana, skrócenie lub zdjęcie biżuterii",
        "konsultacja": "konsultacja",
        "tooth_gems": "ozdoby na zęby",
        "microdermal": "microdermal",
        "makijaz_permanentny": "makijaż permanentny",
        "rzesy_brwi": "przedłużanie, laminacja lub henna rzęs i brwi",
    },
    # Dwie branże spoza holdoutu — listy dopisane 21.09 z wiedzy dziedzinowej,
    # ZANIM zobaczyłem etykiety nowej partii walidacyjnej (bez strojenia pod dane).
    "Medycyna Estetyczna": {
        "toksyna_botulinowa": "toksyna botulinowa (botoks)",
        "kwas_hialuronowy": "wypełniacz z kwasem hialuronowym (usta, zmarszczki, modelowanie twarzy)",
        "stymulator_tkankowy": "stymulator tkankowy lub biostymulator (np. Profhilo, Sculptra, Jalupro, Nucleofill)",
        "mezoterapia_iglowa": "mezoterapia igłowa",
        "mezoterapia_mikroiglowa": "mezoterapia mikroigłowa lub Dermapen",
        "osocze": "osocze bogatopłytkowe (PRP) lub fibryna",
        "nici": "nici liftingujące",
        "lipoliza": "lipoliza iniekcyjna",
        "hialuronidaza": "rozpuszczanie kwasu hialuronowego",
        "peeling_chemiczny": "peeling chemiczny",
        # Walidacja 21.09: „Peeling węglowy BLACK DOLL” szedł w peeling_chemiczny
        # (0,86), a to zabieg laserem Nd:YAG — słowo „peeling” ciągnęło w złą stronę.
        "laser": "zabieg laserowy (frakcyjny, CO2, Nd:YAG, naczynka, zmiany skórne, peeling węglowy / Black Doll)",
        "hifu_rf": "HIFU lub radiofrekwencja",
        "wlew": "wlew dożylny lub kroplówka",
        "konsultacja": "konsultacja lekarska",
    },
    "Makijaż": {
        "makijaz_okolicznosciowy": "makijaż dzienny, wieczorowy lub okolicznościowy",
        "makijaz_slubny": "makijaż ślubny",
        "makijaz_permanentny": "makijaż permanentny (brwi, usta, kreska)",
        "usuwanie_pmu": "usuwanie makijażu permanentnego",
        "lekcja": "lekcja lub nauka makijażu",
        "stylizacja_brwi": "henna, laminacja lub regulacja brwi",
        "rzesy": "przedłużanie, lifting lub laminacja rzęs",
    },
    "Masaż": {
        "klasyczny": "masaż klasyczny",
        "relaksacyjny": "masaż relaksacyjny",
        "leczniczy": "masaż leczniczy lub terapeutyczny",
        "sportowy": "masaż sportowy",
        "twarzy_kobido": "masaż twarzy lub kobido",
        "orientalny": "masaż tajski, balijski, ajurwedyjski lub lomi lomi",
        "kamienie": "masaż gorącymi kamieniami",
        "banka": "masaż bańką chińską",
        "antycellulitowy": "masaż antycellulitowy",
        "drenaz": "drenaż limfatyczny lub presoterapia",
        "modelowanie_sylwetki": "modelowanie sylwetki urządzeniem (endermologia, fala uderzeniowa, RF)",
        "peeling_ciala": "peeling ciała",
        "head_spa": "head spa lub masaż głowy",
    },
}
