"""Drzewo zabiegów: rodzina → rodzaj zabiegu (+ grupa pokrewieństwa i osie).

Po co: porównanie dwóch usług ma robić KOD na klasyfikacji każdej z nich,
a nie model pytany o parę. Klasyfikacja idzie krokami (klasyfikacja.py):
  1. rodzina zabiegu — jedna z RODZINY (wybór),
  2. rodzaje w tej rodzinie — tak/nie dla każdego (usługa bywa zestawem:
     „henna + regulacja”, „manicure + pedicure”),
  3. osie wariantu (obszar, etap, długość, objętość, odbiorca, pakiet) —
     bateria services/typesafe_profile, już zapisywana w service_profile.

Zasady budowy (25.09.2026, z danych, nie z holdoutu):
* Szkielet: drzewo zabiegów Booksy (21 dziedzin, ~370 węzłów, przypisane do
  94% usług) + metody z destylacji GLM (350 tys. nazw). Węzły Booksy, które są
  OBSZAREM („Depilacja nóg”), ODBIORCĄ („dla mężczyzn”) albo WARIANTEM
  („Rzęsy 2D”, „Uzupełnianie rzęs”), nie są tu rodzajem — to wartości osi.
* Rodzina = rodzaj procedury, NIE branża salonu: salon masażu sprzedaje
  depilację laserową, salon paznokci przedłużanie rzęs. Typ salonu jest tylko
  kontekstem czytania nazwy.
* Grupa = pokrewieństwo: dwa różne rodzaje z tej samej grupy to usługi
  powiązane (manicure hybrydowy vs klasyczny), z różnych grup — różne.
  Grupa może przekraczać rodzinę (mezoterapia igłowa vs mikroigłowa).
* Osie rodziny mówią, które cechy rozróżniają warianty w tej rodzinie
  (dokument dla właściciela produktu; porównanie liczy weto.py).

Drzewo jest DECYZJĄ PRODUKTOWĄ — zmiany zatwierdza Alex. Zmiana treści =
nowa WERSJA (klasyfikacje starej wersji nie mieszają się z nowymi).
"""

from __future__ import annotations

WERSJA = 1

# rodzaj: (grupa, opis „co to jest”, opis „czego NIE obejmuje” albo None)
Rodzaj = tuple[str, str, str | None]

RODZINY: dict[str, dict] = {
    "paznokcie": {
        "opis": "manicure, pedicure, stylizacja i przedłużanie paznokci, zdobienia, zabiegi na dłonie",
        "nie": "zabiegi podologiczne i leczenie stóp (to podologia)",
        "osie": ["etap", "dlugosc", "obszar", "odbiorca"],
        "rodzaje": {
            "manicure_klasyczny": ("manicure", "manicure klasyczny lub zwykły lakier, bez trwałego lakieru", "manicure hybrydowy, żel"),
            "manicure_hybrydowy": ("manicure", "manicure hybrydowy — lakier hybrydowy na dłoniach (także samo zdjęcie hybrydy)", None),
            "manicure_japonski": ("manicure", "manicure japoński (pasta i puder, P-Shine)", None),
            "manicure_tytanowy": ("manicure", "manicure tytanowy (proszek, dipping)", None),
            "wzmocnienie_plytki": ("manicure", "żel lub baza budująca na naturalnej płytce: wzmocnienie, nadbudowa, manicure żelowy bez przedłużania", "przedłużanie paznokci"),
            "przedluzanie_paznokci": ("stylizacja", "przedłużanie paznokci żelem, akrylem lub akrylożelem (szablon, tipsy) oraz ich uzupełnienie lub zdjęcie", "żel na naturalnej płytce bez przedłużania"),
            "pedicure_klasyczny": ("pedicure", "pedicure klasyczny lub ekspresowy, bez trwałego lakieru", "pedicure podologiczny"),
            "pedicure_hybrydowy": ("pedicure", "pedicure hybrydowy — lakier hybrydowy na stopach", None),
            "pedicure_pielegnacyjny": ("pedicure", "pedicure pielęgnacyjny: SPA, kwasowy, japoński, z peelingiem i maską", "pedicure podologiczny"),
            "zdobienie": ("dodatek_paznokcie", "zdobienie paznokci: french, baby boomer, wzorki, cyrkonie", None),
            "naprawa_paznokcia": ("dodatek_paznokcie", "naprawa lub rekonstrukcja pojedynczego paznokcia", None),
            "pielegnacja_dloni": ("dlonie", "zabieg na skórę dłoni: parafina, peeling i maska na dłonie", "sam manicure"),
        },
    },
    "depilacja": {
        "opis": "usuwanie owłosienia: laser, IPL, wosk, pasta cukrowa",
        "nie": "golenie brzytwą u barbera, regulacja brwi",
        "osie": ["obszar", "odbiorca", "pakiet"],
        "rodzaje": {
            "depilacja_laserowa": ("depilacja", "depilacja laserowa (laser diodowy, aleksandrytowy, Nd:YAG, Soprano, Motus)", "IPL, wosk"),
            "depilacja_ipl": ("depilacja", "depilacja IPL lub E-light (światło impulsowe)", "laser"),
            "depilacja_woskiem": ("depilacja", "depilacja woskiem (także wosk twardy, wosk z nosa lub uszu)", "pasta cukrowa"),
            "depilacja_pasta": ("depilacja", "depilacja pastą cukrową (cukrowa, sugaring)", "wosk"),
            "depilacja_nieokreslona": ("depilacja", "depilacja bez podanej metody", "nazwa podaje laser, IPL, wosk albo pastę"),
        },
    },
    "brwi_rzesy": {
        "opis": "rzęsy i brwi: przedłużanie, laminacja, lifting, henna, regulacja",
        "nie": "makijaż permanentny brwi (to makijaż permanentny), depilacja twarzy",
        "osie": ["etap", "objetosc", "obszar"],
        "rodzaje": {
            "przedluzanie_rzes": ("rzesy", "przedłużanie lub zagęszczanie rzęs (1:1, 2D, 3D, mega volume), także uzupełnienie i zdjęcie", None),
            "lifting_rzes": ("rzesy", "laminacja lub lifting rzęs", "przedłużanie rzęs"),
            "henna_rzes": ("henna", "henna lub farbowanie rzęs", None),
            "henna_brwi": ("henna", "henna lub farbowanie brwi (klasyczna)", "henna pudrowa"),
            "henna_pudrowa": ("henna", "henna pudrowa brwi", None),
            "regulacja_brwi": ("brwi", "regulacja, geometria lub architektura brwi", None),
            "laminacja_brwi": ("brwi", "laminacja brwi", None),
            "pielegnacja_brwi_rzes": ("brwi", "botoks lub odżywka na brwi albo rzęsy, SPA dla brwi", None),
            "przedluzanie_brwi": ("brwi", "przedłużanie lub zagęszczanie brwi włoskami", None),
        },
    },
    "makijaz": {
        "opis": "makijaż: okolicznościowy, ślubny, nauka makijażu, makijaż permanentny i jego usuwanie",
        "nie": "henna i laminacja brwi",
        "osie": ["obszar", "etap"],
        "rodzaje": {
            "makijaz_okolicznosciowy": ("makijaz", "makijaż dzienny, wieczorowy, okolicznościowy lub próbny", None),
            "makijaz_slubny": ("makijaz", "makijaż ślubny", None),
            "nauka_makijazu": ("makijaz", "lekcja lub nauka makijażu", None),
            "makijaz_permanentny": ("pmu", "makijaż permanentny lub microblading (brwi, usta, kreska), także korekta i odświeżenie", None),
            "usuwanie_pmu": ("usuwanie_pigmentu", "usuwanie makijażu permanentnego (laser, remover)", None),
        },
    },
    "fryzjerstwo": {
        "opis": "włosy na głowie: strzyżenie, koloryzacja, pielęgnacja, stylizacja, przedłużanie",
        "nie": "broda i golenie (to barber)",
        "osie": ["odbiorca", "dlugosc", "etap"],
        "rodzaje": {
            "strzyzenie": ("strzyzenie", "strzyżenie włosów (nożyczkami lub maszynką, fade, buzz cut, grzywka)", None),
            "modelowanie": ("stylizacja", "modelowanie, suszenie, prostowanie lub kręcenie na gorąco", None),
            "upiecie": ("stylizacja", "upięcie, fryzura okolicznościowa lub ślubna, warkocze", None),
            "koloryzacja_globalna": ("koloryzacja", "koloryzacja lub farbowanie całych włosów", "sam odrost"),
            "odrost": ("koloryzacja", "farbowanie samego odrostu", None),
            "balejaz_ombre": ("koloryzacja", "balejaż, ombre, sombre, air touch", None),
            "pasemka_refleksy": ("koloryzacja", "pasemka, refleksy, koloryzacja wielotonowa", None),
            "tonowanie": ("koloryzacja", "tonowanie włosów", None),
            "dekoloryzacja": ("koloryzacja", "dekoloryzacja lub rozjaśnianie", None),
            "regeneracja": ("pielegnacja_wlosow", "regeneracja, rekonstrukcja lub kuracja włosów (Olaplex, botoks na włosy, zabiegi pielęgnacyjne)", None),
            "prostowanie_trwale": ("pielegnacja_wlosow", "keratynowe prostowanie, nanoplastia", None),
            "trwala": ("pielegnacja_wlosow", "trwała ondulacja", None),
            "przedluzanie_wlosow": ("przedluzanie_wlosow", "przedłużanie lub zagęszczanie włosów", None),
            "skora_glowy": ("skora_glowy", "zabieg na skórę głowy: trychologia, peeling skóry głowy, mezoterapia głowy, head spa", None),
            "dredy_warkoczyki": ("stylizacja", "dredy, warkoczyki afro", None),
        },
    },
    "barber": {
        "opis": "broda, wąsy i golenie",
        "nie": "strzyżenie włosów na głowie (to fryzjerstwo)",
        "osie": ["obszar"],
        "rodzaje": {
            "broda": ("broda", "strzyżenie, trymowanie, konturowanie lub modelowanie brody i wąsów", None),
            "golenie": ("golenie", "golenie brzytwą lub na gładko (twarzy albo głowy), gorący ręcznik", None),
            "odsiwianie": ("koloryzacja", "odsiwianie lub kamuflaż siwizny", None),
            "farbowanie_brody": ("koloryzacja", "farbowanie brody", None),
            "pielegnacja_brody": ("broda", "pielęgnacja brody (olejki, maska)", None),
        },
    },
    "pielegnacja_twarzy": {
        "opis": "kosmetologiczne zabiegi pielęgnacyjne na twarz: oczyszczanie, peelingi, mezoterapia mikroigłowa i bezigłowa, maski, nawilżanie",
        "nie": "zastrzyki wykonywane przez lekarza, zabiegi laserowe i radiofrekwencja",
        "osie": ["obszar", "pakiet"],
        "rodzaje": {
            "oczyszczanie_manualne": ("oczyszczanie", "oczyszczanie manualne twarzy lub pleców", None),
            "oczyszczanie_wodorowe": ("oczyszczanie", "oczyszczanie wodorowe, hydrabrazja, aquasure", None),
            "peeling_kawitacyjny": ("oczyszczanie", "peeling kawitacyjny", None),
            "mikrodermabrazja": ("oczyszczanie", "mikrodermabrazja (diamentowa, korundowa) lub oksybrazja", None),
            "peeling_chemiczny": ("peeling", "peeling chemiczny lub kwasy (migdałowy, salicylowy, PRX, BioRePeel, TCA)", None),
            "peeling_enzymatyczny": ("peeling", "peeling enzymatyczny lub mechaniczny", None),
            "mezoterapia_mikroiglowa": ("mezoterapia", "mezoterapia mikroigłowa, Dermapen", "radiofrekwencja mikroigłowa"),
            "mezoterapia_bezigla": ("mezoterapia", "mezoterapia bezigłowa: elektroporacja, sonoforeza, infuzja tlenowa", None),
            "zabieg_pielegnacyjny": ("pielegnacja", "zabieg pielęgnacyjny, nawilżający, odmładzający lub maska na twarz bez aparatu", None),
            "zabieg_tradzik": ("pielegnacja", "zabieg na trądzik lub przebarwienia", None),
            "konsultacja_kosmetologiczna": ("konsultacja", "konsultacja kosmetologiczna", None),
        },
    },
    "iniekcje": {
        "opis": "medycyna estetyczna — zabiegi z igłą lub kaniulą: toksyna botulinowa, wypełniacze, stymulatory, osocze, mezoterapia igłowa, nici, lipoliza",
        "nie": "mezoterapia mikroigłowa (Dermapen), zabiegi laserowe",
        "osie": ["obszar", "pakiet"],
        "rodzaje": {
            "toksyna_botulinowa": ("toksyna", "toksyna botulinowa (botoks), także na nadpotliwość i bruksizm", "botoks na włosy, botoks na brwi"),
            "wypelniacz_kwas": ("wypelniacz", "wypełniacz z kwasem hialuronowym: usta, zmarszczki, wolumetria, modelowanie twarzy", None),
            "stymulator_tkankowy": ("stymulator", "stymulator tkankowy lub skinbooster (Profhilo, Sculptra, Jalupro, Nucleofill, polinukleotydy, egzosomy, biorewitalizacja)", None),
            "osocze": ("stymulator", "osocze bogatopłytkowe (PRP) lub fibryna", None),
            "mezoterapia_iglowa": ("mezoterapia", "mezoterapia igłowa", "mikroigłowa"),
            "nici": ("nici", "nici liftingujące (PDO)", None),
            "lipoliza": ("lipoliza", "lipoliza iniekcyjna (Aqualyx, Lipolab)", None),
            "karboksyterapia": ("karboksyterapia", "karboksyterapia", None),
            "hialuronidaza": ("wypelniacz", "rozpuszczanie kwasu hialuronowego", None),
            "wlew": ("wlew", "wlew dożylny lub kroplówka witaminowa", None),
            "konsultacja_lekarska": ("konsultacja", "konsultacja lekarska medycyny estetycznej", None),
        },
    },
    "aparatura": {
        "opis": "zabiegi urządzeniem na skórę: lasery, IPL, radiofrekwencja, HIFU, fala radiowa",
        "nie": "depilacja, usuwanie tatuażu, modelowanie sylwetki na ciele",
        "osie": ["obszar", "pakiet"],
        "rodzaje": {
            "laser_frakcyjny": ("laser", "laser frakcyjny (CO2, erbowy, tulowy, Fraxel)", None),
            "laser_naczynka": ("laser", "laser lub IPL na naczynka i rumień", None),
            "fotoodmladzanie": ("laser", "fotoodmładzanie, IPL na przebarwienia, peeling węglowy (Black Doll), laser pikosekundowy na skórę", None),
            "radiofrekwencja": ("rf", "radiofrekwencja lub fala radiowa na twarz", "radiofrekwencja mikroigłowa"),
            "rf_mikroiglowa": ("rf", "radiofrekwencja mikroigłowa (Morpheus, Potenza, Sylfirm)", None),
            "hifu": ("hifu", "HIFU (ultradźwięki skupione), Ultherapy", None),
            "inny_aparat": ("aparat", "inny zabieg aparaturowy na twarz (mikroprądy, darsonwalizacja, LED)", None),
        },
    },
    "sylwetka": {
        "opis": "modelowanie sylwetki i zabiegi urządzeniem na ciało",
        "nie": "masaż ręczny",
        "osie": ["obszar", "pakiet"],
        "rodzaje": {
            "endermologia": ("sylwetka", "endermologia, LPG, masaż podciśnieniowy, Icoone", None),
            "kriolipoliza": ("sylwetka", "kriolipoliza", None),
            "kawitacja": ("sylwetka", "kawitacja ultradźwiękowa, liposukcja kawitacyjna", None),
            "ems": ("sylwetka", "elektrostymulacja mięśni, EMS, EMSculpt", None),
            "fala_uderzeniowa": ("sylwetka", "fala uderzeniowa", None),
            "presoterapia": ("sylwetka", "presoterapia", None),
            "rf_cialo": ("sylwetka", "radiofrekwencja, HIFU lub fala radiowa na ciało (ujędrnianie, cellulit)", None),
            "body_wrapping": ("sylwetka", "body wrapping, okłady, bandażowanie", None),
        },
    },
    "usuwanie_zmian": {
        "opis": "usuwanie tatuażu, zmian skórnych, blizn i rozstępów",
        "nie": "usuwanie owłosienia",
        "osie": ["obszar", "pakiet"],
        "rodzaje": {
            "usuwanie_tatuazu": ("usuwanie_pigmentu", "laserowe usuwanie tatuażu", None),
            "usuwanie_zmian_skornych": ("zmiany", "usuwanie włókniaków, kurzajek, brodawek, znamion, elektrokoagulacja", None),
            "blizny_rozstepy": ("zmiany", "zabieg na blizny lub rozstępy", None),
        },
    },
    "masaz": {
        "opis": "masaż ręczny ciała, twarzy lub głowy",
        "nie": "masaż urządzeniem (endermologia), fizjoterapia",
        "osie": ["obszar", "odbiorca"],
        "rodzaje": {
            "masaz_klasyczny": ("masaz", "masaż klasyczny", None),
            "masaz_relaksacyjny": ("masaz", "masaż relaksacyjny, aromaterapeutyczny, rytuał", None),
            "masaz_leczniczy": ("masaz", "masaż leczniczy, terapeutyczny, tkanek głębokich, sportowy", None),
            "masaz_twarzy": ("masaz", "masaż twarzy, kobido, masaż głowy", None),
            "masaz_orientalny": ("masaz", "masaż tajski, balijski, lomi lomi, ajurwedyjski, shiatsu, chiński", None),
            "masaz_kamienie": ("masaz", "masaż gorącymi kamieniami", None),
            "masaz_banka": ("masaz", "masaż bańką chińską", None),
            "masaz_modelujacy": ("masaz", "masaż antycellulitowy, modelujący, ujędrniający, maderoterapia", None),
            "drenaz_limfatyczny": ("masaz", "drenaż limfatyczny ręczny", "presoterapia"),
            "masaz_dzwiekiem": ("masaz", "masaż dźwiękiem, misy tybetańskie", None),
        },
    },
    "podologia": {
        "opis": "podologia: leczenie stóp i paznokci stóp",
        "nie": "pedicure kosmetyczny",
        "osie": ["obszar", "etap"],
        "rodzaje": {
            "zabieg_podologiczny": ("podologia", "zabieg podologiczny, usuwanie odcisków, modzeli, pękających pięt", None),
            "pedicure_podologiczny": ("podologia", "pedicure podologiczny", None),
            "wrastajacy_paznokiec": ("podologia", "wrastający paznokieć: klamra ortonyksyjna, tamponada", None),
            "grzybica": ("podologia", "leczenie grzybicy paznokci lub stóp", None),
            "konsultacja_podologiczna": ("konsultacja", "konsultacja podologiczna", None),
        },
    },
    "spa_cialo": {
        "opis": "SPA i pielęgnacja ciała: peeling ciała, opalanie, sauna, solarium",
        "nie": "masaż",
        "osie": ["odbiorca"],
        "rodzaje": {
            "peeling_ciala": ("spa", "peeling ciała", None),
            "opalanie": ("opalanie", "opalanie natryskowe lub solarium", None),
            "sauna": ("spa", "sauna, grota solna, łaźnia", None),
        },
    },
    "tatuaz_piercing": {
        "opis": "tatuaż, piercing, ozdoby na zęby",
        "nie": "makijaż permanentny, usuwanie tatuażu",
        "osie": ["obszar"],
        "rodzaje": {
            "tatuaz": ("tatuaz", "wykonanie tatuażu", None),
            "piercing": ("piercing", "przekłucie (ucho, nos, pępek, inne) i biżuteria", None),
            "tooth_gems": ("ozdoby", "ozdoby na zęby", None),
        },
    },
    "fizjoterapia": {
        "opis": "fizjoterapia i rehabilitacja",
        "nie": "masaż relaksacyjny",
        "osie": ["obszar"],
        "rodzaje": {
            "terapia_manualna": ("fizjo", "terapia manualna, osteopatia, chiropraktyka", None),
            "kinesiotaping": ("fizjo", "kinesiotaping", None),
            "rehabilitacja": ("fizjo", "rehabilitacja, ćwiczenia, fizjoterapia uroginekologiczna", None),
        },
    },
    "zwierzeta": {
        "opis": "usługi dla zwierząt: strzyżenie, kąpiel, trymowanie",
        "nie": "usługi dla ludzi",
        "osie": [],
        "rodzaje": {
            "grooming": ("zwierzeta", "strzyżenie, kąpiel, trymowanie lub wyczesywanie zwierzęcia", None),
        },
    },
    "inne": {
        "opis": "usługa spoza urody: trening, dietetyka, psychologia, stomatologia, motoryzacja i inne",
        "nie": "zabiegi kosmetyczne, fryzjerskie i medycyny estetycznej",
        "osie": [],
        "rodzaje": {},
    },
}


def grupa(rodzaj: str) -> str | None:
    for r in RODZINY.values():
        if rodzaj in r["rodzaje"]:
            return r["rodzaje"][rodzaj][0]
    return None
