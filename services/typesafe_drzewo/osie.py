"""Katalog cech usługi — wspólny dla WSZYSTKICH branż.

Źródło: osie, które destylacja GLM sama zaproponowała dla 334 tys. nazw
(19 kluczy: metoda, obszar, marka_preparatu, technika, specjalizacja, …),
sklejone w cechy o jednym znaczeniu. Opisy i przykłady celowo z różnych
branż — katalog nie może faworyzować żadnej.

Cechy liczbowe (czas, ilość, liczba zabiegów/okolic) NIE są wybierane przez
model — ich wartości wyciąga kod (liczby i jednostki), model tylko rozpoznaje,
że wartość jest liczbą danego rodzaju.
"""

from __future__ import annotations

# cecha: (co opisuje, czego NIE obejmuje, przykłady z różnych branż)
CECHY: dict[str, tuple[str, str, list[str]]] = {
    "rodzaj_zabiegu": ("jaki zabieg lub usługę się wykonuje",
                       "sposób wykonania, preparat, obszar ciała",
                       ["strzyżenie", "depilacja laserowa", "manicure hybrydowy", "mezoterapia igłowa", "masaż klasyczny", "przedłużanie rzęs"]),
    "technika": ("sposób lub technika wykonania tego samego zabiegu",
                 "nazwa samego zabiegu, marka preparatu",
                 ["hybryda", "żel", "akryl", "brzytwa", "nożyczki", "wosk twardy", "metoda suchą"]),
    "preparat_urzadzenie": ("nazwa własna lub marka preparatu, urządzenia albo technologii",
                            "ogólna nazwa zabiegu",
                            ["Profhilo", "BioRePeel", "Soprano", "aleksandryt", "laser diodowy", "Olaplex", "Indiba"]),
    "material": ("materiał lub produkt użyty w zabiegu",
                 "marka własna, obszar ciała",
                 ["olejek", "czekolada", "kępki rzęs", "tytan", "masło shea"]),
    "obszar": ("część ciała lub obszar, na którym wykonuje się usługę",
               "długość włosów, wielkość obszaru w liczbach",
               ["nogi", "twarz, szyja, dekolt", "pachy", "broda", "paznokcie stóp", "całe ciało"]),
    "dlugosc": ("długość włosów, paznokci lub rzęs",
                "czas trwania zabiegu",
                ["krótkie", "do ramion", "długie", "1-2", "bardzo długie"]),
    "objetosc": ("objętość lub gęstość przedłużanych rzęs albo włosów",
                 "liczba zabiegów",
                 ["1:1", "2D", "3-4D", "mega volume"]),
    "etap": ("etap usługi względem wcześniejszych wizyt",
             "rodzaj zabiegu",
             ["założenie", "uzupełnienie", "zdjęcie", "korekta", "pierwsza wizyta", "konsultacja"]),
    "odbiorca": ("dla kogo jest usługa",
                 "problem skóry, liczba osób",
                 ["kobiety", "mężczyźni", "dzieci", "psy", "kobiety w ciąży"]),
    "liczba_osob": ("ile osób korzysta jednocześnie",
                    "liczba zabiegów w pakiecie",
                    ["dla dwojga", "para", "2 osoby", "4 ręce"]),
    "cel": ("cel, efekt albo problem, któremu zabieg służy",
            "nazwa zabiegu, obszar ciała",
            ["relaksacyjny", "antycellulitowy", "leczniczy", "trądzik", "wrastający paznokieć", "ujędrnienie"]),
    "okazja": ("okazja lub styl usługi",
               "cel zdrowotny",
               ["ślub", "wieczorowy", "dzienny", "sesja zdjęciowa"]),
    "poziom_specjalisty": ("poziom lub staż osoby wykonującej",
                           "rodzaj zabiegu",
                           ["junior", "top stylistka", "master barber", "lekarz"]),
    "zwierze": ("cecha zwierzęcia: rasa, wielkość, sierść",
                "usługa dla ludzi",
                ["do 10 kg", "długowłosy", "york"]),
    "liczba_zabiegow": ("LICZBA zabiegów, sesji lub wizyt w cenie, także pakiet i combo",
                        "czas trwania",
                        ["pakiet", "10 zabiegów", "1 zabieg", "combo", "seria 5"]),
    "czas_trwania": ("czas trwania zabiegu",
                     "długość włosów",
                     ["60 min", "90 minut", "30 min"]),
    "ilosc_preparatu": ("ilość preparatu lub liczba okolic albo sztuk",
                        "liczba zabiegów",
                        ["1 ml", "2 okolice", "1 szt", "5 cm2"]),
    "nie_cecha": ("informacja, która nie opisuje usługi",
                  "jakakolwiek cecha zabiegu",
                  ["płatność gotówką", "online", "promocja", "indywidualnie"]),
}

# Cechy, których wartości porównuje kod (liczby), nie lista wartości.
LICZBOWE = frozenset({"liczba_zabiegow", "czas_trwania", "ilosc_preparatu"})
