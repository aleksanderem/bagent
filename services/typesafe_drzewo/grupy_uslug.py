"""Górny poziom drzewa usług v11 — grupy zdefiniowane tym, co się robi (bd BEAUTY_AUDIT-asrk, 28.09).

NIE kategorie salonów z Booksy: te opisują salon, nakładają się i są wybierane przez salon
(„manicure” w Paznokciach i Salonie kosmetycznym). Grupa opisuje USŁUGĘ i ma być podziałem:
każda usługa należy do jednej grupy, a pole „not_for” mówi, gdzie przebiega granica z sąsiednią.
Laser, peeling czy przedłużanie to metody, nie grupy — usługa idzie tam, gdzie jest to,
co się z klientem robi (depilacja laserowa → depilacja, laserowe usuwanie tatuażu → tatuaż).
"""

from __future__ import annotations

GRUPY: dict[str, dict] = {
    "włosy": {
        "what": "strzyżenie, koloryzacja, stylizacja, upięcia, pielęgnacja i przedłużanie włosów na głowie, trychologia",
        "not_for": "broda i golenie, brwi, rzęsy, usuwanie owłosienia z ciała",
        "examples": ["strzyżenie damskie", "koloryzacja", "baleyage", "keratynowe prostowanie", "upięcie", "przedłużanie włosów"]},
    "broda i golenie": {
        "what": "zarost: trymowanie i modelowanie brody, golenie brzytwą, koloryzacja brody",
        "not_for": "strzyżenie włosów na głowie",
        "examples": ["trymowanie brody", "golenie brzytwą", "modelowanie brody", "odsiwianie brody"]},
    "paznokcie": {
        "what": "manicure i pedicure kosmetyczny, stylizacja, przedłużanie, uzupełnianie i zdobienie paznokci dłoni i stóp",
        "not_for": "leczenie stóp i chorób paznokci: wrastające paznokcie, grzybica, odciski",
        "examples": ["manicure hybrydowy", "pedicure klasyczny", "przedłużanie paznokci żelem", "uzupełnienie żelu", "zdobienie"]},
    "stopy — podologia": {
        "what": "leczenie stóp i paznokci: wrastające paznokcie, grzybica, onycholiza, odciski, modzele, brodawki, pękające pięty, pedicure leczniczy",
        "not_for": "pedicure kosmetyczny i malowanie paznokci",
        "examples": ["pedicure podologiczny", "klamra ortonyksyjna", "usunięcie odcisku", "terapia brodawek", "opracowanie paznokci grzybiczych"]},
    "brwi i rzęsy": {
        "what": "henna, regulacja, laminacja i stylizacja brwi; przedłużanie, lifting, laminacja i farbowanie rzęs",
        "not_for": "makijaż permanentny brwi, makijaż okolicznościowy",
        "examples": ["henna brwi", "laminacja brwi", "przedłużanie rzęs 1:1", "lifting rzęs", "regulacja brwi"]},
    "twarz — pielęgnacja": {
        "what": "zabiegi pielęgnacyjne twarzy, szyi i dekoltu bez igieł: oczyszczanie, peelingi, mikrodermabrazja, maski, infuzja tlenowa, zabiegi nawilżające",
        "not_for": "iniekcje i zabiegi lekarskie, masaż jako osobna usługa",
        "examples": ["oczyszczanie wodorowe", "peeling kawitacyjny", "mikrodermabrazja", "zabieg nawilżający twarzy", "oxygeneo"]},
    "ciało — modelowanie i pielęgnacja": {
        "what": "zabiegi na ciało bez igieł: modelowanie sylwetki, cellulit, ujędrnianie, endermologia, kriolipoliza, body wrapping, peeling ciała",
        "not_for": "masaż, depilacja, iniekcje",
        "examples": ["endermologia LPG", "kriolipoliza", "body wrapping", "fale radiowe na brzuch", "peeling całego ciała"]},
    "medycyna estetyczna": {
        "what": "zabiegi z igłą lub lekarskie: toksyna botulinowa, kwas hialuronowy, stymulatory tkankowe, mezoterapia igłowa, osocze, nici, lipoliza iniekcyjna, radiofrekwencja mikroigłowa, lasery ablacyjne",
        "not_for": "zabiegi kosmetyczne bez igieł, botoks na włosy",
        "examples": ["botoks", "powiększanie ust", "wolumetria", "mezoterapia igłowa", "nici PDO", "osocze bogatopłytkowe"]},
    "depilacja": {
        "what": "usuwanie owłosienia z ciała i twarzy: wosk, pasta cukrowa, laser, IPL, elektroepilacja",
        "not_for": "regulacja brwi, golenie i trymowanie brody",
        "examples": ["depilacja woskiem nóg", "depilacja laserowa pach", "pasta cukrowa bikini", "elektroepilacja"]},
    "masaż": {
        "what": "masaże ciała, twarzy i części ciała: klasyczny, relaksacyjny, leczniczy, drenaż limfatyczny, masaże orientalne i gorącymi kamieniami",
        "not_for": "fizjoterapia i rehabilitacja, rytuały spa łączące kilka zabiegów",
        "examples": ["masaż klasyczny", "masaż relaksacyjny", "drenaż limfatyczny", "masaż tajski", "masaż gorącymi kamieniami"]},
    "fizjoterapia": {
        "what": "rehabilitacja i terapia narządu ruchu: terapia manualna, osteopatia, fala uderzeniowa, taping, suche igłowanie, fizjoterapia uroginekologiczna, elektroterapia",
        "not_for": "masaż relaksacyjny",
        "examples": ["terapia manualna", "fala uderzeniowa", "kinesiotaping", "osteopatia", "konsultacja fizjoterapeutyczna"]},
    "makijaż": {
        "what": "makijaż okolicznościowy, ślubny, wieczorowy, dzienny i nauka makijażu",
        "not_for": "makijaż permanentny",
        "examples": ["makijaż ślubny", "makijaż wieczorowy", "makijaż próbny", "lekcja makijażu"]},
    "makijaż permanentny i tatuaż": {
        "what": "pigmentacja skóry: makijaż permanentny brwi, ust i kresek, tatuaż, a także usuwanie makijażu permanentnego i tatuażu",
        "not_for": "henna brwi, przekłuwanie",
        "examples": ["makijaż permanentny brwi", "brwi pudrowe", "tatuaż", "laserowe usuwanie tatuażu", "dopigmentowanie"]},
    "piercing": {
        "what": "przekłuwanie uszu i ciała, wymiana i skracanie biżuterii",
        "not_for": "tatuaż",
        "examples": ["przekłucie uszu", "helix", "przekłucie nosa", "wymiana biżuterii"]},
    "stomatologia": {
        "what": "leczenie, higiena i estetyka zębów: higienizacja, wybielanie, leczenie kanałowe, ortodoncja, protetyka, implanty",
        "not_for": "biżuteria nazębna bez zabiegu stomatologicznego",
        "examples": ["higienizacja", "wybielanie zębów", "leczenie kanałowe", "wypełnienie", "aparat ortodontyczny"]},
    "zdrowie i psychologia": {
        "what": "konsultacje i usługi lekarzy, psychologów i psychoterapeutów, badania, akupunktura i terapie niekonwencjonalne",
        "not_for": "fizjoterapia, dietetyka",
        "examples": ["konsultacja psychologiczna", "psychoterapia", "konsultacja dermatologiczna", "badania krwi", "akupunktura"]},
    "dietetyka i trening": {
        "what": "konsultacje dietetyczne, trening personalny, joga i zajęcia ruchowe, analiza składu ciała",
        "not_for": "zabiegi modelujące ciało",
        "examples": ["konsultacja dietetyczna", "trening personalny", "joga", "analiza składu ciała"]},
    "spa i wellness": {
        "what": "rytuały spa łączące kilka zabiegów, sauna, kąpiele, solarium, pakiety relaksacyjne",
        "not_for": "pojedynczy masaż albo pojedynczy zabieg na twarz",
        "examples": ["rytuał spa", "sauna", "kąpiel relaksacyjna", "solarium"]},
    "zwierzęta": {
        "what": "pielęgnacja, kąpiel i strzyżenie zwierząt",
        "not_for": "usługi dla ludzi",
        "examples": ["strzyżenie psa", "kąpiel psa", "trymowanie psa", "pielęgnacja kota"]},
    "szkolenia i kursy": {
        "what": "szkolenia i kursy dla osób uczących się zawodu",
        "not_for": "zabieg wykonywany klientowi",
        "examples": ["szkolenie z przedłużania rzęs", "kurs makijażu", "szkolenie z modelowania ust"]},
    "inne usługi": {
        "what": "usługi spoza urody i zdrowia: motoryzacja, korepetycje, nauka jazdy, wynajem sali, sprzątanie",
        "not_for": "usługi urody i zdrowia",
        "examples": ["detailing samochodu", "korepetycje", "nauka jazdy", "wynajem sali"]},
}

__all__ = ["GRUPY"]
