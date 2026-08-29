"""Testy ekstraktora kontaktów.

Zestaw false-positive'ów pochodzi z realnego przebiegu A/B na 40 domenach
salonów (2026-08-18): luźna deobfuskacja "X at Y dot Z" łapała fragmenty
polskich zdań ("statystyk. Czy" → st@ystyk.ycz), CSS-owe klasy i lorem ipsum.
Każdy z nich wszedłby do bazy jako prawdziwy adres, więc zostają tu na stałe.
"""

from __future__ import annotations

import pytest

from enrichment.extract import (
    Extracted,
    email_confidence,
    extract,
    normalize_phone,
    valid_email,
)

# ── Śmieci, które MUSZĄ zostać odrzucone ────────────────────────────────────

SMIECI = [
    "st@ystyk.ycz",                      # "…statystyk. Czy…"
    "wi@nosem.jego",                     # "…wi nosem. Jego…"
    "stom@ologii.dzi",                   # "…stomatologii. Dzi…"
    "eksplo@acji.czym",                  # "…eksploatacji. Czym…"
    "pari@ur.excepteur",                 # lorem ipsum
    "osteop@ii.od",                      # "…osteopatii. Od…"
    "spokojnej@mosferze.czu",            # "…spokojnej atmosferze. Czu…"
    "autom@ic.your",                     # fragment JS/CSS
    ".use-floating-valid@ion-tip.wpcf",  # klasa CSS Contact Form 7
]


@pytest.mark.parametrize("smiec", SMIECI)
def test_odrzuca_falszywe_maile_z_deobfuskacji(smiec: str) -> None:
    assert not valid_email(smiec), f"{smiec} powinien być odrzucony"


PRAWDZIWE = [
    "salon.ambernova@gmail.com",
    "paulinarychlewska.kosmetologia@gmail.com",
    "recepcja@lexus-kielce.pl",
    "andersa@estetican.pl",
    "kontakt@enlingo.pl",
    "biuro@salon-uroda.com.pl",
    "info@studio.beauty",
    "hello@nails.co.uk",
]


@pytest.mark.parametrize("mail", PRAWDZIWE)
def test_przepuszcza_prawdziwe_maile(mail: str) -> None:
    assert valid_email(mail), f"{mail} powinien przejść"


# ── Deobfuskacja: tylko realne zapisy antyspamowe ───────────────────────────

@pytest.mark.parametrize("html,oczekiwany", [
    ("<p>napisz: kontakt (at) salon.pl</p>", "kontakt@salon.pl"),
    ("<p>biuro [małpa] uroda.com.pl</p>", "biuro@uroda.com.pl"),
    ("<p>salon (małpa) studio (kropka) pl</p>", "salon@studio.pl"),
])
def test_deobfuskacja_lapie_realne_zapisy(html: str, oczekiwany: str) -> None:
    found = extract(html, "https://salon.pl")
    assert oczekiwany in [m for m, _ in found.emails]


@pytest.mark.parametrize("zdanie", [
    "<p>Prowadzimy statystyki. Czytaj więcej o nas.</p>",
    "<p>Zabiegi stomatologii. Dziś zapraszamy.</p>",
    "<p>Lorem ipsum dolor sit amet pariatur. Excepteur sint occaecat.</p>",
    "<p>W spokojnej atmosferze. Czujemy się dobrze.</p>",
    "<p>Gabinet osteopatii. Od poniedziałku do piątku.</p>",
])
def test_deobfuskacja_nie_lapie_polskich_zdan(zdanie: str) -> None:
    found = extract(zdanie, "https://salon.pl")
    assert found.emails == [], f"złapał śmieć: {found.emails}"


# ── Priorytet i pewność ─────────────────────────────────────────────────────

def test_mailto_ma_wyzsza_pewnosc_niz_tekst() -> None:
    z_mailto = email_confidence("a@salon.pl", from_mailto=True, page_url="https://salon.pl")
    z_tekstu = email_confidence("a@salon.pl", from_mailto=False, page_url="https://salon.pl")
    assert z_mailto > z_tekstu


def test_mail_w_domenie_strony_bije_obcy() -> None:
    wlasny = email_confidence("kontakt@salon.pl", from_mailto=True, page_url="https://salon.pl")
    obcy = email_confidence("kontakt@agencja-web.pl", from_mailto=True, page_url="https://salon.pl")
    assert wlasny > obcy


def test_adres_agencyjny_dostaje_kare() -> None:
    zwykly = email_confidence("kontakt@salon.pl", from_mailto=True, page_url="https://salon.pl")
    agencja = email_confidence("webmaster@salon.pl", from_mailto=True, page_url="https://salon.pl")
    assert agencja < zwykly


def test_best_email_wybiera_najwyzsza_pewnosc() -> None:
    found = Extracted(emails=[("a@x.pl", 40), ("b@x.pl", 90), ("c@x.pl", 60)])
    assert found.best_email() == ("b@x.pl", 90)


# ── Telefony ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("surowy,oczekiwany", [
    ("+48 501 234 567", "+48501234567"),
    ("501-234-567", "+48501234567"),
    ("(22) 123 45 67", "+48221234567"),
    ("0048 501 234 567", "+48501234567"),
])
def test_normalizacja_telefonu(surowy: str, oczekiwany: str) -> None:
    assert normalize_phone(surowy) == oczekiwany


@pytest.mark.parametrize("zly", [
    "123",                 # za krótki
    "12345678901234",      # za długi
    "111 111 111",         # powtórzenie — placeholder
    "000 000 000",
])
def test_odrzuca_bledne_telefony(zly: str) -> None:
    assert normalize_phone(zly) is None


def test_telefon_z_href_tel() -> None:
    found = extract('<a href="tel:+48501234567">zadzwoń</a>', "https://salon.pl")
    assert found.phones == ["+48501234567"]


def test_nie_lapie_cen_jako_telefonow() -> None:
    html = "<p>Manicure hybrydowy 120 zł, pedicure 150 zł, zabieg 250 zł</p>"
    assert extract(html, "https://salon.pl").phones == []


# ── Social i NIP ────────────────────────────────────────────────────────────

def test_wyciaga_uchwyty_social() -> None:
    html = ('<a href="https://www.instagram.com/salon_uroda/">IG</a>'
            '<a href="https://www.facebook.com/SalonUroda">FB</a>')
    found = extract(html, "https://salon.pl")
    assert found.instagram == ["salon_uroda"]
    assert found.facebook == ["salonuroda"]


def test_pomija_smieciowe_sciezki_social() -> None:
    html = ('<a href="https://www.facebook.com/sharer/sharer.php?u=x">udostępnij</a>'
            '<a href="https://www.instagram.com/p/ABC123/">post</a>')
    found = extract(html, "https://salon.pl")
    assert found.facebook == []
    assert "p" not in found.instagram


def test_facebook_profile_php_daje_id() -> None:
    html = '<a href="https://www.facebook.com/profile.php?id=100069722544059">FB</a>'
    assert extract(html, "https://salon.pl").facebook == ["100069722544059"]


def test_wyciaga_nip() -> None:
    assert extract("<p>NIP: 525-234-40-78</p>", "https://salon.pl").nip == "5252344078"


# ── JSON-LD i linki kontaktowe ──────────────────────────────────────────────

def test_email_z_json_ld() -> None:
    html = ('<script type="application/ld+json">'
            '{"@type":"BeautySalon","email":"kontakt@salon.pl"}</script>')
    found = extract(html, "https://salon.pl")
    assert "kontakt@salon.pl" in [m for m, _ in found.emails]


def test_zbiera_linki_do_podstron_kontaktowych() -> None:
    html = '<a href="/kontakt">Kontakt</a><a href="/cennik">Cennik</a>'
    found = extract(html, "https://salon.pl")
    assert "https://salon.pl/kontakt" in found.contact_links
    assert all("cennik" not in link for link in found.contact_links)


def test_ignoruje_maile_platform_i_trackerow() -> None:
    html = ('<p>błąd@sentry.io wsparcie@wixpress.com '
            'noreply@salon.pl kontakt@salon.pl</p>')
    maile = [m for m, _ in extract(html, "https://salon.pl").emails]
    assert maile == ["kontakt@salon.pl"]
