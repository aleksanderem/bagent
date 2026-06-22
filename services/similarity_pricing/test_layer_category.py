"""Testy dla warstwy C — layer_category.py.

Pokrycie:
  - is_generic_name: typowe przypadki generyczne i specyficzne, case/diakrytyki
  - apply_category_context: weight=1.0 twardy filtr, weight=0.0 brak filtrowania,
    nazwy niespecyficzne, safety fallback (filtr pusty), category_name=None/emoji/numerowane,
    immutability
"""
import copy

import pytest

from services.similarity_pricing.layer_category import (
    apply_category_context,
    is_generic_name,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_subject(
    name: str,
    category: str | None = "Medycyna estetyczna",
    price: int = 20000,
) -> dict:
    return {
        "service_id": 1,
        "service_name": name,
        "price_grosze": price,
        "duration_minutes": 30,
        "category_name": category,
        "is_package": False,
    }


def _make_sample(
    service_id: int,
    name: str,
    category: str | None,
    price: int = 5000,
    similarity: float = 0.83,
) -> dict:
    return {
        "service_id": service_id,
        "booksy_id": 50 + service_id,
        "salon_name": f"Salon {service_id}",
        "service_name": name,
        "price_grosze": price,
        "duration_minutes": 30,
        "similarity": similarity,
        "category_name": category,
        "is_package": False,
    }


# Przykładowe samples do testów filtrowania kategorii
SAMPLES_MED_EST = [
    _make_sample(1, "Konsultacja", "Medycyna estetyczna", price=10000),
    _make_sample(2, "Konsultacja", "Medycyna estetyczna", price=15000),
]

SAMPLES_RZESY = [
    _make_sample(3, "Konsultacja", "Stylizacja rzęs", price=3000),
    _make_sample(4, "Konsultacja", "Stylizacja rzęs", price=4000),
]

SAMPLES_MIXED = SAMPLES_MED_EST + SAMPLES_RZESY


# ---------------------------------------------------------------------------
# Testy is_generic_name
# ---------------------------------------------------------------------------

class TestIsGenericName:
    def test_konsultacja_jest_generyczna(self):
        assert is_generic_name("Konsultacja") is True

    def test_wizyta_kontrolna_jest_generyczna(self):
        assert is_generic_name("Wizyta kontrolna") is True

    def test_pakiet_jest_generyczny(self):
        assert is_generic_name("Pakiet") is True

    def test_wizyta_jest_generyczna(self):
        assert is_generic_name("Wizyta") is True

    def test_sesja_jest_generyczna(self):
        assert is_generic_name("Sesja") is True

    def test_zabieg_jest_generyczny(self):
        assert is_generic_name("Zabieg") is True

    def test_voucher_jest_generyczny(self):
        assert is_generic_name("Voucher") is True

    def test_bon_jest_generyczny(self):
        assert is_generic_name("Bon") is True

    def test_bon_podarunkowy_jest_generyczny(self):
        assert is_generic_name("Bon podarunkowy") is True

    def test_dla_niego_jest_generyczny(self):
        assert is_generic_name("Dla niego") is True

    def test_dla_niej_jest_generyczna(self):
        assert is_generic_name("Dla niej") is True

    def test_promocja_jest_generyczna(self):
        assert is_generic_name("Promocja") is True

    def test_oferta_jest_generyczna(self):
        assert is_generic_name("Oferta") is True

    def test_bestseller_jest_generyczny(self):
        assert is_generic_name("Bestseller") is True

    def test_porada_jest_generyczna(self):
        assert is_generic_name("Porada") is True

    def test_diagnostyka_jest_generyczna(self):
        assert is_generic_name("Diagnostyka") is True

    def test_kontrola_jest_generyczna(self):
        assert is_generic_name("Kontrola") is True

    def test_dobor_ze_znakiem_jest_generyczny(self):
        assert is_generic_name("Dobór") is True

    def test_spotkanie_jest_generyczne(self):
        assert is_generic_name("Spotkanie") is True

    # Nazwy specyficzne — powinny zwracać False

    def test_presoterapia_drenaż_nie_jest_generyczna(self):
        assert is_generic_name("Presoterapia drenaż limfatyczny") is False

    def test_botoks_na_wlosy_nie_jest_generyczny(self):
        assert is_generic_name("Botoks na włosy") is False

    def test_manicure_hybrydowy_nie_jest_generyczny(self):
        assert is_generic_name("Manicure hybrydowy") is False

    def test_konsultacja_kosmetologiczna_z_analiza_nie_jest_generyczna(self):
        """Konsultacja + deskryptor = specyficzna."""
        assert is_generic_name("Konsultacja kosmetologiczna z analizą skóry") is False

    def test_konsultacja_dermatologiczna_nie_jest_generyczna(self):
        assert is_generic_name("Konsultacja dermatologiczna") is False

    def test_zabieg_na_twarz_nie_jest_generyczny(self):
        assert is_generic_name("Zabieg na twarz kwas hialuronowy") is False

    def test_stylizacja_rzeseń_nie_jest_generyczna(self):
        assert is_generic_name("Stylizacja rzęs 1:1") is False

    def test_pedicure_klasyczny_nie_jest_generyczny(self):
        assert is_generic_name("Pedicure klasyczny") is False

    # Case- i diacritic-insensitive

    def test_uppercase_konsultacja_jest_generyczna(self):
        assert is_generic_name("KONSULTACJA") is True

    def test_mixed_case_konsultacja_jest_generyczna(self):
        assert is_generic_name("Konsultacja") is True

    def test_konsultacja_bez_diakrytykow_jest_generyczna(self):
        # Znormalizowany: "konsultacja"
        assert is_generic_name("konsultacja") is True

    def test_pusta_nazwa_zwraca_false(self):
        assert is_generic_name("") is False

    def test_none_like_whitespace_zwraca_false(self):
        assert is_generic_name("   ") is False


# ---------------------------------------------------------------------------
# Testy apply_category_context
# ---------------------------------------------------------------------------

class TestApplyCategoryContext:
    # --- weight=1.0 twardy filtr dla nazwy generycznej ---

    def test_weight_1_generyczna_filtruje_niezgodne(self):
        """Dla generycznej nazwy i weight=1 zostają tylko samples ze zgodną kategorią."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        samples = SAMPLES_MIXED.copy()

        result, meta = apply_category_context(subject, samples, weight=1.0)

        assert meta["was_generic"] is True
        assert meta["weight"] == 1.0
        assert meta["safety_fallback"] is False
        # Tylko medycyna estetyczna powinna zostać
        result_cats = {s["category_name"] for s in result}
        assert "Stylizacja rzęs" not in result_cats
        assert "Medycyna estetyczna" in result_cats
        assert meta["n_out"] == len(SAMPLES_MED_EST)

    def test_weight_1_generyczna_safety_gdy_wszystkie_niezgodne(self):
        """Gdy filtr usunąłby wszystko — safety fallback, lista niepusta."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        all_rzesy = SAMPLES_RZESY.copy()

        result, meta = apply_category_context(subject, all_rzesy, weight=1.0)

        assert meta["safety_fallback"] is True
        assert len(result) > 0  # nie zwraca pustej listy
        assert meta["n_out"] == len(all_rzesy)

    # --- weight=0.0 brak filtrowania ---

    def test_weight_0_brak_filtrowania(self):
        """weight=0 — wszystkie samples zostają, boost tylko informacyjny."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        samples = SAMPLES_MIXED.copy()

        result, meta = apply_category_context(subject, samples, weight=0.0)

        assert meta["n_out"] == meta["n_in"] == len(samples)
        assert meta["safety_fallback"] is False
        # Boost oznaczony na każdym sample
        for s in result:
            assert "category_boost" in s

    def test_weight_0_generyczna_wszystkie_zostaja(self):
        """Nawet dla generycznej nazwy i weight=0 — brak filtra."""
        subject = _make_subject("Wizyta kontrolna", category="Kosmetologia")
        result, meta = apply_category_context(subject, SAMPLES_MIXED, weight=0.0)
        assert meta["n_out"] == len(SAMPLES_MIXED)

    # --- Nazwy niespecyficzne (specyficzne) — samples przeżywa niezależnie od kategorii ---

    def test_specyficzna_nazwa_nie_filtruje(self):
        """Dla nazwy specyficznej — samples nieruszone niezależnie od weight."""
        subject = _make_subject("Presoterapia drenaż limfatyczny", category="Medycyna estetyczna")
        samples = SAMPLES_RZESY.copy()

        result, meta = apply_category_context(subject, samples, weight=1.0)

        assert meta["was_generic"] is False
        assert meta["n_out"] == len(samples)
        assert meta["safety_fallback"] is False

    def test_specyficzna_nazwa_weight_05_nie_filtruje(self):
        subject = _make_subject("Manicure hybrydowy", category="Paznokcie")
        result, meta = apply_category_context(subject, SAMPLES_MIXED, weight=0.5)
        assert meta["was_generic"] is False
        assert meta["n_out"] == len(SAMPLES_MIXED)

    # --- category_name = None / śmieciowe wartości ---

    def test_subject_cat_none_boost_0_dla_wszystkich(self):
        """Subject bez kategorii: brak zgodności z nikim; safety nie odpala bo was_generic=True i all mismatch → safety_fallback."""
        subject = _make_subject("Konsultacja", category=None)
        result, meta = apply_category_context(subject, SAMPLES_MIXED, weight=1.0)
        # Filtr usuwa wszystkich → safety fallback
        assert meta["safety_fallback"] is True
        assert len(result) > 0

    def test_sample_cat_none_dostaje_boost_0(self):
        """Sample z category_name=None ma boost=0.0."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        sample_none_cat = [_make_sample(99, "Konsultacja", None)]
        result, _ = apply_category_context(subject, sample_none_cat, weight=0.0)
        assert result[0]["category_boost"] == 0.0

    def test_sample_cat_emoji_normalizuje_sie_do_none(self):
        """Kategoria '🌸🌸🌸' normalizuje się do braku → boost=0."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        sample_emoji = [_make_sample(100, "Konsultacja", "🌸🌸🌸")]
        result, _ = apply_category_context(subject, sample_emoji, weight=0.0)
        assert result[0]["category_boost"] == 0.0

    def test_sample_cat_numerowana_normalizuje_sie_poprawnie(self):
        """Kategoria '01. Konsultacja' normalizuje się do 'konsultacja' — nieistotna tutaj,
        ale ważne że nie crashuje i zachowuje się sensownie."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        sample_num = [_make_sample(101, "Konsultacja", "01. Medycyna estetyczna")]
        result, meta = apply_category_context(subject, sample_num, weight=1.0)
        # "01. Medycyna estetyczna" normalizuje się do "medycyna estetyczna" — powinno pasować
        assert meta["n_category_match"] == 1

    def test_category_dash_normalizuje_sie_do_none(self):
        """Kategoria '—' to brak kategorii."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        sample_dash = [_make_sample(102, "Konsultacja", "—")]
        result, _ = apply_category_context(subject, sample_dash, weight=0.0)
        assert result[0]["category_boost"] == 0.0

    # --- Immutability ---

    def test_nie_mutuje_wejscia_subject(self):
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        original_subject = copy.deepcopy(subject)
        apply_category_context(subject, SAMPLES_MIXED, weight=1.0)
        assert subject == original_subject

    def test_nie_mutuje_wejscia_samples(self):
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        samples = copy.deepcopy(SAMPLES_MIXED)
        original_samples = copy.deepcopy(samples)
        apply_category_context(subject, samples, weight=1.0)
        assert samples == original_samples

    def test_zwracane_dicty_to_nowe_obiekty(self):
        """Returned samples are new dicts, not references to original."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        samples = SAMPLES_MIXED.copy()
        result, _ = apply_category_context(subject, samples, weight=0.0)
        for orig, new in zip(samples, result):
            assert orig is not new

    # --- Meta poprawność ---

    def test_meta_n_in_n_out_poprawne(self):
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        result, meta = apply_category_context(subject, SAMPLES_MIXED, weight=1.0)
        assert meta["n_in"] == len(SAMPLES_MIXED)
        assert meta["n_out"] == len(result)

    def test_meta_n_category_match_poprawne(self):
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        _, meta = apply_category_context(subject, SAMPLES_MIXED, weight=0.0)
        assert meta["n_category_match"] == len(SAMPLES_MED_EST)

    def test_meta_was_generic_true_dla_generycznej(self):
        subject = _make_subject("Wizyta")
        _, meta = apply_category_context(subject, SAMPLES_MIXED, weight=0.5)
        assert meta["was_generic"] is True

    def test_meta_was_generic_false_dla_specyficznej(self):
        subject = _make_subject("Botoks na włosy")
        _, meta = apply_category_context(subject, SAMPLES_MIXED, weight=1.0)
        assert meta["was_generic"] is False

    # --- Puste wejście ---

    def test_puste_samples_zwraca_pusta_liste(self):
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        result, meta = apply_category_context(subject, [], weight=1.0)
        assert result == []
        assert meta["n_in"] == 0
        assert meta["n_out"] == 0

    # --- Miękkie filtrowanie weight=0.5 ---

    def test_weight_05_generyczna_usuwa_niezgodne_gdy_sa_zgodne(self):
        """weight=0.5 + generyczna + są zgodne → usuwa niezgodne."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        result, meta = apply_category_context(subject, SAMPLES_MIXED, weight=0.5)
        result_cats = {s["category_name"] for s in result}
        assert "Stylizacja rzęs" not in result_cats
        assert "Medycyna estetyczna" in result_cats

    def test_weight_04_generyczna_zachowuje_wszystkie(self):
        """weight=0.4 < 0.5 + generyczna → boost informacyjny, brak filtrowania."""
        subject = _make_subject("Konsultacja", category="Medycyna estetyczna")
        result, meta = apply_category_context(subject, SAMPLES_MIXED, weight=0.4)
        assert meta["n_out"] == len(SAMPLES_MIXED)
