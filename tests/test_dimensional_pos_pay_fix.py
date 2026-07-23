"""pos_pay_by_app = OR(pos_pay_by_app_enabled, pos_market_pay_enabled).

Bug 2026-07-23: dimension czytał tylko pos_pay_by_app_enabled, więc salony
przyjmujące płatność w aplikacji przez pos_market_pay_enabled=True (a
pos_pay_by_app_enabled=False) pokazywały fałszywe "Płatność w aplikacji 0%".
"""
from pipelines.competitor_dimensional_scores import compute_digital_maturity_scores as f


def test_pos_market_pay_counts_as_pay_by_app():
    # salon 98814: pos_pay_by_app=False, pos_market_pay=True → funkcja JEST
    r = f(has_online_services=False, has_online_vouchers=False,
          pos_pay_by_app=False, partner_system="native", pos_market_pay=True)
    assert r["pos_pay_by_app"] == 1.0


def test_neither_flag_is_zero():
    r = f(has_online_services=False, has_online_vouchers=False,
          pos_pay_by_app=False, partner_system="native", pos_market_pay=False)
    assert r["pos_pay_by_app"] == 0.0


def test_pay_by_app_flag_alone_still_counts():
    r = f(has_online_services=False, has_online_vouchers=False,
          pos_pay_by_app=True, partner_system="native", pos_market_pay=None)
    assert r["pos_pay_by_app"] == 1.0


def test_digital_maturity_score_includes_combined_pay():
    # oba pay false, vouchers true, online true → score = 1(online)+1(vouch)+0 = 2
    r = f(has_online_services=True, has_online_vouchers=True,
          pos_pay_by_app=False, partner_system="native", pos_market_pay=False)
    assert r["digital_maturity_score"] == 2.0
    # dodaj pos_market_pay → 3
    r2 = f(has_online_services=True, has_online_vouchers=True,
           pos_pay_by_app=False, partner_system="native", pos_market_pay=True)
    assert r2["digital_maturity_score"] == 3.0
