"""Continuity with the previous audit (BEAUTY_AUDIT-8w15).

A re-audited salon must not get suggestions that contradict what we
recommended last time: applied suggestions are canonical, pending ones
are repeated verbatim.
"""

from types import SimpleNamespace

from pipelines.report import (
    _build_previous_audit_context,
    _reconcile_descriptions_with_previous,
    _reconcile_naming_with_previous,
)


def _svc(name: str, description: str = "") -> SimpleNamespace:
    return SimpleNamespace(name=name, description=description)


def _scraped(*services: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(
        salonName="Salon Test",
        categories=[SimpleNamespace(name="Kategoria", services=list(services))],
    )


def _prev(*transformations: dict) -> dict:
    return {
        "convex_audit_id": "prev123",
        "report_created_at": "2026-08-01T10:00:00+00:00",
        "transformations": list(transformations),
    }


LONG_DESC = (
    "Zabieg keratynowego prostowania włosów z pielęgnacją i wygładzeniem, "
    "efekt utrzymuje się do 6 miesięcy."
)


# ── _build_previous_audit_context ──


def test_applied_name_detected():
    scraped = _scraped(_svc("Keratynowe prostowanie włosów"))
    prev = _prev({
        "type": "name",
        "service_name": "keratyna",
        "before_text": "keratyna",
        "after_text": "Keratynowe prostowanie włosów",
    })
    ctx = _build_previous_audit_context(scraped, prev)
    assert ctx is not None
    assert "keratynowe prostowanie włosów" in ctx["applied_names"]
    assert ctx["pending_names"] == {}
    assert ctx["counts"]["appliedNames"] == 1
    assert "Keratynowe prostowanie włosów" in ctx["prompt_block"]


def test_pending_name_detected():
    scraped = _scraped(_svc("keratyna"))
    prev = _prev({
        "type": "name",
        "service_name": "keratyna",
        "before_text": "keratyna",
        "after_text": "Keratynowe prostowanie włosów",
    })
    ctx = _build_previous_audit_context(scraped, prev)
    assert ctx is not None
    assert ctx["applied_names"] == set()
    assert ctx["pending_names"]["keratyna"]["after"] == "Keratynowe prostowanie włosów"
    assert '"keratyna" → "Keratynowe prostowanie włosów"' in ctx["prompt_block"]


def test_degenerate_suggestion_ignored():
    # przed==po (bd fh2q class) — nothing to enforce
    scraped = _scraped(_svc("Manicure hybrydowy"))
    prev = _prev({
        "type": "name",
        "service_name": "Manicure hybrydowy",
        "before_text": "Manicure hybrydowy",
        "after_text": "Manicure hybrydowy",
    })
    assert _build_previous_audit_context(scraped, prev) is None


def test_no_overlap_returns_none():
    scraped = _scraped(_svc("Zupełnie inna usługa"))
    prev = _prev({
        "type": "name",
        "service_name": "keratyna",
        "before_text": "keratyna",
        "after_text": "Keratynowe prostowanie włosów",
    })
    assert _build_previous_audit_context(scraped, prev) is None


def test_applied_description_detected_after_rename():
    # Salon applied BOTH our rename and our description — the description
    # transformation still references the OLD name.
    scraped = _scraped(_svc("Keratynowe prostowanie włosów", LONG_DESC))
    prev = _prev(
        {
            "type": "name",
            "service_name": "keratyna",
            "before_text": "keratyna",
            "after_text": "Keratynowe prostowanie włosów",
        },
        {
            "type": "description",
            "service_name": "keratyna",
            "before_text": "",
            "after_text": LONG_DESC + "  ",  # whitespace noise must not matter
        },
    )
    ctx = _build_previous_audit_context(scraped, prev)
    assert ctx is not None
    assert "keratynowe prostowanie włosów" in ctx["applied_desc_names"]
    assert ctx["pending_descs"] == {}


def test_pending_description_detected():
    scraped = _scraped(_svc("Keratynowe prostowanie włosów", "stary opis"))
    prev = _prev({
        "type": "description",
        "service_name": "Keratynowe prostowanie włosów",
        "before_text": "",
        "after_text": LONG_DESC,
    })
    ctx = _build_previous_audit_context(scraped, prev)
    assert ctx is not None
    assert ctx["pending_descs"]["keratynowe prostowanie włosów"] == LONG_DESC


# ── _reconcile_naming_with_previous ──


def _naming_result(*transformations: dict, optimized=None, already=0) -> dict:
    return {
        "transformations": list(transformations),
        "coverage": {
            "totalChecked": 10,
            "optimized": len(transformations) if optimized is None else optimized,
            "alreadyOptimal": already,
            "rejected": 0,
        },
    }


def _ctx_names(applied=(), pending=None) -> dict:
    return {
        "applied_names": set(applied),
        "pending_names": pending or {},
        "applied_desc_names": set(),
        "pending_descs": {},
    }


def test_reconcile_drops_proposal_against_applied_name():
    result = _naming_result({
        "type": "name",
        "serviceName": "Keratynowe prostowanie włosów",
        "before": "Keratynowe prostowanie włosów",
        "after": "Prostowanie keratynowe premium",
    })
    out, rec = _reconcile_naming_with_previous(
        result, _ctx_names(applied=["keratynowe prostowanie włosów"])
    )
    assert out["transformations"] == []
    assert rec == {"dropped": 1, "reused": 0, "injected": 0}
    assert out["coverage"]["alreadyOptimal"] == 1
    assert out["coverage"]["optimized"] == 0


def test_reconcile_overrides_fresh_proposal_with_previous_suggestion():
    result = _naming_result({
        "type": "name",
        "serviceName": "keratyna",
        "before": "keratyna",
        "after": "Wymyślona nowa nazwa",
    })
    ctx = _ctx_names(pending={
        "keratyna": {"name": "keratyna", "after": "Keratynowe prostowanie włosów"},
    })
    out, rec = _reconcile_naming_with_previous(result, ctx)
    assert out["transformations"][0]["after"] == "Keratynowe prostowanie włosów"
    assert rec["reused"] == 1
    assert rec["injected"] == 0


def test_reconcile_injects_skipped_pending_suggestion():
    result = _naming_result()  # agent proposed nothing at all
    ctx = _ctx_names(pending={
        "keratyna": {"name": "keratyna", "after": "Keratynowe prostowanie włosów"},
    })
    out, rec = _reconcile_naming_with_previous(result, ctx)
    assert len(out["transformations"]) == 1
    t = out["transformations"][0]
    assert t["before"] == "keratyna"
    assert t["after"] == "Keratynowe prostowanie włosów"
    assert rec == {"dropped": 0, "reused": 0, "injected": 1}
    assert out["coverage"]["optimized"] == 1


def test_reconcile_keeps_matching_proposal_untouched():
    result = _naming_result({
        "type": "name",
        "serviceName": "keratyna",
        "before": "keratyna",
        "after": "Keratynowe prostowanie włosów",
        "reason": "Poprawa nazwy usługi",
    })
    ctx = _ctx_names(pending={
        "keratyna": {"name": "keratyna", "after": "Keratynowe prostowanie włosów"},
    })
    out, rec = _reconcile_naming_with_previous(result, ctx)
    assert rec == {"dropped": 0, "reused": 0, "injected": 0}
    assert out["transformations"][0]["reason"] == "Poprawa nazwy usługi"


# ── _reconcile_descriptions_with_previous ──


def test_reconcile_drops_desc_for_applied_description():
    result = {
        "transformations": [{
            "type": "description",
            "serviceName": "Keratynowe prostowanie włosów",
            "before": "",
            "after": "Nowy wymyślony opis",
        }],
        "coverage": {"totalChecked": 5, "optimized": 1, "alreadyOptimal": 0, "rejected": 0},
    }
    ctx = {
        "applied_names": set(),
        "pending_names": {},
        "applied_desc_names": {"keratynowe prostowanie włosów"},
        "pending_descs": {},
    }
    out, rec = _reconcile_descriptions_with_previous(result, ctx)
    assert out["transformations"] == []
    assert rec == {"dropped": 1, "reused": 0}
    assert out["coverage"]["alreadyOptimal"] == 1


def test_reconcile_overrides_desc_with_pending_previous_suggestion():
    result = {
        "transformations": [{
            "type": "description",
            "serviceName": "Keratynowe prostowanie włosów",
            "before": "",
            "after": "Nowy wymyślony opis",
        }],
        "coverage": {"totalChecked": 5, "optimized": 1, "alreadyOptimal": 0, "rejected": 0},
    }
    ctx = {
        "applied_names": set(),
        "pending_names": {},
        "applied_desc_names": set(),
        "pending_descs": {"keratynowe prostowanie włosów": LONG_DESC},
    }
    out, rec = _reconcile_descriptions_with_previous(result, ctx)
    assert out["transformations"][0]["after"] == LONG_DESC
    assert rec["reused"] == 1


def test_reconcile_short_pending_desc_not_reused():
    # A stub (<50 chars) from the previous audit must not overwrite a real
    # fresh proposal.
    result = {
        "transformations": [{
            "type": "description",
            "serviceName": "Manicure",
            "before": "",
            "after": "Nowy pełnoprawny opis usługi z korzyścią dla klienta.",
        }],
        "coverage": {"totalChecked": 1, "optimized": 1, "alreadyOptimal": 0, "rejected": 0},
    }
    ctx = {
        "applied_names": set(),
        "pending_names": {},
        "applied_desc_names": set(),
        "pending_descs": {"manicure": "krótki stub"},
    }
    out, rec = _reconcile_descriptions_with_previous(result, ctx)
    assert out["transformations"][0]["after"].startswith("Nowy pełnoprawny")
    assert rec["reused"] == 0
