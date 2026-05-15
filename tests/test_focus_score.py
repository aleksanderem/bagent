"""Unit tests dla services/focus_score.py — pure logic, no I/O."""
import numpy as np
import pytest

from services.focus_score import (
    SalonFocusBundle,
    compute_focus_distribution_by_tid,
    compute_focus_distribution_by_variant,
    compute_salon_portfolio_embedding,
    compute_service_focus_weights,
    cosine_similarity_dense,
    cosine_similarity_sparse,
    parse_focus_distribution_jsonb,
)


def make_svc(name="Service", description="", photos=None, tid=None, vid=None, emb=None):
    return {
        "name": name,
        "description": description,
        "photos": photos or [],
        "booksy_treatment_id": tid,
        "variant_id": vid,
        "name_embedding": emb,
    }


def make_emb(seed: int) -> list[float]:
    """Deterministic 1536-d embedding dla testów."""
    rng = np.random.RandomState(seed)
    v = rng.randn(1536)
    return (v / np.linalg.norm(v)).tolist()


# ============================================================================
# compute_service_focus_weights
# ============================================================================

class TestFocusWeights:
    def test_empty_services(self):
        assert compute_service_focus_weights([], []) == []

    def test_baseline_only(self):
        svc = [make_svc(name="Generic")]
        w = compute_service_focus_weights(svc, [])
        assert len(w) == 1
        assert w[0] == pytest.approx(0.05)  # baseline only

    def test_top_service_match(self):
        svc = [make_svc(name="Botoks 1 okolica")]
        w = compute_service_focus_weights(svc, ["Botoks"])
        # baseline + top_service signal
        assert w[0] == pytest.approx(0.45)

    def test_top_service_partial_match_substring(self):
        # Top: "Botoks", svc: "Botoks 2 okolice" — substring match
        svc = [make_svc(name="Botoks 2 okolice")]
        w = compute_service_focus_weights(svc, ["Botoks"])
        assert w[0] == pytest.approx(0.45)

    def test_description_length_normalized(self):
        # Service ze średnim opisem (50 znaków) i bez opisu — pierwszy ma większą wagę
        svc1 = make_svc(name="A", description="x" * 100)
        svc2 = make_svc(name="B", description="x" * 50)
        svc3 = make_svc(name="C", description="")
        weights = compute_service_focus_weights([svc1, svc2, svc3], [])
        # baseline + 0.20 × (100/100) = 0.25
        assert weights[0] == pytest.approx(0.25)
        # baseline + 0.20 × (50/100) = 0.15
        assert weights[1] == pytest.approx(0.15)
        # baseline only
        assert weights[2] == pytest.approx(0.05)

    def test_has_photo_adds_0_15(self):
        svc = [make_svc(name="A", photos=["url1.jpg"])]
        w = compute_service_focus_weights(svc, [])
        assert w[0] == pytest.approx(0.20)  # baseline 0.05 + photo 0.15

    def test_variant_depth(self):
        # Salon z 3 variantami w tid=10 i 1 variantem w tid=20 — usługi w tid=10
        # dostają pełne 0.15, usługi w tid=20 dostają 0.05 (1/3 × 0.15)
        svcs = [
            make_svc(name=f"v{i}", tid=10, vid=100+i) for i in range(3)
        ] + [make_svc(name="t20", tid=20, vid=200)]
        w = compute_service_focus_weights(svcs, [])
        # baseline + 0.15 (max=3, this tid=3, ratio=1.0)
        assert w[0] == pytest.approx(0.20)
        assert w[1] == pytest.approx(0.20)
        assert w[2] == pytest.approx(0.20)
        # baseline + 0.15 × (1/3) = 0.10
        assert w[3] == pytest.approx(0.10)

    def test_full_stack_realistic(self):
        # Service z wszystkimi sygnałami: top + długi opis + zdjęcie + multi-variant
        svcs = [
            make_svc(name="Botoks 1 okolica", description="x" * 200, photos=["a"], tid=245, vid=386),
            make_svc(name="Botoks 2 okolice", description="x" * 200, photos=["b"], tid=245, vid=398),
            make_svc(name="Manicure stub", description="", tid=288, vid=999),
        ]
        w = compute_service_focus_weights(svcs, ["Botoks"])
        # Botoks 1: 0.05 + 0.40 (top) + 0.20 (max desc) + 0.15 (photo) + 0.15 (2/2 variants) = 0.95
        assert w[0] == pytest.approx(0.95)
        # Manicure stub: 0.05 + 0 + 0 + 0 + 0.15 × (1/2) = 0.125
        assert w[2] == pytest.approx(0.125)


# ============================================================================
# focus_distribution
# ============================================================================

class TestFocusDistribution:
    def test_empty(self):
        assert compute_focus_distribution_by_tid([], []) == {}

    def test_all_same_tid(self):
        svcs = [make_svc(tid=10), make_svc(tid=10)]
        w = [0.5, 0.3]
        dist = compute_focus_distribution_by_tid(svcs, w)
        assert dist == {10: pytest.approx(1.0)}

    def test_normalization_to_total(self):
        # tid=10 weight 0.6, tid=20 weight 0.3, tid=30 weight 0.1
        svcs = [make_svc(tid=10), make_svc(tid=20), make_svc(tid=30)]
        w = [0.6, 0.3, 0.1]
        dist = compute_focus_distribution_by_tid(svcs, w)
        assert dist[10] == pytest.approx(0.6)
        assert dist[20] == pytest.approx(0.3)
        assert dist[30] == pytest.approx(0.1)
        assert sum(dist.values()) == pytest.approx(1.0)

    def test_skip_null_tid(self):
        svcs = [make_svc(tid=10), make_svc(tid=None), make_svc(tid=20)]
        w = [0.5, 0.3, 0.2]
        dist = compute_focus_distribution_by_tid(svcs, w)
        # null tid skipped, normalized over remaining 0.5+0.2=0.7
        assert dist[10] == pytest.approx(0.5 / 0.7)
        assert dist[20] == pytest.approx(0.2 / 0.7)

    def test_top_k_truncation(self):
        svcs = [make_svc(tid=i) for i in range(50)]
        w = [1.0 - i / 100 for i in range(50)]  # decreasing weights
        dist = compute_focus_distribution_by_tid(svcs, w, top_k=5)
        assert len(dist) == 5
        # Top 5 should be tids 0..4 (highest weights)
        assert set(dist.keys()) == {0, 1, 2, 3, 4}


# ============================================================================
# Portfolio embedding
# ============================================================================

class TestPortfolioEmbedding:
    def test_empty(self):
        assert compute_salon_portfolio_embedding([], []) is None

    def test_no_embeddings(self):
        svcs = [make_svc()]
        emb = compute_salon_portfolio_embedding(svcs, [0.5])
        assert emb is None

    def test_single_service(self):
        e = make_emb(42)
        svcs = [make_svc(emb=e)]
        portfolio = compute_salon_portfolio_embedding(svcs, [1.0])
        # Single service with weight 1.0 → portfolio = that embedding (L2-normalized)
        assert portfolio is not None
        assert portfolio.shape == (1536,)
        assert np.linalg.norm(portfolio) == pytest.approx(1.0)

    def test_weighted_mean(self):
        # Two embeddings, weight 0.8 on first, 0.2 on second
        e1 = make_emb(1)
        e2 = make_emb(2)
        svcs = [make_svc(emb=e1), make_svc(emb=e2)]
        portfolio = compute_salon_portfolio_embedding(svcs, [0.8, 0.2])
        assert portfolio is not None
        # Portfolio should be closer to e1 than e2 (because weight 0.8 > 0.2)
        assert np.dot(portfolio, e1) > np.dot(portfolio, e2)
        # L2-normalized
        assert np.linalg.norm(portfolio) == pytest.approx(1.0)

    def test_parse_string_embedding(self):
        """Embedding może przyjść jako string z pgvector serializacji."""
        e = make_emb(7)
        e_str = "[" + ",".join(str(x) for x in e) + "]"
        svcs = [make_svc(emb=e_str)]
        portfolio = compute_salon_portfolio_embedding(svcs, [1.0])
        assert portfolio is not None
        assert portfolio.shape == (1536,)


# ============================================================================
# Cosine similarity
# ============================================================================

class TestCosineSimilarity:
    def test_sparse_empty(self):
        assert cosine_similarity_sparse({}, {}) == 0.0
        assert cosine_similarity_sparse({1: 1.0}, {}) == 0.0

    def test_sparse_no_overlap(self):
        assert cosine_similarity_sparse({1: 0.5, 2: 0.5}, {3: 1.0}) == 0.0

    def test_sparse_identical(self):
        d = {1: 0.6, 2: 0.3, 3: 0.1}
        assert cosine_similarity_sparse(d, d) == pytest.approx(1.0)

    def test_sparse_proportional(self):
        # Same direction, different magnitude → cosine = 1
        d1 = {1: 0.6, 2: 0.4}
        d2 = {1: 0.3, 2: 0.2}
        assert cosine_similarity_sparse(d1, d2) == pytest.approx(1.0)

    def test_sparse_orthogonal(self):
        d1 = {1: 1.0}
        d2 = {2: 1.0}
        assert cosine_similarity_sparse(d1, d2) == 0.0

    def test_dense_identical(self):
        a = np.array([1.0, 0.0, 0.0])
        a = a / np.linalg.norm(a)
        assert cosine_similarity_dense(a, a) == pytest.approx(1.0)

    def test_dense_none(self):
        assert cosine_similarity_dense(None, np.array([1.0])) == 0.0


# ============================================================================
# SalonFocusBundle
# ============================================================================

class TestSalonFocusBundle:
    def test_from_services_empty(self):
        b = SalonFocusBundle.from_services(1, 100, [], [])
        assert b.salon_id == 1
        assert b.booksy_id == 100
        assert b.portfolio_embedding is None
        assert b.focus_distribution == {}
        assert b.service_count == 0

    def test_from_services_full(self):
        svcs = [
            make_svc(name="Botoks 1 okolica", tid=245, vid=386, emb=make_emb(1)),
            make_svc(name="Mezoterapia", tid=244, vid=653, emb=make_emb(2)),
        ]
        b = SalonFocusBundle.from_services(1, 100, svcs, ["Botoks"])
        assert b.service_count == 2
        assert b.embedded_count == 2
        assert b.portfolio_embedding is not None
        assert 245 in b.focus_distribution
        assert 244 in b.focus_distribution

    def test_to_db_payload_serialization(self):
        svcs = [make_svc(name="A", tid=10, vid=100, emb=make_emb(1))]
        b = SalonFocusBundle.from_services(1, 100, svcs, [])
        payload = b.to_db_payload()
        # Portfolio embedding jako list
        assert isinstance(payload["portfolio_embedding"], list)
        assert len(payload["portfolio_embedding"]) == 1536
        # Focus distribution keys jako string (jsonb wymaga)
        assert isinstance(payload["focus_distribution"], dict)
        assert all(isinstance(k, str) for k in payload["focus_distribution"])

    def test_parse_focus_distribution_jsonb_roundtrip(self):
        d = {1: 0.5, 2: 0.3, 3: 0.2}
        # Serializing-deserializing should preserve
        as_jsonb = {str(k): float(v) for k, v in d.items()}
        parsed = parse_focus_distribution_jsonb(as_jsonb)
        assert parsed == d

    def test_parse_focus_distribution_jsonb_none(self):
        assert parse_focus_distribution_jsonb(None) == {}
        assert parse_focus_distribution_jsonb([]) == {}
        assert parse_focus_distribution_jsonb({"bad": "not-a-number"}) == {}


# ============================================================================
# Realistic scenario test — Beauty4ever-like z fałszywym konkurentem
# ============================================================================

class TestRealisticScenario:
    def test_focus_separates_real_from_fake_competitor(self):
        """Subject Beauty4ever skupia się na laserze + body shape.
        Fake competitor (high portfolio sim, low focus sim) ma manicure focus.
        """
        # Subject: laser + body shape clinic
        subj_svcs = [
            make_svc(name="Laser CO2 twarz", description="x"*200, photos=["a"],
                     tid=242, vid=2197, emb=make_emb(1)),
            make_svc(name="Laser CO2 dekolt", description="x"*200, photos=["b"],
                     tid=242, vid=2198, emb=make_emb(2)),
            make_svc(name="Endermologia LPG", description="x"*200, photos=["c"],
                     tid=503, vid=677, emb=make_emb(3)),
            make_svc(name="Endermologia pakiet 10", description="x"*200, photos=["d"],
                     tid=503, vid=678, emb=make_emb(4)),
        ]
        subj_top = ["Laser CO2", "Endermologia"]
        subj = SalonFocusBundle.from_services(1, 100, subj_svcs, subj_top)

        # Real competitor: same focus profile (laser + body shape)
        real_svcs = [
            make_svc(name="Laser frakcyjny", description="x"*200, photos=["a"],
                     tid=242, vid=2197, emb=make_emb(1)),
            make_svc(name="LPG Endermologia", description="x"*200, photos=["b"],
                     tid=503, vid=677, emb=make_emb(3)),
        ]
        real = SalonFocusBundle.from_services(2, 200, real_svcs, ["Laser frakcyjny"])

        # Fake competitor: high services but mostly manicure
        fake_svcs = [
            make_svc(name="Manicure hybrydowy", description="x"*200, photos=["a"],
                     tid=288, vid=600, emb=make_emb(99)),
            make_svc(name="Manicure klasyczny", description="x"*200, photos=["b"],
                     tid=288, vid=601, emb=make_emb(98)),
            make_svc(name="Pedicure", description="x"*200, photos=["c"],
                     tid=285, vid=129, emb=make_emb(97)),
            # Marginalne medical service — w cenniku ale nie focus
            make_svc(name="Laser CO2", description="", tid=242, vid=2197,
                     emb=make_emb(1)),
        ]
        fake_top = ["Manicure", "Pedicure"]
        fake = SalonFocusBundle.from_services(3, 300, fake_svcs, fake_top)

        # Focus tid sim
        real_focus = cosine_similarity_sparse(
            subj.focus_distribution, real.focus_distribution,
        )
        fake_focus = cosine_similarity_sparse(
            subj.focus_distribution, fake.focus_distribution,
        )

        # Real competitor focus_tid_sim much higher
        assert real_focus > 0.8, f"real focus too low: {real_focus}"
        assert fake_focus < 0.2, f"fake focus too high: {fake_focus}"
        assert real_focus > fake_focus * 2

    def test_baseline_smoothing_prevents_zero_division(self):
        """Każda usługa dostaje baseline 0.05 — żeby salon z 0 sygnałów (no top,
        no desc, no photos, no variants) miał sensowny focus distribution."""
        svcs = [make_svc(name="A", tid=10), make_svc(name="B", tid=20)]
        bundle = SalonFocusBundle.from_services(1, 100, svcs, [])
        # Powinno być 50/50 — bo obie usługi mają baseline 0.05
        assert bundle.focus_distribution[10] == pytest.approx(0.5)
        assert bundle.focus_distribution[20] == pytest.approx(0.5)
