"""Tests for new bagent endpoints: /api/keywords, /api/ai/text, /api/embeddings."""

import os

os.environ["API_KEY"] = "test-api-key-12345"

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from server import app

client = TestClient(app)

API_KEY = "test-api-key-12345"
HEADERS = {"x-api-key": API_KEY}


# ============================================
# /api/keywords
# ============================================


class TestKeywordsEndpoint:
    def test_keywords_requires_api_key(self, sample_scraped_data):
        response = client.post("/api/keywords", json={"auditId": "a1", "scrapedData": sample_scraped_data})
        assert response.status_code == 422

    def test_keywords_validates_input(self):
        response = client.post("/api/keywords", json={}, headers=HEADERS)
        assert response.status_code == 422

    def test_keywords_returns_structure(self, sample_scraped_data):
        """Keywords endpoint should return keywords, distribution, and suggestions."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="- keratyna: popularne słowo kluczowe\n- botox: zabieg odmładzający")]
        mock_response.usage = MagicMock(input_tokens=100, output_tokens=50)

        with patch("services.minimax.MiniMaxClient") as MockClient:
            instance = MockClient.return_value
            instance.create_message = AsyncMock(return_value=mock_response)

            response = client.post(
                "/api/keywords",
                json={"auditId": "test-kw-1", "scrapedData": sample_scraped_data},
                headers=HEADERS,
            )

        assert response.status_code == 200
        data = response.json()
        assert "keywords" in data
        assert "categoryDistribution" in data
        assert "suggestions" in data
        assert isinstance(data["keywords"], list)
        assert isinstance(data["categoryDistribution"], list)
        assert isinstance(data["suggestions"], list)

    def test_keywords_extracts_known_keywords(self, sample_scraped_data):
        """Rule-based extraction should find keywords from BEAUTY_KEYWORDS in service names."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="- test: test")]
        mock_response.usage = MagicMock(input_tokens=10, output_tokens=10)

        with patch("services.minimax.MiniMaxClient") as MockClient:
            instance = MockClient.return_value
            instance.create_message = AsyncMock(return_value=mock_response)

            response = client.post(
                "/api/keywords",
                json={"auditId": "test-kw-2", "scrapedData": sample_scraped_data},
                headers=HEADERS,
            )

        data = response.json()
        keyword_names = [k["keyword"] for k in data["keywords"]]
        # Sample data has: "Manicure hybrydowy", "Pedicure", "Henna brwi", "Masaż relaksacyjny", "Drenaż limfatyczny"
        assert "manicure" in keyword_names
        assert "pedicure" in keyword_names
        assert "masaż" in keyword_names

    def test_keywords_distribution_per_category(self, sample_scraped_data):
        """Distribution should have one entry per category."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="- test: test")]
        mock_response.usage = MagicMock(input_tokens=10, output_tokens=10)

        with patch("services.minimax.MiniMaxClient") as MockClient:
            instance = MockClient.return_value
            instance.create_message = AsyncMock(return_value=mock_response)

            response = client.post(
                "/api/keywords",
                json={"auditId": "test-kw-3", "scrapedData": sample_scraped_data},
                headers=HEADERS,
            )

        data = response.json()
        cat_names = [d["categoryName"] for d in data["categoryDistribution"]]
        assert "Fryzjerstwo" in cat_names
        assert "Kosmetyka" in cat_names
        assert "Masaż" in cat_names


# ============================================
# /api/ai/text
# ============================================


class TestAiTextEndpoint:
    def test_ai_text_requires_api_key(self):
        response = client.post("/api/ai/text", json={"prompt": "test"})
        assert response.status_code == 422

    def test_ai_text_validates_input(self):
        response = client.post("/api/ai/text", json={}, headers=HEADERS)
        assert response.status_code == 422

    def test_ai_text_returns_text(self):
        """Should return AI-generated text with token counts."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="To jest odpowiedź AI.")]
        mock_response.usage = MagicMock(input_tokens=50, output_tokens=30)

        with patch("services.minimax.MiniMaxClient") as MockClient:
            instance = MockClient.return_value
            instance.create_message = AsyncMock(return_value=mock_response)

            response = client.post(
                "/api/ai/text",
                json={"prompt": "Zasugeruj kategorie", "maxTokens": 2000, "temperature": 0.3},
                headers=HEADERS,
            )

        assert response.status_code == 200
        data = response.json()
        assert "text" in data
        assert data["text"] == "To jest odpowiedź AI."
        assert data["inputTokens"] == 50
        assert data["outputTokens"] == 30

    def test_ai_text_uses_defaults(self):
        """Should use default maxTokens and temperature when not provided."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="ok")]
        mock_response.usage = MagicMock(input_tokens=10, output_tokens=5)

        with patch("services.minimax.MiniMaxClient") as MockClient:
            instance = MockClient.return_value
            instance.create_message = AsyncMock(return_value=mock_response)

            response = client.post(
                "/api/ai/text",
                json={"prompt": "test"},
                headers=HEADERS,
            )

        assert response.status_code == 200
        # Verify create_message was called with defaults
        call_kwargs = instance.create_message.call_args
        assert call_kwargs.kwargs.get("max_tokens", call_kwargs[1].get("max_tokens")) == 4000
        assert call_kwargs.kwargs.get("temperature", call_kwargs[1].get("temperature")) == 0.4


# ============================================
# /api/embeddings
# ============================================


class TestEmbeddingsEndpoint:
    def test_embeddings_requires_api_key(self):
        response = client.post("/api/embeddings", json={"texts": ["test"]})
        assert response.status_code == 422

    def test_embeddings_empty_texts(self):
        """Empty texts list should return empty embeddings."""
        response = client.post("/api/embeddings", json={"texts": []}, headers=HEADERS)
        assert response.status_code == 200
        data = response.json()
        assert data["embeddings"] == []

    def test_embeddings_returns_vectors(self):
        """Should return embedding vectors for each input text."""
        mock_json = {"embeddings": [{"values": [0.1, 0.2, 0.3]}, {"values": [0.4, 0.5, 0.6]}]}

        with patch("httpx.AsyncClient") as MockHttpx:
            mock_client = AsyncMock()
            mock_resp = MagicMock()
            mock_resp.json.return_value = mock_json
            mock_resp.raise_for_status = MagicMock()
            mock_client.post = AsyncMock(return_value=mock_resp)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            MockHttpx.return_value = mock_client

            response = client.post(
                "/api/embeddings",
                json={"texts": ["salon fryzjerski", "manicure hybrydowy"]},
                headers=HEADERS,
            )

        assert response.status_code == 200
        data = response.json()
        assert len(data["embeddings"]) == 2
        assert data["embeddings"][0] == [0.1, 0.2, 0.3]


# ============================================
# PURE LOGIC: keyword extraction (no mocks needed)
# ============================================


class TestKeywordExtractionPure:
    """Test rule-based keyword extraction — pure functions, no AI, no mocks."""

    def test_extract_keywords_finds_beauty_terms(self):
        from pipelines.keywords import extract_keywords

        categories = [
            {
                "name": "Zabiegi",
                "services": [
                    {"name": "Mezoterapia igłowa", "description": "Zabieg odmładzający z kwasem hialuronowym"},
                    {"name": "Peeling kawitacyjny", "description": "Oczyszczanie twarzy"},
                ],
            }
        ]
        keywords = extract_keywords(categories)
        kw_names = [k["keyword"] for k in keywords]
        assert "mezoterapia" in kw_names
        assert "peeling" in kw_names
        assert "oczyszczanie" in kw_names

    def test_extract_keywords_counts_correctly(self):
        from pipelines.keywords import extract_keywords

        categories = [
            {
                "name": "Manicure",
                "services": [
                    {"name": "Manicure klasyczny", "description": ""},
                    {"name": "Manicure hybrydowy", "description": "Trwały manicure"},
                ],
            },
            {
                "name": "Pedicure",
                "services": [
                    {"name": "Pedicure spa", "description": ""},
                ],
            },
        ]
        keywords = extract_keywords(categories)
        manicure = next(k for k in keywords if k["keyword"] == "manicure")
        assert manicure["count"] == 2  # "Manicure klasyczny" + "Manicure hybrydowy" (description says "manicure" but already counted per-service)
        assert "Manicure" in manicure["categories"]

    def test_extract_keywords_empty_input(self):
        from pipelines.keywords import extract_keywords

        assert extract_keywords([]) == []

    def test_calculate_distribution(self):
        from pipelines.keywords import calculate_category_distribution, extract_keywords

        categories = [
            {"name": "Hair", "services": [{"name": "Strzyżenie damskie", "description": "koloryzacja"}]},
            {"name": "Nails", "services": [{"name": "Manicure hybrydowy", "description": ""}]},
        ]
        keywords = extract_keywords(categories)
        dist = calculate_category_distribution(categories, keywords)
        assert len(dist) == 2
        assert all("categoryName" in d for d in dist)
        assert all("keywordCount" in d for d in dist)
