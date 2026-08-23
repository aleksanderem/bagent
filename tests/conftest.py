"""Shared test fixtures for bagent tests."""

from __future__ import annotations

import socket
from collections.abc import Callable, Iterator
from typing import Any, NoReturn

import pytest

# ---------------------------------------------------------------------------
# Hermetycznosc zestawu offline (BEAUTY_AUDIT-x67b)
# ---------------------------------------------------------------------------
# `server.py` wola `load_dotenv()` na imporcie — swiadomie, bo Bugsink DSN musi
# byc w os.environ zanim ruszy sentry_sdk.init. Skutek uboczny: wystarczy, ze
# JEDEN test zaimportuje `server` (robi to np. tests/test_free_report.py), a
# klucze z `.env` widzi caly proces pytest — takze testy, ktore "przeciez nie
# maja klucza". Tak `test_competitor_analysis.py` chodzil po embeddingi do
# api.openai.com i byl zielony w OBIE strony: z kluczem szedl sciezka OpenAI,
# bez klucza cichym fallbackiem na trigramy, a asercje nie odrozniaja jednego
# od drugiego. Zielony wynik nie mowil, ktora sciezka byla testowana.
#
# Dwie warstwy, obie tylko dla testow BEZ markera `integration` / `e2e`:
#   1. `_strip_provider_api_keys` — kasuje z os.environ klucze czytane wprost
#      przez konstruktory klientow (OpenAI(), QdrantClient(...)).
#   2. `_block_network_access` — twarda bariera na socket: kazda proba DNS albo
#      connect() wychodzi jako BLAD TESTU z nazwa hosta, zamiast cicho wpasc w
#      fallback.
# Naprawa siedzi w warstwie testow. `load_dotenv()` w server.py zostaje.

#: Klucze kasowane przed kazdym testem offline. Lista jest krotka celowo — sa
#: tu WYLACZNIE zmienne, ktore (a) sa w `.env` i (b) kod czyta bezposrednio z
#: os.environ przy budowie klienta, wiec ich skasowanie naprawde odcina ruch:
#:   OPENAI_API_KEY        → OpenAI() / AsyncOpenAI() bez argumentow
#:                           (services/taxonomy_inference.py, openai_synthesis.py,
#:                           embeddings.py, meta_ads_insights.py, workers/…)
#:   QDRANT_URL / _API_KEY → services/similarity_pricing/qdrant_search.get_client
#: MINIMAX_API_KEY, SUPABASE_*, CONVEX_*, API_KEY, GEMINI_API_KEY celowo NIE sa
#: tutaj: te wartosci ida przez `config.Settings`, ktore pydantic-settings czyta
#: wprost z PLIKU `.env` przy imporcie config.py. Usuniecie ich z os.environ
#: niczego nie zmienia (settings i tak ma wartosc), wiec dawaloby zludzenie
#: ochrony. Za te sciezki odpowiada bariera sieciowa nizej.
_PROVIDER_ENV_KEYS = ("OPENAI_API_KEY", "QDRANT_URL", "QDRANT_API_KEY")


class NetworkAccessInTestError(BaseException):
    """Test offline probowal ruchu sieciowego.

    Dziedziczy po BaseException, NIE po Exception — i to jest cala sol tej
    klasy. Kod produkcyjny jest gesty od `except Exception:` z cichym
    fallbackiem (`_embed_batch`, `_call_infer_rpc`, `search_twins`…). Gdyby
    bariera rzucala zwyklym Exception, produkcja polknelaby ja tak samo jak
    prawdziwy timeout, test zostalby zielony i bylby dokladnie tak samo
    nieinformatywny jak przed naprawa.
    """


#: Proby ruchu sieciowego zablokowane w tej sesji ("nodeid -> host:port").
#: Pusta lista po przebiegu = dowod hermetycznosci, nie tylko "zielono".
#: Wypisywana przez `pytest_terminal_summary` nizej.
blocked_network_attempts: list[str] = []

#: Polaczenia na loopback (Redis, sidecar embeddingow) — PRZEPUSZCZANE, ale
#: wypisywane w podsumowaniu. To nadal zaleznosc od srodowiska maszyny, tyle ze
#: tania i lokalna; blokada wywalilaby dzis tests/test_http_error_log.py, ktory
#: przez TestClient(app) uruchamia lifespan i puli arq/Redis. Widoczna lista >
#: cicha zgoda.
local_network_attempts: list[str] = []

#: Hosty uznawane za lokalne. Wszystko poza ta lista = twardy blad testu.
_LOCAL_HOSTS = frozenset({"", "localhost", "localhost.localdomain", "0.0.0.0", "::1", "::"})


def _is_offline_test(request: pytest.FixtureRequest) -> bool:
    """Testy z markerem `integration` albo `e2e` maja prawo do sieci."""
    return all(
        request.node.get_closest_marker(marker) is None
        for marker in ("integration", "e2e")
    )


def _is_local_host(host: Any) -> bool:
    if host is None:
        return True
    text = str(host).strip().strip("[]").lower()
    return text in _LOCAL_HOSTS or text.startswith("127.")


def _describe_address(address: Any) -> str:
    if isinstance(address, tuple) and address:
        port = address[1] if len(address) > 1 else "?"
        return f"{address[0]}:{port}"
    return str(address)


def _address_host(address: Any) -> Any:
    return address[0] if isinstance(address, tuple) and address else address


@pytest.fixture(autouse=True)
def _strip_provider_api_keys(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zdejmuje klucze dostawcow z os.environ na czas testu offline.

    Zakres FUNKCJI, nie sesji — klucz wchodzi do os.environ dopiero przy
    imporcie `server` (czyli w trakcie kolekcji/pierwszego testu), wiec fixture
    sesyjna zdazylaby go skasowac ZANIM dotenv go tam wstawi.
    """
    if not _is_offline_test(request):
        return
    for key in _PROVIDER_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def _block_network_access(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    """Twarda bariera: zaden test offline nie wychodzi poza maszyne.

    Lapiemy DNS (`socket.getaddrinfo`, przez ktory idzie i httpx, i
    `socket.create_connection`, i `loop.getaddrinfo`) oraz samo
    `socket.socket.connect(_ex)` dla AF_INET/AF_INET6 — czyli takze polaczenie
    po samym IP, bez rozwiazywania nazwy. Gniazda AF_UNIX i socketpair
    (asyncio) przechodza bez zmian, loopback przechodzi i jest raportowany.
    """
    if not _is_offline_test(request):
        yield
        return

    real_getaddrinfo = socket.getaddrinfo
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    node_id = request.node.nodeid

    def _refuse(target: str) -> NoReturn:
        blocked_network_attempts.append(f"{node_id} -> {target}")
        raise NetworkAccessInTestError(
            f"Test offline '{node_id}' probowal polaczyc sie z {target}. "
            "Zamockuj warstwe sieciowa na najnizszym seamie modulu (np. "
            "services.taxonomy_inference._embed_batch) albo oznacz test "
            "@pytest.mark.integration."
        )

    def _note_local(target: str) -> None:
        local_network_attempts.append(f"{node_id} -> {target}")

    def guard_getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any) -> Any:
        target = f"{host}:{port}"
        if not _is_local_host(host):
            _refuse(target)
        _note_local(target)
        return real_getaddrinfo(host, port, *args, **kwargs)

    def guard_connect(self: socket.socket, address: Any, *args: Any, **kwargs: Any) -> Any:
        if self.family in (socket.AF_INET, socket.AF_INET6):
            if not _is_local_host(_address_host(address)):
                _refuse(_describe_address(address))
            _note_local(_describe_address(address))
        return real_connect(self, address, *args, **kwargs)

    def guard_connect_ex(self: socket.socket, address: Any, *args: Any, **kwargs: Any) -> Any:
        if self.family in (socket.AF_INET, socket.AF_INET6):
            if not _is_local_host(_address_host(address)):
                _refuse(_describe_address(address))
            _note_local(_describe_address(address))
        return real_connect_ex(self, address, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", guard_getaddrinfo)
    monkeypatch.setattr(socket.socket, "connect", guard_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guard_connect_ex)
    yield


@pytest.fixture
def barrier_attempt_log() -> Iterator[Callable[[], list[str]]]:
    """Zwraca getter na proby zablokowane W TRAKCIE tego testu i sprzata je.

    Kanarek z tests/test_network_barrier.py wali w bariere CELOWO — bez tego
    jego wpisy trafilyby do podsumowania sesji i podsumowanie przestaloby
    znaczyc "zero niezamierzonych prob wyjscia do sieci".
    """
    start = len(blocked_network_attempts)

    def entries() -> list[str]:
        return list(blocked_network_attempts[start:])

    yield entries
    del blocked_network_attempts[start:]


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:  # noqa: ANN001, ARG001
    """Wypisuje na koncu sesji caly ruch sieciowy testow offline."""
    terminalreporter.section("HERMETYCZNOSC: ruch sieciowy w testach offline")
    if blocked_network_attempts:
        terminalreporter.write_line("ZABLOKOWANE (wyjscie poza maszyne):")
        for attempt in blocked_network_attempts:
            terminalreporter.write_line(f"  {attempt}")
    else:
        terminalreporter.write_line("zablokowane: 0 — nic nie probowalo wyjsc poza maszyne")

    if local_network_attempts:
        terminalreporter.write_line(
            f"loopback (przepuszczone, {len(local_network_attempts)}) — zaleznosc od "
            "uslug na maszynie:"
        )
        for attempt in sorted(set(local_network_attempts)):
            terminalreporter.write_line(f"  {attempt}")
    else:
        terminalreporter.write_line("loopback: 0")


@pytest.fixture
def sample_scraped_data() -> dict:
    """Small salon with 3 categories, ~10 services."""
    return {
        "salonName": "Beauty Salon Test",
        "salonAddress": "ul. Testowa 1, Warszawa",
        "salonLogoUrl": None,
        "totalServices": 10,
        "categories": [
            {
                "name": "Fryzjerstwo",
                "services": [
                    {
                        "name": "Strzyżenie damskie",
                        "price": "120 zł",
                        "duration": "45 min",
                        "description": "Profesjonalne strzyżenie",
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Koloryzacja",
                        "price": "250 zł",
                        "duration": "120 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Modelowanie",
                        "price": "80 zł",
                        "duration": "30 min",
                        "description": "Suszenie i modelowanie",
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "STRZYŻENIE MĘSKIE...",
                        "price": "60 zł",
                        "duration": "30 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                ],
            },
            {
                "name": "Kosmetyka",
                "services": [
                    {
                        "name": "Manicure hybrydowy",
                        "price": "100 zł",
                        "duration": "60 min",
                        "description": "Trwały manicure z lakierem hybrydowym",
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Pedicure",
                        "price": "120 zł",
                        "duration": "75 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Henna brwi +rzęs",
                        "price": "od 50 zł",
                        "duration": "30 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                ],
            },
            {
                "name": "Masaż",
                "services": [
                    {
                        "name": "Masaż relaksacyjny",
                        "price": "150 zł",
                        "duration": "60 min",
                        "description": "Relaksujący masaż całego ciała",
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Masaż sportowy",
                        "price": "180 zł",
                        "duration": "60 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Drenaż limfatyczny",
                        "price": "200 zł",
                        "duration": "90 min",
                        "description": "Wspomaganie krążenia limfatycznego",
                        "imageUrl": None,
                        "variants": [
                            {"label": "Nogi", "price": "100 zł", "duration": "45 min"},
                            {"label": "Całe ciało", "price": "200 zł", "duration": "90 min"},
                        ],
                    },
                ],
            },
        ],
    }


@pytest.fixture
def large_scraped_data() -> dict:
    """Large salon with many services for stress testing."""
    categories = []
    service_count = 0
    for i in range(15):
        services = []
        for j in range(12):
            services.append(
                {
                    "name": f"Usługa {i + 1}-{j + 1}",
                    "price": f"{50 + j * 20} zł",
                    "duration": f"{30 + j * 15} min",
                    "description": f"Opis usługi {i + 1}-{j + 1}" if j % 2 == 0 else None,
                    "imageUrl": None,
                    "variants": None,
                }
            )
            service_count += 1
        categories.append({"name": f"Kategoria {i + 1}", "services": services})
    return {
        "salonName": "Duży Salon Beauty",
        "salonAddress": "ul. Długa 10, Kraków",
        "salonLogoUrl": None,
        "totalServices": service_count,
        "categories": categories,
    }


@pytest.fixture
def api_key() -> str:
    """Test API key matching config."""
    return "test-api-key-12345"
