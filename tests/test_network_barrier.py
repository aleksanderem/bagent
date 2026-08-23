"""Testy samej bariery sieciowej z tests/conftest.py (BEAUTY_AUDIT-x67b).

Bariera bez testu to bariera, ktora cicho gnije — wystarczy zmiana kolejnosci
fixture'ow albo innego zachowania monkeypatch na klasach i zestaw znowu zacznie
po cichu wychodzic do sieci, zielony jak zawsze. Dokladnie ten tryb awarii
naprawiamy, wiec kanarek zostaje w repo na stale zamiast zniknac po jednym
przebiegu.

Kazdy test tutaj wali w bariere CELOWO i sprzata po sobie licznik prob przez
fixture `barrier_attempt_log`, zeby podsumowanie sesji dalej znaczylo "zero
niezamierzonych prob wyjscia do sieci".
"""

from __future__ import annotations

import contextlib
import os
import socket
from collections.abc import Callable

import pytest

from tests.conftest import NetworkAccessInTestError


def test_dns_lookup_is_blocked_and_recorded(
    barrier_attempt_log: Callable[[], list[str]],
) -> None:
    with pytest.raises(NetworkAccessInTestError) as excinfo:
        socket.getaddrinfo("api.openai.com", 443)

    assert "api.openai.com:443" in str(excinfo.value)
    assert any("api.openai.com:443" in entry for entry in barrier_attempt_log())


def test_barrier_is_not_swallowed_by_except_exception(
    barrier_attempt_log: Callable[[], list[str]],
) -> None:
    """Sedno sprawy: kod produkcyjny lapie `except Exception` i cicho robi
    fallback. Bariera musi przez to przelatywac, inaczej test zostaje zielony i
    dalej nie wiadomo, ktora sciezka byla wykonana."""
    with pytest.raises(NetworkAccessInTestError):
        try:
            socket.getaddrinfo("api.openai.com", 443)
        except Exception:  # noqa: BLE001
            pytest.fail("`except Exception` przechwycil bariere sieciowa")

    assert len(barrier_attempt_log()) == 1


def test_raw_ip_connect_is_blocked(
    barrier_attempt_log: Callable[[], list[str]],
) -> None:
    """Polaczenie po samym IP omija DNS — connect() tez musi byc zablokowany.

    192.0.2.1 to TEST-NET-1 (RFC 5737): adres, ktory nigdy nie jest routowalny,
    wiec gdyby bariera nie zadzialala, test i tak nie dobilby sie nigdzie realnego.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with contextlib.closing(sock), pytest.raises(NetworkAccessInTestError):
        sock.connect(("192.0.2.1", 443))

    assert any("192.0.2.1:443" in entry for entry in barrier_attempt_log())


def test_loopback_is_allowed_but_recorded() -> None:
    """Loopback przechodzi (Redis/sidecar na maszynie deva), ale zostaje slad
    w podsumowaniu sesji — zaleznosc od srodowiska ma byc widoczna, nie cicha.

    getaddrinfo na literalnym IP nie wychodzi do zadnego resolvera, wiec ten
    test niczego nie odpytuje po sieci.
    """
    from tests.conftest import local_network_attempts

    before = len(local_network_attempts)
    socket.getaddrinfo("127.0.0.1", 6379)
    new_entries = local_network_attempts[before:]

    assert any("127.0.0.1:6379" in entry for entry in new_entries)
    del local_network_attempts[before:]


@pytest.mark.integration
def test_integration_marked_tests_are_exempt() -> None:
    """Bariera NIE moze dotykac testow integracyjnych — te maja prawo do sieci.

    Sprawdzamy sam fakt braku podmiany socketa, bez wykonywania ruchu, wiec ten
    test jest darmowy i nie potrzebuje zadnych kluczy mimo markera.
    """
    assert socket.getaddrinfo.__name__ != "guard_getaddrinfo"
    assert socket.socket.connect.__name__ != "guard_connect"
    assert socket.socket.connect_ex.__name__ != "guard_connect_ex"


def test_provider_api_keys_are_stripped_from_environ() -> None:
    """Pierwsza warstwa: klucze wstrzykniete przez `load_dotenv()` w server.py
    nie moga byc widoczne dla testu offline."""
    for key in ("OPENAI_API_KEY", "QDRANT_URL", "QDRANT_API_KEY"):
        assert os.environ.get(key) is None, f"{key} wciaz widoczny w tescie offline"
