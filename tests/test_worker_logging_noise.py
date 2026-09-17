"""Log workera nie może zapisywać każdego zapytania HTTP.

17.09: bagent-worker-error.log urósł do 22 GB (~1,6 GB na dobę). W próbce
200 tys. linii z końca 199 407 to „httpx INFO HTTP Request: …” — zapis każdego
zapytania do bazy, zero wartości diagnostycznej. Błędy HTTP i tak wychodzą
jako wyjątki z naszego kodu, a httpx/httpcore na WARNING dalej pokazują
problemy z połączeniem.
"""

from __future__ import annotations

import logging

from workers.main import configure_worker_logging


def test_http_client_request_lines_are_silenced():
    configure_worker_logging()

    assert not logging.getLogger("httpx").isEnabledFor(logging.INFO)
    assert not logging.getLogger("httpcore").isEnabledFor(logging.INFO)
    assert logging.getLogger("httpx").isEnabledFor(logging.WARNING)


def test_our_own_progress_logs_stay_on_info():
    configure_worker_logging()

    for name in ("pipelines", "services", "agent", "bagent.workers", "arq.worker"):
        assert logging.getLogger(name).isEnabledFor(logging.INFO), name
