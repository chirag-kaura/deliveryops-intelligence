"""Poll local DeliveryOps UI and container endpoints until ready."""

import sys
import time
import urllib.error
import urllib.request

REQUIRED_URLS = (
    ("FastAPI", "http://127.0.0.1:8001/health"),
    ("Streamlit", "http://127.0.0.1:8501"),
)

OPTIONAL_CONTAINER_URLS = (
    ("Postgres UI", "http://127.0.0.1:8082"),
    ("Redpanda Console", "http://127.0.0.1:8081"),
    ("MinIO Console", "http://127.0.0.1:9001"),
    ("Jaeger UI", "http://127.0.0.1:16686"),
    ("LocalStack AWS", "http://127.0.0.1:4566/_localstack/health"),
)


def check_url(url: str) -> bool:
    """Return True if the HTTP endpoint responds with status < 400."""
    try:
        with urllib.request.urlopen(url, timeout=1.5) as resp:
            return int(resp.status) < 400
    except (urllib.error.URLError, OSError):
        return False


def main() -> int:
    """Wait up to 25 seconds for FastAPI and Streamlit, and report container UI status."""
    deadline = time.time() + 25.0
    ready = {name: False for name, _ in REQUIRED_URLS}
    while time.time() < deadline:
        for name, url in REQUIRED_URLS:
            if not ready[name] and check_url(url):
                ready[name] = True
        if all(ready.values()):
            break
        time.sleep(0.5)

    if not all(ready.values()):
        missing = [name for name, ok in ready.items() if not ok]
        print(f"[WARN] Timed out waiting for: {', '.join(missing)}")
        return 1

    container_status = [
        f"{name}={'UP' if check_url(url) else 'OFFLINE'}"
        for name, url in OPTIONAL_CONTAINER_URLS
    ]
    print(
        "[OK] FastAPI (8001) & Streamlit (8501) are LIVE | "
        + " | ".join(container_status)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
