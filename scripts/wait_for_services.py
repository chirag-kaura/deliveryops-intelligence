"""Poll local FastAPI and Streamlit endpoints until both respond with HTTP 200."""

import sys
import time
import urllib.error
import urllib.request

URLS = (
    ("FastAPI", "http://127.0.0.1:8001/health"),
    ("Streamlit", "http://127.0.0.1:8501"),
)


def check_url(url: str) -> bool:
    """Return True if the HTTP endpoint responds with status 200."""
    try:
        with urllib.request.urlopen(url, timeout=1.5) as resp:
            return int(resp.status) == 200
    except (urllib.error.URLError, OSError):
        return False


def main() -> int:
    """Wait up to 20 seconds for FastAPI and Streamlit to start listening."""
    deadline = time.time() + 20.0
    ready = {name: False for name, _ in URLS}
    while time.time() < deadline:
        for name, url in URLS:
            if not ready[name] and check_url(url):
                ready[name] = True
        if all(ready.values()):
            print("[OK] FastAPI (127.0.0.1:8001) and Streamlit (127.0.0.1:8501) are LIVE.")
            return 0
        time.sleep(0.5)

    missing = [name for name, ok in ready.items() if not ok]
    print(f"[WARN] Timed out waiting for: {', '.join(missing)}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
