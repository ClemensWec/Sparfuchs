"""Selenium E2E test fixtures — headless Chrome + FastAPI dev server."""
from __future__ import annotations

import os
import socket
import threading
import time
from pathlib import Path

import pytest
import uvicorn
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

SCREENSHOT_DIR = Path(__file__).resolve().parent / "screenshots"

# ── helpers ──────────────────────────────────────────────────────────────

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _UvicornThread(threading.Thread):
    """Run the FastAPI app in a background thread for tests."""

    def __init__(self, host: str, port: int):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.server = None

    def run(self):
        config = uvicorn.Config(
            "app.main:app",
            host=self.host,
            port=self.port,
            log_level="warning",
        )
        self.server = uvicorn.Server(config)
        self.server.run()

    def stop(self):
        if self.server:
            self.server.should_exit = True


# ── fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def base_url():
    """Start the FastAPI dev server on a free port, return base URL."""
    port = _free_port()
    host = "127.0.0.1"
    thread = _UvicornThread(host, port)
    thread.start()

    # Wait for server to be ready
    url = f"http://{host}:{port}"
    for _ in range(60):
        try:
            import urllib.request
            urllib.request.urlopen(f"{url}/api/category-tiles", timeout=2)
            break
        except Exception:
            time.sleep(0.5)
    else:
        raise RuntimeError(f"Server did not start on {url}")

    yield url

    thread.stop()


@pytest.fixture(scope="session")
def browser():
    """Headless Chrome browser (shared across all tests in session)."""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--force-device-scale-factor=1")
    # German locale
    opts.add_argument("--lang=de-DE")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.implicitly_wait(5)

    yield driver

    driver.quit()


@pytest.fixture(autouse=True)
def _screenshot_on_failure(request, browser):
    """Take a screenshot automatically when a test fails."""
    yield
    if request.node.rep_call and request.node.rep_call.failed:
        SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        name = request.node.name.replace("[", "_").replace("]", "")
        path = SCREENSHOT_DIR / f"FAIL_{name}.png"
        browser.save_screenshot(str(path))
        print(f"\n  Screenshot saved: {path}")


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Attach test result to item so _screenshot_on_failure can read it."""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, f"rep_{rep.when}", rep)


def save_screenshot(browser, name: str) -> Path:
    """Save a named screenshot and return its path."""
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = SCREENSHOT_DIR / f"{name}.png"
    browser.save_screenshot(str(path))
    return path
