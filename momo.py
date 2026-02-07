import csv
import time
import re
from pathlib import Path
from shutil import which
import requests
from multiprocessing import Pool, cpu_count

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service


TELEGRAM_TOKEN = "7987151223:AAHKtQldEIJZErrm4z2nrRKsnjGRnl99o80"
TELEGRAM_CHAT_ID = "-1003833683489"

ISBN_FILE = "isbns.txt"
OUTPUT_CSV = "resultats_momox.csv"

BASE_HOME = "https://www.momox.fr/"
BASE_OFFER = "https://www.momox.fr/offer/{}"

WORKERS = 2
WAIT_SECONDS = 12


def tg_send_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=20)


def tg_send_file(path: str, caption: str = ""):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    with open(path, "rb") as f:
        requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
            files={"document": f},
            timeout=60
        )


def accept_cookies_shadow(driver, timeout=8):
    end = time.time() + timeout
    while time.time() < end:
        clicked = driver.execute_script("""
            const host = document.querySelector('#cmpwrapper');
            if (!host || !host.shadowRoot) return false;
            const root = host.shadowRoot;
            const spanTxt = root.querySelector('#cmpbntyestxt');
            if (!spanTxt) return false;
            const btn = spanTxt.closest('a');
            if (!btn) return false;
            try { btn.scrollIntoView({block:'center'}); } catch(e) {}
            btn.click();
            return true;
        """)
        if clicked:
            return True
        time.sleep(0.10)
    return False


def ensure_offer_page(driver, isbn: str) -> bool:
    url = driver.current_url or ""
    return ("/offer/" in url) and (isbn in url)


def is_not_bought_message_present(driver) -> bool:
    try:
        txt = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
    except Exception:
        return False
    return (
        "nous n'achetons malheureusement pas" in txt
        or "nous n’achetons malheureusement pas" in txt
        or "nous n'achetons pas cet article" in txt
        or "nous n’achetons pas cet article" in txt
    )


def extract_title(driver) -> str:
    try:
        t = (driver.find_element(By.XPATH, "//h1").text or "").strip()
        if t:
            return t
    except Exception:
        pass
    try:
        return (driver.title or "").strip()
    except Exception:
        return ""


def price_to_float(price_str: str) -> float:
    if not price_str:
        return 0.0
    s = price_str.replace("\u00a0", " ").strip()
    m = re.search(r"(\d+(?:[.,]\d{1,2})?)\s*€", s)
    return float(m.group(1).replace(",", ".")) if m else 0.0


def extract_main_price(driver, wait) -> str:
    wait.until(lambda d: d.find_elements(By.ID, "buttonAddToCart"))

    # stop loading ASAP
    try:
        driver.execute_script("window.stop();")
    except Exception:
        pass

    els = driver.find_elements(
        By.CSS_SELECTOR,
        ".searchresult-price-block .text-xxl span.text-blackRedesign"
    )
    if els:
        txt = (els[0].text or "").strip()
        if "€" in txt:
            return txt

    btn = driver.find_element(By.ID, "buttonAddToCart")
    block = btn.find_element(
        By.XPATH, "./ancestor::*[contains(@class,'searchresult-price-block')][1]"
    )
    price_el = block.find_element(By.CSS_SELECTOR, ".text-xxl span.text-blackRedesign")
    return (price_el.text or "").strip()


def make_driver(worker_id: int):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=fr-FR")

    # EAGER = ne pas attendre images/scripts etc
    options.page_load_strategy = "eager"

    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # Profil persistant par worker (cookies consent)
    options.add_argument(f"--user-data-dir=/tmp/momox_profile_{worker_id}")

    # prefs perf
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2,
    }
    options.add_experimental_option("prefs", prefs)

    chrome_path = (
        which("chromium-browser")
        or which("chromium")
        or which("google-chrome")
        or which("google-chrome-stable")
    )
    driver_path = which("chromedriver")
    if not chrome_path:
        raise RuntimeError("Chrome/Chromium introuvable.")
    if not driver_path:
        raise RuntimeError("chromedriver introuvable.")

    options.binary_location = chrome_path
    service = Service(driver_path)

    driver = webdriver.Chrome(service=service, options=options)

    # block heavy resources + trackers
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd("Network.setBlockedURLs", {
        "urls": [
            "*.png","*.jpg","*.jpeg","*.gif","*.webp","*.svg",
            "*.woff","*.woff2","*.ttf","*.css",
            "*doubleclick*","*googlesyndication*","*google-analytics*","*googletagmanager*","*gtm*"
        ]
    })

    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"}
    )

    driver.set_page_load_timeout(18)
    return driver


def run_batch(args):
    worker_id, isbns_chunk = args
    driver = make_driver(worker_id)
    wait = WebDriverWait(driver, WAIT_SECONDS)

    bought_local = []
    try:
        driver.get(BASE_HOME)
        accept_cookies_shadow(driver, 10)

        for isbn in isbns_chunk:
            driver.get(BASE_OFFER.format(isbn))
            accept_cookies_shadow(driver, 2)

            if not ensure_offer_page(driver, isbn):
                driver.get(BASE_OFFER.format(isbn))
                accept_cookies_shadow(driver, 2)
            if not ensure_offer_page(driver, isbn):
                continue

            # wait only what we need: either buy button OR not-bought text
            try:
                wait.until(lambda d: d.find_elements(By.ID, "buttonAddToCart") or "Nous n’achetons malheureusement pas" in d.page_source)
            except Exception:
                continue

            if is_not_bought_message_present(driver):
                continue

            title = extract_title(driver)
            try:
                price_str = extract_main_price(driver, wait)
                val = price_to_float(price_str)
                if val > 0:
                    bought_local.append((isbn, title, val, price_str))
            except Exception:
                continue

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return bought_local


def main():
    if not Path(ISBN_FILE).exists():
        tg_send_message("❌ isbns.txt introuvable sur le serveur.")
        return

    isbns = [l.strip() for l in Path(ISBN_FILE).read_text(encoding="utf-8").splitlines() if l.strip()]
    if not isbns:
        tg_send_message("❌ isbns.txt est vide.")
        return

    workers = max(1, min(WORKERS, cpu_count()))
    tg_send_message(f"🔄 Analyse en cours… ({len(isbns)} ISBN) | Workers: {workers}")

    chunks = [isbns[i::workers] for i in range(workers)]
    args = [(idx, chunks[idx]) for idx in range(workers)]

    all_bought = []
    with Pool(processes=workers) as pool:
        for result in pool.map(run_batch, args):
            all_bought.extend(result)

    all_bought.sort(key=lambda x: x[2], reverse=True)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["ISBN", "Titre", "Prix"])
        for isbn, title, _, price_str in all_bought:
            w.writerow([isbn, title, price_str])

    if not all_bought:
        tg_send_message("❌ Analyse terminée : aucun livre racheté trouvé.")
        return

    tg_send_message(f"✅ Analyse terminée. Livres rachetés: {len(all_bought)}. Envoi du CSV…")
    tg_send_file(OUTPUT_CSV, caption="📄 Résultat Momox (trié du + cher au - cher)")


if __name__ == "__main__":
    main()
