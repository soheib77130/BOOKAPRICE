import csv
import time
import re
from pathlib import Path
from shutil import which
import requests
from multiprocessing import Pool

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service


# ===================== TELEGRAM =====================
TELEGRAM_TOKEN = "7987151223:AAHKtQldEIJZErrm4z2nrRKsnjGRnl99o80"
TELEGRAM_CHAT_ID = "-1003833683489"

# ===================== FILES =====================
ISBN_FILE = "isbns.txt"
OUTPUT_CSV = "resultats_momox.csv"

BASE_HOME = "https://www.momox.fr/"
BASE_OFFER = "https://www.momox.fr/offer/{}"

# ===================== SPEED (ULTIMATE) =====================
WORKERS = 5                 # ✅ 5 navigateurs en parallèle
WAIT_SECONDS = 9            # ✅ plus agressif (augmente si ça rate)
PAGELOAD_TIMEOUT = 14       # ✅ plus agressif (augmente si timeout)
COOKIE_TIMEOUT_HOME = 6
COOKIE_TIMEOUT_OFFER = 1.8


# ===================== TELEGRAM (session reuse) =====================
TG = requests.Session()

def tg_send_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        TG.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=15)
    except Exception:
        pass

def tg_send_file(path: str, caption: str = ""):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    try:
        with open(path, "rb") as f:
            TG.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                files={"document": f},
                timeout=60
            )
    except Exception:
        pass


# ===================== HELPERS =====================
_price_re = re.compile(r"(\d+(?:[.,]\d{1,2})?)\s*€")

def accept_cookies_shadow(driver, timeout=2.5):
    """Clique sur 'OK, compris !' dans le banner cookies (shadow DOM) si présent."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            clicked = driver.execute_script("""
                const host = document.querySelector('#cmpwrapper');
                if (!host || !host.shadowRoot) return false;
                const btn = host.shadowRoot.querySelector('#cmpbntyestxt')?.closest('a');
                if (!btn) return false;
                try { btn.click(); } catch(e) {}
                return true;
            """)
            if clicked:
                return True
        except Exception:
            pass
        time.sleep(0.08)
    return False

def ensure_offer_page(driver, isbn: str) -> bool:
    url = driver.current_url or ""
    return ("/offer/" in url) and (isbn in url)

def is_not_bought_message_present(driver) -> bool:
    # body.text (pas page_source) pour éviter coût énorme
    try:
        txt = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
    except Exception:
        return False
    return ("nous n'achetons" in txt) or ("nous n’achetons" in txt)

def extract_title(driver) -> str:
    try:
        t = (driver.find_element(By.XPATH, "//h1").text or "").strip()
        return t if t else ""
    except Exception:
        return (driver.title or "").strip()

def price_to_float(price_str: str) -> float:
    if not price_str:
        return 0.0
    s = price_str.replace("\u00a0", " ").strip()
    m = _price_re.search(s)
    return float(m.group(1).replace(",", ".")) if m else 0.0

def extract_main_price(driver, wait) -> str:
    # attendre le bouton “vendre” (élément stable de la page offer)
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

    # fallback ancré au bouton
    btn = driver.find_element(By.ID, "buttonAddToCart")
    block = btn.find_element(By.XPATH, "./ancestor::*[contains(@class,'searchresult-price-block')][1]")
    price_el = block.find_element(By.CSS_SELECTOR, ".text-xxl span.text-blackRedesign")
    return (price_el.text or "").strip()


# ===================== DRIVER =====================
def make_driver(worker_id: int):
    options = webdriver.ChromeOptions()

    # ultra perf + headless
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=fr-FR")

    # ne pas attendre le chargement complet
    options.page_load_strategy = "eager"

    # réduire overhead chrome
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--no-first-run")
    options.add_argument("--metrics-recording-only")

    # user-agent “réel”
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

    # profil persistant par worker (consent cookies une seule fois)
    options.add_argument(f"--user-data-dir=/tmp/momox_profile_{worker_id}")

    # bloquer images/css/fonts via prefs
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2,
    }
    options.add_experimental_option("prefs", prefs)

    # réduire signaux automation
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

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

    # bloquer ressources lourdes + trackers (CDP)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setBlockedURLs", {
            "urls": [
                "*.png","*.jpg","*.jpeg","*.gif","*.webp","*.svg",
                "*.woff","*.woff2","*.ttf","*.css",
                "*doubleclick*","*googlesyndication*","*google-analytics*","*googletagmanager*","*gtm*"
            ]
        })
    except Exception:
        pass

    # patch webdriver
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"}
        )
    except Exception:
        pass

    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
    return driver


# ===================== WORKER =====================
def run_batch(args):
    worker_id, isbns_chunk = args
    driver = make_driver(worker_id)
    wait = WebDriverWait(driver, WAIT_SECONDS)

    bought_local = []
    try:
        # warmup + cookies
        driver.get(BASE_HOME)
        accept_cookies_shadow(driver, COOKIE_TIMEOUT_HOME)

        for isbn in isbns_chunk:
            try:
                driver.get(BASE_OFFER.format(isbn))
            except Exception:
                continue

            accept_cookies_shadow(driver, COOKIE_TIMEOUT_OFFER)

            # anti-redirection
            if not ensure_offer_page(driver, isbn):
                continue

            # attendre soit bouton soit message non-racheté (sans page_source)
            try:
                wait.until(lambda d: d.find_elements(By.ID, "buttonAddToCart") or is_not_bought_message_present(d))
            except Exception:
                continue

            if is_not_bought_message_present(driver):
                continue

            try:
                price_str = extract_main_price(driver, wait)
                val = price_to_float(price_str)
                if val <= 0:
                    continue
                title = extract_title(driver)
                bought_local.append((isbn, title, val, price_str))
            except Exception:
                continue

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return bought_local


# ===================== MAIN =====================
def main():
    if not Path(ISBN_FILE).exists():
        tg_send_message("❌ isbns.txt introuvable sur le serveur.")
        return

    isbns = [l.strip() for l in Path(ISBN_FILE).read_text(encoding="utf-8").splitlines() if l.strip()]
    if not isbns:
        tg_send_message("❌ isbns.txt est vide.")
        return

    workers = WORKERS  # ✅ forcé à 5
    tg_send_message(f"🔄 Analyse en cours… ({len(isbns)} ISBN) | Workers: {workers}")

    # chunks contigus (meilleur équilibrage)
    chunk_size = (len(isbns) + workers - 1) // workers
    chunks = [isbns[i:i + chunk_size] for i in range(0, len(isbns), chunk_size)]
    args = [(idx, chunks[idx]) for idx in range(len(chunks))]

    all_bought = []
    with Pool(processes=workers) as pool:
        done = 0
        for result in pool.imap_unordered(run_batch, args):
            all_bought.extend(result)
            done += 1
            # petit progress (pas trop spam)
            tg_send_message(f"⏳ Progress: {done}/{len(args)} workers terminés…")

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
