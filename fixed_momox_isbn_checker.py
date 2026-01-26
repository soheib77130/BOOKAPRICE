import asyncio
import json
import os
import re
import socket
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple

import aiohttp
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "https://www.momox.fr/offer/{}"

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-CH-UA": '"Chromium";v="120", "Not-A.Brand";v="24", "Google Chrome";v="120"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Linux"',
    "DNT": "1",
}


@dataclass
class OfferResult:
    isbn: str
    url: str
    rachete: bool
    prix_eur: Optional[float]
    titre: Optional[str]
    statut: str  # OK / NON_RACHETE / HTTP_XXX / TIMEOUT / CONNECT_ERROR / PARSE_FAIL


def normalize_isbn(s: str) -> str:
    return re.sub(r"[^0-9Xx]", "", s).upper()


def extract_price_eur(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"(\d+[.,]\d{2})\s*€", text)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def extract_json_ld_price(soup: BeautifulSoup) -> Optional[float]:
    scripts = soup.find_all("script", {"type": "application/ld+json"})
    for script in scripts:
        raw = script.get_text(strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            offers = item.get("offers") if isinstance(item, dict) else None
            if isinstance(offers, dict):
                price = offers.get("price")
                try:
                    return float(str(price).replace(",", "."))
                except (TypeError, ValueError):
                    continue
    return None


def parse_offer_page(html: str, isbn: str, url: str) -> OfferResult:
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.get_text(strip=True) if soup.title else None

    text_all = soup.get_text(" ", strip=True)
    price = extract_json_ld_price(soup) or extract_price_eur(text_all)

    lowered = text_all.lower()
    negative_markers = [
        "nous ne rachetons pas",
        "nous ne pouvons pas racheter",
        "pas d'offre",
        "actuellement indisponible",
        "malheureusement",
    ]
    positive_markers = [
        "nous rachetons",
        "offre d'achat",
        "prix d'achat",
        "votre offre",
    ]

    is_negative = any(k in lowered for k in negative_markers)
    is_positive = any(k in lowered for k in positive_markers)

    if price is not None:
        return OfferResult(isbn, url, True, price, title, "OK")

    # pas de prix trouvé -> probable non racheté
    if is_negative and not is_positive:
        return OfferResult(isbn, url, False, None, title, "NON_RACHETE")

    # cas ambigu
    if is_positive and not is_negative:
        return OfferResult(isbn, url, True, None, title, "OK_SANS_PRIX")

    return OfferResult(isbn, url, False, None, title, "PARSE_FAIL")


async def fetch_html_with_playwright(url: str, proxy: Optional[str] = None) -> Tuple[Optional[str], str]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return None, "PLAYWRIGHT_NOT_AVAILABLE"

    async def attempt(with_proxy: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        launch_options = {"headless": True, "args": ["--no-sandbox"]}
        if with_proxy:
            launch_options["proxy"] = {"server": with_proxy}
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(**launch_options)
                context = await browser.new_context(
                    user_agent=BASE_HEADERS["User-Agent"],
                    locale="fr-FR",
                    extra_http_headers={
                        "Accept": BASE_HEADERS["Accept"],
                        "Accept-Language": BASE_HEADERS["Accept-Language"],
                    },
                )
                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000, referer="https://www.momox.fr/")
                content = await page.content()
                await browser.close()
                return content, None
        except Exception as exc:
            message = str(exc).splitlines()[0]
            return None, f"PLAYWRIGHT_{type(exc).__name__}:{message}"

    html, error = await attempt(proxy)
    if html is not None:
        return html, "OK"
    if proxy:
        html, fallback_error = await attempt(None)
        if html is not None:
            return html, "OK"
        if fallback_error:
            return None, fallback_error
    return None, error or "PLAYWRIGHT_UNKNOWN_ERROR"


async def quick_connect_test(session: aiohttp.ClientSession, proxy: Optional[str]) -> str:
    """Test simple pour voir si on atteint momox."""
    test_url = BASE_URL.format("9782070368228")
    try:
        async with session.get(
            test_url,
            headers=BASE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=15),
            proxy=proxy,
        ) as resp:
            return f"CONNECT_OK_HTTP_{resp.status}"
    except aiohttp.ClientHttpProxyError:
        return "CONNECT_ERROR_ClientHttpProxyError"
    except aiohttp.ClientProxyConnectionError:
        return "CONNECT_ERROR_ClientProxyConnectionError"
    except aiohttp.ClientConnectorError:
        return "CONNECT_ERROR_ClientConnectorError"
    except asyncio.TimeoutError:
        return "CONNECT_ERROR_TIMEOUT"
    except aiohttp.ClientError as e:
        return f"CONNECT_ERROR_{type(e).__name__}"


def resolve_proxy(proxy: Optional[str], use_env_proxy: bool) -> Optional[str]:
    if proxy:
        return proxy
    if not use_env_proxy:
        return None
    if os.environ.get("MOMOX_NO_PROXY", "").strip().lower() in {"1", "true", "yes"}:
        return None
    return (
        os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
    )


async def warmup_session(session: aiohttp.ClientSession, proxy: Optional[str]) -> None:
    """Prépare la session en visitant la page d'accueil pour récupérer les cookies."""
    try:
        async with session.get(
            "https://www.momox.fr/",
            headers=BASE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=15),
            proxy=proxy,
        ):
            return
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return


async def fetch_html_with_retries(
    session: aiohttp.ClientSession,
    url: str,
    retries: int = 3,
    proxy: Optional[str] = None,
    use_playwright_fallback: bool = True,
) -> Tuple[Optional[str], str]:
    """
    Télécharge le HTML avec plusieurs tentatives. Sur certaines infrastructures,
    la résolution DNS ou le filtrage peut causer des erreurs de connexion. Cette
    fonction renvoie le HTML et "OK" si la requête aboutit avec un HTTP 200.
    En cas d'échec, elle renvoie None et le dernier statut rencontré (ex.: "HTTP_403",
    "TIMEOUT", "CONNECT_ERROR"). Pour les statuts HTTP différents de 200, on
    réessaie avec un léger backoff. Une stratégie de secours sans sous-domaine
    (momox.fr au lieu de www.momox.fr) est tentée si tous les essais échouent.
    """
    last_status = "UNKNOWN"
    # Liste de variantes d'URL à essayer (www.momox.fr puis momox.fr)
    variants = [url]
    if "//www." in url:
        variants.append(url.replace("//www.", "//"))

    proxies = [proxy] if proxy else [None]
    if proxy:
        proxies.append(None)

    for current_proxy in proxies:
        for variant in variants:
            for attempt in range(1, retries + 1):
                try:
                    headers = dict(BASE_HEADERS)
                    headers["Referer"] = "https://www.momox.fr/"
                    async with session.get(
                        variant,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=25),
                        proxy=current_proxy,
                    ) as resp:
                        last_status = f"HTTP_{resp.status}"
                        if resp.status in {403, 429}:
                            await warmup_session(session, current_proxy)
                            await asyncio.sleep(0.6 * attempt)
                            continue
                        # on accepte les codes 200
                        if resp.status == 200:
                            return await resp.text(), "OK"
                        # Si 3xx ou 4xx, on considère qu'il y a un blocage temporaire;
                        # on attend un peu avant de réessayer ou de passer au variant suivant.
                        await asyncio.sleep(0.6 * attempt)
                        continue
                except asyncio.TimeoutError:
                    last_status = "TIMEOUT"
                except aiohttp.ClientHttpProxyError:
                    last_status = "PROXY_ERROR"
                except aiohttp.ClientProxyConnectionError:
                    last_status = "PROXY_CONNECT_ERROR"
                except aiohttp.ClientConnectorError:
                    last_status = "CONNECT_ERROR"
                except aiohttp.ClientError:
                    last_status = "CLIENT_ERROR"

                # backoff
                await asyncio.sleep(0.6 * attempt)
            # Si on atteint ici, tentative échouée pour ce variant; on passe au suivant

    if use_playwright_fallback:
        html, pw_status = await fetch_html_with_playwright(url, proxy=proxy)
        if html is not None:
            return html, pw_status
        if pw_status != "PLAYWRIGHT_NOT_AVAILABLE":
            return None, pw_status

    return None, last_status


async def fetch_one(
    session: aiohttp.ClientSession,
    isbn: str,
    sem: asyncio.Semaphore,
    proxy: Optional[str] = None,
    use_playwright_fallback: bool = True,
) -> OfferResult:
    isbn = normalize_isbn(isbn)
    url = BASE_URL.format(isbn)

    async with sem:
        html, status = await fetch_html_with_retries(
            session,
            url,
            retries=3,
            proxy=proxy,
            use_playwright_fallback=use_playwright_fallback,
        )

    if html is None:
        return OfferResult(isbn, url, False, None, None, status)

    return parse_offer_page(html, isbn, url)


async def run(
    isbns: List[str],
    concurrency: int = 4,
    proxy: Optional[str] = None,
    use_env_proxy: bool = False,
    use_playwright_fallback: bool = True,
) -> List[OfferResult]:
    sem = asyncio.Semaphore(concurrency)
    resolved_proxy = resolve_proxy(proxy, use_env_proxy)

    # Le fix principal: forcer IPv4 (évite beaucoup de ClientConnectorError sur certains réseaux)
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)

    async with aiohttp.ClientSession(
        connector=connector,
        trust_env=use_env_proxy,
        cookie_jar=aiohttp.CookieJar(unsafe=True),
    ) as session:
        await warmup_session(session, resolved_proxy)
        # Diagnostic rapide
        diag = await quick_connect_test(session, resolved_proxy)
        print(f"[DIAG] {diag} (si CONNECT_ERROR: réseau/DNS/filtrage probable)")

        tasks = [
            fetch_one(session, isbn, sem, proxy=resolved_proxy, use_playwright_fallback=use_playwright_fallback)
            for isbn in isbns
            if normalize_isbn(isbn)
        ]
        return await asyncio.gather(*tasks)


def save_reports(results: List[OfferResult], csv_path: str = "momox_report.csv", html_path: str = "momox_report.html"):
    df = pd.DataFrame([asdict(r) for r in results])

    # tri: prix décroissant, puis racheté
    df["prix_sort"] = df["prix_eur"].fillna(-1)
    df = df.sort_values(by=["prix_sort", "rachete"], ascending=[False, False]).drop(columns=["prix_sort"])

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    df_html = df.copy()
    df_html["url"] = df_html["url"].apply(lambda u: f'<a href="{u}" target="_blank">{u}</a>')
    html = df_html.to_html(index=False, escape=False)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(
            "<html><head><meta charset='utf-8'><title>Rapport Momox</title></head>"
            "<body><h1>Rapport Momox (trié du plus cher au moins cher)</h1>"
            + html +
            "</body></html>"
        )

    print(f"✅ CSV:  {csv_path}")
    print(f"✅ HTML: {html_path}")


if __name__ == "__main__":
    ISBN_LIST = [
        "9782070368228",
        "9782253006329",
        "9782749948571",
    ]

    results = asyncio.run(run(ISBN_LIST, concurrency=4))
    save_reports(results)
