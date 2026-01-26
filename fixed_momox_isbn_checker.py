import asyncio
import re
import socket
from dataclasses import dataclass, asdict
from typing import List, Optional

import aiohttp
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "https://www.momox.fr/offer/{}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
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


def parse_offer_page(html: str, isbn: str, url: str) -> OfferResult:
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.get_text(strip=True) if soup.title else None

    text_all = soup.get_text(" ", strip=True)
    price = extract_price_eur(text_all)

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


async def quick_connect_test(session: aiohttp.ClientSession) -> str:
    """Test simple pour voir si on atteint momox."""
    test_url = BASE_URL.format("9782070368228")
    try:
        async with session.get(test_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            return f"CONNECT_OK_HTTP_{resp.status}"
    except aiohttp.ClientConnectorError:
        return "CONNECT_ERROR_ClientConnectorError"
    except asyncio.TimeoutError:
        return "CONNECT_ERROR_TIMEOUT"
    except aiohttp.ClientError as e:
        return f"CONNECT_ERROR_{type(e).__name__}"


async def fetch_html_with_retries(session: aiohttp.ClientSession, url: str, retries: int = 3) -> (Optional[str], str):
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

    for variant in variants:
        for attempt in range(1, retries + 1):
            try:
                async with session.get(
                    variant,
                    headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=25),
                ) as resp:
                    last_status = f"HTTP_{resp.status}"
                    # on accepte les codes 200
                    if resp.status == 200:
                        return await resp.text(), "OK"
                    # Si 3xx ou 4xx, on considère qu'il y a un blocage temporaire;
                    # on attend un peu avant de réessayer ou de passer au variant suivant.
                    await asyncio.sleep(0.6 * attempt)
                    continue
            except asyncio.TimeoutError:
                last_status = "TIMEOUT"
            except aiohttp.ClientConnectorError:
                last_status = "CONNECT_ERROR"
            except aiohttp.ClientError:
                last_status = "CLIENT_ERROR"

            # backoff
            await asyncio.sleep(0.6 * attempt)
        # Si on atteint ici, tentative échouée pour ce variant; on passe au suivant

    return None, last_status


async def fetch_one(session: aiohttp.ClientSession, isbn: str, sem: asyncio.Semaphore) -> OfferResult:
    isbn = normalize_isbn(isbn)
    url = BASE_URL.format(isbn)

    async with sem:
        html, status = await fetch_html_with_retries(session, url, retries=3)

    if html is None:
        return OfferResult(isbn, url, False, None, None, status)

    return parse_offer_page(html, isbn, url)


async def run(isbns: List[str], concurrency: int = 4, proxy: Optional[str] = None) -> List[OfferResult]:
    sem = asyncio.Semaphore(concurrency)

    # Le fix principal: forcer IPv4 (évite beaucoup de ClientConnectorError sur certains réseaux)
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)

    # "trust_env=True" permet à aiohttp de prendre en compte les variables
    # d'environnement HTTP(S)_PROXY utilisées dans certains environnements (comme
    # celui de ce script). Sans cette option, la résolution DNS peut échouer et
    # entraîner des erreurs ClientConnectorError.
    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        # Diagnostic rapide
        diag = await quick_connect_test(session)
        print(f"[DIAG] {diag} (si CONNECT_ERROR: réseau/DNS/filtrage probable)")

        # Si tu as un proxy HTTP(S) à utiliser, tu peux le passer en argument (voir plus bas)
        if proxy:
            session._default_headers.update({"Proxy": proxy})  # non bloquant, informatif

        tasks = [fetch_one(session, isbn, sem) for isbn in isbns if normalize_isbn(isbn)]
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