# kolscan_leaderboard_monthly_full.py
# pip install playwright pandas
# playwright install

import re, time, pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://kolscan.io/leaderboard"
OUT = "kolscan_leaderboard_monthly.csv"

def human_wait(sec=0.6): time.sleep(sec)

def get_pairs(page):
    pairs = []
    for a in page.query_selector_all("a[href^='/account/']"):
        href = a.get_attribute("href") or ""
        m = re.search(r"/account/([^?/#]+)", href)
        if not m: 
            continue
        wallet = (m.group(1) or "").strip()
        kol = (a.inner_text() or "").strip()
        if kol and wallet and len(wallet) >= 32:
            pairs.append((kol, wallet))
    return pairs

def find_scroll_viewport(page):
    # essaie Radix ScrollArea puis quelques fallbacks courants
    selectors = [
        "[data-radix-scroll-area-viewport]",
        "[data-radix-scroll-area-viewport] > div",
        "div[style*='overflow: auto']",
        "div[style*='overflow: scroll']",
        "main div:has(a[href^='/account/'])",
    ]
    for sel in selectors:
        els = page.query_selector_all(sel)
        for el in els:
            # on veut un container qui contient les cartes de leaderboard
            if el.query_selector("a[href^='/account/']"):
                return el
    return None

def scroll_until_stable(page, max_idle_rounds=8, max_total_s=180):
    start = time.time()
    idle = 0
    last_count = -1
    viewport = find_scroll_viewport(page)

    while True:
        # 1) scroll fenêtre
        try:
            page.evaluate("window.scrollBy(0, 1400)")
        except Exception:
            pass

        # 2) scroll conteneur interne si dispo
        if viewport:
            try:
                page.evaluate("(el) => el.scrollBy(0, 1400)", viewport)
            except Exception:
                pass

        # 3) petit nudge molette (déclenche lazy-load)
        try:
            page.mouse.wheel(0, 1000)
        except Exception:
            pass

        human_wait(0.4)

        # 4) compter
        count = len(page.query_selector_all("a[href^='/account/']"))
        if count <= last_count:
            idle += 1
        else:
            idle = 0
            last_count = count

        # 5) conditions d’arrêt
        if idle >= max_idle_rounds:
            break
        if time.time() - start > max_total_s:
            break

def click_monthly(page):
    # essaie plusieurs sélecteurs (bouton, lien, texte)
    for sel in ["role=button[name='Monthly']", "text=Monthly", "a:has-text('Monthly')"]:
        loc = page.locator(sel).first
        if loc.count():
            try:
                loc.click()
                return True
            except PWTimeout:
                pass
            except Exception:
                pass
    return False

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # passe à False pour voir le scroll
        page = browser.new_page()
        page.set_default_timeout(20000)

        page.goto(URL, wait_until="domcontentloaded")
        human_wait(1.2)

        if not click_monthly(page):
            # fallback si l’onglet n’a pas réagi (parfois marche quand même)
            try:
                page.goto(URL + "?timeframe=30", wait_until="domcontentloaded")
                human_wait(1.2)
            except Exception:
                pass

        # scroll intensif jusqu’à stabilisation du nombre d’items
        scroll_until_stable(page, max_idle_rounds=10, max_total_s=240)
        human_wait(0.6)

        pairs = get_pairs(page)
        browser.close()

    df = pd.DataFrame(pairs, columns=["KOL", "WALLET"]).drop_duplicates()
    df.to_csv(OUT, index=False)
    print(f"✔ Exporté {len(df)} lignes -> {OUT}")

if __name__ == "__main__":
    main()
