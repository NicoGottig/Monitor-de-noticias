# -*- coding: utf-8 -*-
"""
Scrapper semanal incremental sin 'state/' (todo se infiere desde data/raw/)
Modos:
 - MODE='window'   → superposición de OVERLAP_DAYS respecto al max(fecha) histórico (RECOMENDADO)
 - MODE='sentinel' → corta al encontrar el primer enlace ya visto en la fecha máxima histórica

Salidas (solo de esta semana):
 data/tmp/week_YYYY-MM-DD/
   - analisisdigital_provinciales.tmp.csv
   - apfdigital_provinciales.tmp.csv
   - elargentino_provincia.tmp.csv
   - unificado_semana.tmp.csv
"""

import os, re, time, random, unicodedata, argparse, sys
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Set

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ----- Selenium opcional (solo para APF) -----
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_OK = True
except Exception:
    SELENIUM_OK = False

# =========================
# Config rápida (editá acá)
# =========================
MODE = "window"           # "window" (recomendado) | "sentinel"
OVERLAP_DAYS = 7          # solo aplica a MODE="window"
DEFAULT_DRY_PAGES = 0     # 0 = sin límite

# =========================
# Paths y constantes
# =========================
ROOT_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(ROOT_DIR, "data")
RAW_DIR    = os.path.join(DATA_DIR, "raw")
TMP_DIR    = os.path.join(DATA_DIR, "tmp")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

HOY = date.today()
WEEK_STAMP = HOY.isoformat()
WEEK_DIR = os.path.join(TMP_DIR, f"week_{WEEK_STAMP}")
os.makedirs(WEEK_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36")
}

MESES = {
    "enero":"01","febrero":"02","marzo":"03","abril":"04","mayo":"05","junio":"06",
    "julio":"07","agosto":"08","septiembre":"09","setiembre":"09","octubre":"10",
    "noviembre":"11","diciembre":"12"
}
SPANISH_DATE_RE = re.compile(r"(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})", re.I)

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def _norm(s: str) -> str:
    if s is None: return ""
    s = s.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("\u00a0"," ").replace("\u202f"," ").replace("\u200b","")
    return re.sub(r"\s+", " ", s)

def parse_spanish_date(text: str) -> Optional[str]:
    if not text: return None
    m = SPANISH_DATE_RE.search(_norm(text).lower())
    if not m: return None
    d, mes_txt, a = m.groups()
    mes = MESES.get(mes_txt)
    if not mes: return None
    try:
        return f"{int(a):04d}-{mes}-{int(d):02d}"
    except Exception:
        return None

def extract_date_generic(soup: BeautifulSoup, visibles_css: List[str]) -> Tuple[str, str, str]:
    """
    Devuelve (fecha_iso, fuente, fecha_texto)
    fecha_iso: 'YYYY-MM-DD' o ''
    fuente: 'meta' | 'time' | 'visible:<css>' | 'body' | 'none'
    fecha_texto: texto crudo informativo
    """
    meta = soup.find("meta", attrs={"property": "article:published_time"}) or \
           soup.find("meta", attrs={"name": "article:published_time"}) or \
           soup.find("meta", attrs={"property": "og:updated_time"})
    if meta and meta.get("content"):
        raw = meta["content"]
        m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        return (m.group(1) if m else (parse_spanish_date(raw) or "")), "meta", raw

    t = soup.find("time", datetime=True)
    if t and t.get("datetime"):
        raw = t["datetime"]
        m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        return (m.group(1) if m else (parse_spanish_date(raw) or "")), "time", raw

    for css in visibles_css:
        tag = soup.select_one(css)
        if tag:
            raw = tag.get_text(" ", strip=True)
            parsed = parse_spanish_date(raw) or ""
            if parsed:
                return parsed, f"visible:{css}", raw
            if " de " in _norm(raw).lower():
                return "", f"visible:{css}", raw

    body = soup.get_text(" ", strip=True)
    parsed = parse_spanish_date(body) or ""
    if parsed:
        return parsed, "body", body
    return "", "none", ""

# =========================
# Inferencia desde RAW
# =========================
def infer_last_from_raw(raw_path: str) -> Tuple[Optional[date], Set[str]]:
    """
    Lee el histórico del medio:
      - max_fecha_raw: máxima fecha (date) en columna 'fecha'
      - sentinel_links: enlaces presentes en esa max_fecha_raw (para MODE='sentinel')
    Si no hay archivo o no hay fechas, devuelve (None, set()).
    """
    if not os.path.exists(raw_path):
        return None, set()
    try:
        df = pd.read_csv(raw_path)
        if "fecha" not in df.columns: return None, set()
        s = pd.to_datetime(df["fecha"], errors="coerce").dt.date.dropna()
        if not len(s): return None, set()
        max_fecha = max(s)
        sentinels = set()
        if "enlace" in df.columns:
            sentinels = set(df.loc[pd.to_datetime(df["fecha"], errors="coerce").dt.date == max_fecha, "enlace"].dropna().astype(str))
        return max_fecha, sentinels
    except Exception:
        return None, set()

def in_window(fecha_iso: str, max_fecha_raw: Optional[date]) -> bool:
    """True si la fecha (o vacío) cae dentro de la ventana de superposición."""
    if max_fecha_raw is None:
        return True  # primera corrida: traé todo
    if not fecha_iso:
        return True  # sin fecha, lo resolvemos luego en validación
    try:
        d = datetime.strptime(fecha_iso, "%Y-%m-%d").date()
    except Exception:
        return True
    min_keep = max_fecha_raw - timedelta(days=OVERLAP_DAYS - 1)
    return d >= min_keep

def dedupe_by_enlace(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    if "enlace" in df.columns:
        return df.drop_duplicates(subset=["enlace"]).reset_index(drop=True)
    keys = [c for c in ["titulo","medio","fecha"] if c in df.columns]
    return df.drop_duplicates(subset=keys).reset_index(drop=True)

def save_tmp(df: pd.DataFrame, medio_tag: str, filename: str) -> str:
    path = os.path.join(WEEK_DIR, filename)
    if not df.empty:
        df = dedupe_by_enlace(df)
        df.to_csv(path, index=False, encoding="utf-8-sig")
    else:
        cols = ["medio","fecha","fecha_texto","fuente_fecha","titulo",
                "contenido","enlace","seccion","fecha_de_extraccion"]
        pd.DataFrame(columns=cols).to_csv(path, index=False, encoding="utf-8-sig")
    log(f"[{medio_tag}] Guardado TMP → {path} ({len(df)} filas)")
    return path

# =========================
# Scraper: AnalisisDigital
# =========================
def scrape_analisisdigital(max_fecha_raw: Optional[date], sentinel_links: Set[str], dry_pages: Optional[int]) -> pd.DataFrame:
    MEDIO = "analisisdigital"
    SECCION = "provinciales"
    URL_BASE = "https://www.analisisdigital.com.ar"
    SECCION_URL = f"{URL_BASE}/provinciales"
    MAX_PAGINAS = 200

    sess = requests.Session()
    registros = []
    collected_any = False

    log(f"[AD] mode={MODE} | max_fecha={max_fecha_raw or 'None'} | dry_pages={dry_pages or '-'}")
    page_iter = range(1, (dry_pages or MAX_PAGINAS)+1)

    for n_pag in page_iter:
        url = SECCION_URL if n_pag == 1 else f"{SECCION_URL}?page={n_pag-1}"
        r = sess.get(url, headers=HEADERS, timeout=25)
        if not r.ok:
            log(f"[AD] Página {n_pag} HTTP {r.status_code} → fin.")
            break
        soup = BeautifulSoup(r.text, "html.parser")
        main_content = soup.find("div", class_="body")
        items = main_content.find_all("div", class_="views-row") if main_content else []
        log(f"[AD] Página {n_pag}: {len(items)} items")
        if not items: break

        for i, item in enumerate(items, 1):
            a_tag = item.find("a", href=True)
            if not a_tag: continue
            enlace = a_tag["href"]
            if not enlace.startswith("http"):
                enlace = URL_BASE + enlace

            if MODE == "sentinel" and enlace in sentinel_links:
                log(f"[AD]   [{i}] Encontrado sentinela → corte inmediato")
                return pd.DataFrame(registros)  # no incluimos el ya visto

            titulo_tag = item.find(["h2","h3"])
            titulo = titulo_tag.get_text(strip=True) if titulo_tag else ""

            r2 = sess.get(enlace, headers=HEADERS, timeout=25)
            if not r2.ok:
                log(f"[AD]   [{i}] Detalle HTTP {r2.status_code} → skip")
                continue
            soup2 = BeautifulSoup(r2.text, "html.parser")

            fecha_iso, fuente, fecha_texto = extract_date_generic(
                soup2, ["div.field--name-node-post-date", "div.grupo-fecha-autor", "div.submitted"]
            )

            # Filtro según modo
            keep = in_window(fecha_iso, max_fecha_raw) if MODE == "window" else True
            if not keep:
                if collected_any:
                    log(f"[AD]   [{i}] {enlace} fuera de ventana → corte medio")
                    return pd.DataFrame(registros)
                else:
                    continue

            # Contenido
            cuerpo_div = soup2.find("div", class_=lambda x: x and "body-noticia" in x) \
                         or soup2.find("div", class_="note-body") or soup2
            parrafos = [p.get_text(" ", strip=True) for p in (cuerpo_div.find_all("p") if cuerpo_div else [])]
            contenido = "\n".join([p for p in parrafos if p]) if parrafos else ""

            registros.append({
                "medio": MEDIO, "fecha": fecha_iso, "fecha_texto": fecha_texto, "fuente_fecha": fuente,
                "titulo": titulo, "contenido": contenido, "enlace": enlace,
                "seccion": SECCION, "fecha_de_extraccion": HOY.isoformat()
            })
            collected_any = True
            log(f"[AD]   [{i}] OK | {fecha_iso or fecha_texto} ({fuente})")
            time.sleep(random.uniform(0.25, 0.55))

        time.sleep(random.uniform(0.7, 1.0))

    return pd.DataFrame(registros)

# =========================
# Scraper: APF (Selenium)
# =========================
def scrape_apf(max_fecha_raw: Optional[date], sentinel_links: Set[str], dry_pages: Optional[int]) -> pd.DataFrame:
    MEDIO = "apfdigital"
    SECCION = "provinciales"
    URL = "https://www.apfdigital.com.ar/provinciales"

    if not SELENIUM_OK:
        log("[APF] Selenium no disponible. Saltando medio.")
        return pd.DataFrame(columns=["medio","fecha","fecha_texto","fuente_fecha","titulo",
                                     "contenido","enlace","seccion","fecha_de_extraccion"])

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 8)

    registros = []
    collected_any = False
    pagina = 1
    vistos = set()

    log(f"[APF] mode={MODE} | max_fecha={max_fecha_raw or 'None'} | dry_pages={dry_pages or '-'}")
    try:
        driver.get(URL)
        time.sleep(2.0)

        while True:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            cards = soup.find_all("article", class_="listado-noticias-relacionadas")
            log(f"[APF] Página {pagina}: {len(cards)} items")
            if not cards: break

            for idx, art in enumerate(cards, 1):
                link_tag = art.find("a", class_="div-image")
                if not link_tag or not link_tag.get("href"): continue
                enlace = "https://www.apfdigital.com.ar" + link_tag["href"]
                if enlace in vistos: continue
                vistos.add(enlace)

                if MODE == "sentinel" and enlace in sentinel_links:
                    log(f"[APF]   [{idx}] Encontrado sentinela → corte inmediato")
                    return pd.DataFrame(registros)

                tit_tag = art.find("h2", class_="text-noticia-simple-titulo")
                titulo = tit_tag.get_text(strip=True) if tit_tag else ""

                # abrir detalle en la misma pestaña
                driver.execute_script("window.open(arguments[0], '_self');", enlace)
                time.sleep(1.1)
                s2 = BeautifulSoup(driver.page_source, "html.parser")

                fecha_iso, fuente, fecha_texto = extract_date_generic(
                    s2, ["div.noticia-fecha", "div.fecha"]
                )

                keep = in_window(fecha_iso, max_fecha_raw) if MODE == "window" else True
                if not keep:
                    if collected_any:
                        log(f"[APF]   [{idx}] fuera de ventana → corte medio")
                        return pd.DataFrame(registros)
                    else:
                        driver.execute_script("window.history.go(-1)")
                        time.sleep(0.8)
                        continue

                copete_tag = s2.find("div", class_="noticia-copete")
                copete = copete_tag.get_text(" ", strip=True) if copete_tag else ""
                cont_tag = s2.find("div", class_="noticia-contenido")
                parrafos = [p.get_text(" ", strip=True) for p in (cont_tag.find_all("p") if cont_tag else [])]
                if copete and parrafos and copete.strip() == parrafos[0].strip():
                    parrafos = parrafos[1:]
                contenido = (copete + ("\n" if copete else "") + "\n".join(parrafos)).strip()

                registros.append({
                    "medio": MEDIO, "fecha": fecha_iso, "fecha_texto": fecha_texto, "fuente_fecha": fuente,
                    "titulo": titulo, "contenido": contenido, "enlace": enlace,
                    "seccion": SECCION, "fecha_de_extraccion": HOY.isoformat()
                })
                collected_any = True
                log(f"[APF]   [{idx}] OK | {fecha_iso or fecha_texto} ({fuente})")

                driver.execute_script("window.history.go(-1)")
                time.sleep(0.9)

            # Ver más noticias / paginación
            if dry_pages and pagina >= dry_pages:
                break
            try:
                boton = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//span[contains(.,'Ver más noticias')]|//button[contains(.,'Ver más noticias')]")
                ))
                driver.execute_script("arguments[0].click();", boton)
                pagina += 1
                time.sleep(1.4)
            except Exception as e:
                log(f"[APF] Fin de listado o botón no disponible: {e}")
                break

    finally:
        try: driver.quit()
        except Exception: pass

    return pd.DataFrame(registros)

# =========================
# Scraper: El Argentino
# =========================
def scrape_elargentino(max_fecha_raw: Optional[date], sentinel_links: Set[str], dry_pages: Optional[int]) -> pd.DataFrame:
    MEDIO = "elargentino"
    SECCION = "provincia"
    BASE_URL = "https://diarioelargentino.com"
    sess = requests.Session()

    def recolectar_links(soup: BeautifulSoup):
        noticias = []
        principal = soup.find("div", class_="grid-9 destacada-4 destacadas-listados")
        if principal:
            for art in principal.find_all("article"):
                a = art.find("a", class_="article__ancla-title")
                if a and a.get("href"): noticias.append((a.get_text(strip=True), a["href"]))
        listado = soup.find("div", class_="listado-article")
        if listado:
            for art in listado.find_all("article"):
                a = art.find("a", class_="en-bandera__ancla-title")
                if a and a.get("href"): noticias.append((a.get_text(strip=True), a["href"]))
        return noticias

    registros = []
    collected_any = False
    pagina = 0
    max_pages = dry_pages if dry_pages else 9999

    log(f"[ELARG] mode={MODE} | max_fecha={max_fecha_raw or 'None'} | dry_pages={dry_pages or '-'}")

    while pagina < max_pages:
        url_list = f"{BASE_URL}/{SECCION}" + (f"/{pagina}" if pagina > 0 else "")
        r = sess.get(url_list, headers=HEADERS, timeout=25)
        if not r.ok:
            log(f"[ELARG] HTTP {r.status_code} en listado → fin.")
            break
        s = BeautifulSoup(r.text, "html.parser")
        links = recolectar_links(s)
        log(f"[ELARG] Página {pagina}: {len(links)} items")
        if not links: break

        for idx, (titulo, href) in enumerate(links, 1):
            enlace = BASE_URL + href

            if MODE == "sentinel" and enlace in sentinel_links:
                log(f"[ELARG]   [{idx}] Encontrado sentinela → corte inmediato")
                return pd.DataFrame(registros)

            r2 = sess.get(enlace, headers=HEADERS, timeout=25)
            if not r2.ok:
                log(f"[ELARG]   [{idx}] Detalle HTTP {r2.status_code} → skip")
                continue
            s2 = BeautifulSoup(r2.text, "html.parser")

            fecha_iso, fuente, fecha_texto = extract_date_generic(
                s2, ["span.fecha-nota", "div.cont-cuerpo .timeline-date-time-up"]
            )

            keep = in_window(fecha_iso, max_fecha_raw) if MODE == "window" else True
            if not keep:
                if collected_any:
                    log(f"[ELARG]   [{idx}] fuera de ventana → corte medio")
                    return pd.DataFrame(registros)
                else:
                    continue

            copete_tag = s2.find("div", class_="bajada")
            copete = copete_tag.get_text(" ", strip=True) if copete_tag else ""
            cuerpo_tag = s2.find("div", class_="texto")
            parrafos = [p.get_text(" ", strip=True) for p in (cuerpo_tag.find_all("p") if cuerpo_tag else [])]
            contenido = (copete + ("\n" if copete else "") + "\n".join([p for p in parrafos if p])).strip()

            registros.append({
                "medio": MEDIO, "fecha": fecha_iso, "fecha_texto": fecha_texto, "fuente_fecha": fuente,
                "titulo": titulo, "contenido": contenido, "enlace": enlace,
                "seccion": SECCION, "fecha_de_extraccion": HOY.isoformat()
            })
            collected_any = True
            log(f"[ELARG]   [{idx}] OK | {fecha_iso or fecha_texto} ({fuente})")
            time.sleep(random.uniform(0.35, 0.6))

        pagina += 1
        time.sleep(random.uniform(0.8, 1.1))

    return pd.DataFrame(registros)

# =========================
# Runner
# =========================
def main():
    parser = argparse.ArgumentParser(description="Scrapper semanal incremental (sin state/, con ventana o sentinela).")
    parser.add_argument("--medio", choices=["analisisdigital","apfdigital","elargentino","all"],
                        default="all", help="Qué medio scrapear")
    parser.add_argument("--dry", type=int, default=DEFAULT_DRY_PAGES,
                        help="Limitar páginas por medio para prueba (0 = sin límite)")
    args = parser.parse_args()

    medios = [args.medio] if args.medio != "all" else ["analisisdigital","apfdigital","elargentino"]
    tmp_paths = []

    # Inferir desde RAW (por medio)
    ad_max, ad_sentinels = infer_last_from_raw(os.path.join(RAW_DIR, "analisisdigital_provinciales.csv"))
    apf_max, apf_sentinels = infer_last_from_raw(os.path.join(RAW_DIR, "apfdigital_provinciales.csv"))
    el_max, el_sentinels = infer_last_from_raw(os.path.join(RAW_DIR, "elargentino_provincia.csv"))

    if "analisisdigital" in medios:
        df_ad = scrape_analisisdigital(ad_max, ad_sentinels, (args.dry or None))
        p = save_tmp(df_ad, "AD", "analisisdigital_provinciales.tmp.csv")
        tmp_paths.append(p)

    if "apfdigital" in medios:
        df_apf = scrape_apf(apf_max, apf_sentinels, (args.dry or None))
        p = save_tmp(df_apf, "APF", "apfdigital_provinciales.tmp.csv")
        tmp_paths.append(p)

    if "elargentino" in medios:
        df_el = scrape_elargentino(el_max, el_sentinels, (args.dry or None))
        p = save_tmp(df_el, "ELARG", "elargentino_provincia.tmp.csv")
        tmp_paths.append(p)

    # Unificado semanal TMP
    dfs = []
    for p in tmp_paths:
        try:
            dfs.append(pd.read_csv(p))
        except Exception:
            pass
    if dfs:
        df_week = dedupe_by_enlace(pd.concat(dfs, ignore_index=True))
    else:
        cols = ["medio","fecha","fecha_texto","fuente_fecha","titulo",
                "contenido","enlace","seccion","fecha_de_extraccion"]
        df_week = pd.DataFrame(columns=cols)

    week_unified_path = os.path.join(WEEK_DIR, "unificado_semana.tmp.csv")
    df_week.to_csv(week_unified_path, index=False, encoding="utf-8-sig")
    log(f"[UNIFICADO] Guardado → {week_unified_path} ({len(df_week)} filas)")
    log("Listo. Ahora corré process_week.py para validar y consolidar el histórico y generar las tablas del Shiny.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrumpido por usuario.")
        sys.exit(1)
