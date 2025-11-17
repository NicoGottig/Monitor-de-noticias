# ===========================================
# Scraper FULL - APFDigital (Entre Ríos)
# ===========================================
# - Sección: Municipales
# - Filtra por intendentes y ciudades clave
# - Selenium para listado, Requests para detalle
# - Corte por fecha y guardado incremental
# ===========================================

import os, re, time, random, logging
from datetime import datetime, timedelta
from hashlib import md5
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException, StaleElementReferenceException, TimeoutException
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys

# ------------------ CONFIG ------------------

SECCIONES_INICIO = [
    "https://www.apfdigital.com.ar/municipales"
]

if len(sys.argv) > 1:
    FECHA_CORTE_STR = sys.argv[1]
    FECHA_CORTE_DT = datetime.strptime(FECHA_CORTE_STR, "%Y-%m-%d")
else:
    FECHA_CORTE_DT = datetime.today() - timedelta(days=7)
    FECHA_CORTE_STR = FECHA_CORTE_DT.strftime("%Y-%m-%d")
print(f"Fecha de corte: {FECHA_CORTE_STR}")

OUT_PATH          = "../data/raw/apfdigital_municipales.csv"
LOG_PATH          = f"logs/apfdigital_{datetime.now().date()}.log"
HEADLESS          = True
MAX_NOTAS_TOTAL   = None
SAVE_EVERY        = 100
TMP_DIR           = "tmp"
BASE_URL          = "https://www.apfdigital.com.ar"

WAIT_SELECTOR_LIST = (
    "article.listado-noticias-relacionadas, "
    "article.listado-noticias, "
    "article.noticia-relacionada, "
    "article.listado-noticias-simple"
)

CLAVES_RELEVANTES = [
    "rosario romero", "romero",
    "francisco azcué", "azcué", "azcue",
    "mauricio davico", "davico",
    "jose eduardo lauritto", "jose lauritto", "lauritto",
    "dora bogdan", "bogdan",
    "claudia monjo", "monjo",
    "paraná",
    "concordia",
    "gualeguaychú", "gualeguaychu",
    "concepción del uruguay", "concepcion del uruguay",
    "gualeguay",
    "villaguay"
]

def menciona_relevante(texto):
    texto_limpio = texto.lower()
    return any(clave in texto_limpio for clave in CLAVES_RELEVANTES)

os.makedirs("logs", exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

def log(msg, level="info"):
    getattr(logging, level)(msg)
    print(msg)

session = requests.Session()
retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[500,502,503,504])
session.mount("https://", HTTPAdapter(max_retries=retries))
HEADERS = {"User-Agent": "Mozilla/5.0"}

MESES_ES = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12
}
def parse_fecha_apf(txt):
    if not txt: return None
    m = re.search(r'(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})', txt.lower())
    if not m: return None
    d = int(m.group(1)); mes = MESES_ES.get(m.group(2).strip(" .")); y = int(m.group(3))
    if not mes: return None
    try: return datetime(y, mes, d)
    except: return None

def setup_driver(headless=True):
    opts = Options()
    if headless: opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox")
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(60)
    return drv

def make_hash(v: str) -> str:
    return md5(v.encode("utf-8")).hexdigest()

def get_articles_on_page(driver):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    cards = soup.select(WAIT_SELECTOR_LIST)
    out = []
    for c in cards:
        a_tag = c.select_one("a[href]")
        h2_tag = c.select_one("h2.text-noticia-simple-titulo, h2")
        if not a_tag: continue
        href   = urljoin(BASE_URL + "/", a_tag.get("href"))
        titulo = h2_tag.get_text(strip=True) if h2_tag else ""
        out.append({"url": href, "titulo": titulo})
    return out

def click_next_page(driver, prev_count, timeout=10):
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "span.button[data-role='categorias']"))
        )
    except TimeoutException:
        for sel in ["//a[contains(translate(.,'SIGUIENTE','siguiente'),'siguiente')]",
                    "a[rel='next']","li.next a"]:
            try:
                nxt = driver.find_element(By.XPATH, sel) if sel.startswith("//") else driver.find_element(By.CSS_SELECTOR, sel)
                driver.execute_script("arguments[0].scrollIntoView();", nxt)
                time.sleep(0.2)
                driver.execute_script("arguments[0].click();", nxt)
                WebDriverWait(driver, timeout).until(
                    lambda d: len(BeautifulSoup(d.page_source,"html.parser").select(WAIT_SELECTOR_LIST)) > prev_count
                )
                return True
            except Exception:
                continue
        log("No hay botón de siguiente.", "info")
        return False

    try:
        qpage_before = btn.get_attribute("data-qpage")
        driver.execute_script("arguments[0].scrollIntoView();", btn)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", btn)
    except StaleElementReferenceException:
        return False
    except Exception as e:
        log(f"No se pudo clickear span.button: {e}", "warning")
        return False

    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(BeautifulSoup(d.page_source,"html.parser").select(WAIT_SELECTOR_LIST)) > prev_count
        )
        return True
    except TimeoutException:
        try:
            btn_after = driver.find_element(By.CSS_SELECTOR, "span.button[data-role='categorias']")
            if btn_after.get_attribute("data-qpage") != qpage_before:
                return True
        except Exception:
            pass
        log("No cargaron nuevas noticias tras el click.", "info")
        return False

def scrap_articulo_requests(url_abs, titulo_listado, default_section=None):
    r = session.get(url_abs, headers=HEADERS, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    h1 = soup.select_one("h1.titulo-nota") or soup.select_one("h1")
    titulo = h1.get_text(strip=True) if h1 else titulo_listado

    h2_bajada = soup.select_one("h2.bajada")
    copete = h2_bajada.get_text(strip=True) if h2_bajada else ""

    fecha = None
    tag_data = soup.select_one("[data-fecha]")
    if tag_data and tag_data.get("data-fecha"):
        try:
            fecha = datetime.strptime(tag_data["data-fecha"], "%Y/%m/%d %H:%M:%S")
        except Exception:
            pass
    if fecha is None:
        tag_txt = soup.select_one("div[class*='fecha'], span[class*='fecha'], time")
        fecha_txt = tag_txt.get_text(strip=True) if tag_txt else ""
        fecha = parse_fecha_apf(fecha_txt)

    cuerpo_div = soup.select_one("div.noticia-contenido, div.cuerpo-nota, div.texto, div#cuerpo-nota")
    if cuerpo_div:
        texto = " ".join(p.get_text(strip=True) for p in cuerpo_div.find_all(["p","h3"]) if p.get_text(strip=True))
    else:
        texto = ""

    contenido_completo = (copete + " " + texto).strip()

    if not (menciona_relevante(titulo) or menciona_relevante(contenido_completo)):
        raise ValueError("nota no relevante")

    return {
        "id": make_hash(url_abs),
        "medio": "apfdigital",
        "fecha": fecha.strftime("%Y-%m-%d") if fecha else None,
        "seccion": "municipales",
        "titulo": titulo,
        "url": url_abs,
        "contenido": contenido_completo
    }, fecha

def save_incremental(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        prev = pd.read_csv(path)
        before = len(prev)
        all_df = pd.concat([prev, df], ignore_index=True)
        all_df.drop_duplicates(subset=["id"], inplace=True)
        all_df.to_csv(path, index=False)
        log(f"Incremental: {before} -> {len(all_df)} filas (+{len(all_df)-before})")
    else:
        df.to_csv(path, index=False)
        log(f"Archivo nuevo guardado: {len(df)} filas")

def run_full_apf(secciones_inicio, fecha_corte, out_path,
                 headless=True, max_notas_total=None, save_every=SAVE_EVERY):

    drv = setup_driver(headless=headless)
    resultados = []
    total_scraped = 0
    cortar = False

    try:
        for start_url in secciones_inicio:
            if cortar: break
            log(f"=== Sección inicial: {start_url} ===")

            drv.get(start_url)
            time.sleep(2)

            while True:
                articles = get_articles_on_page(drv)
                prev_count = len(articles)
                if prev_count == 0:
                    log("Sin artículos. Fin sección.")
                    break

                log(f"Voy a scrapear {prev_count} notas (acumulado={total_scraped})")

                for i, art in enumerate(articles, start=1):
                    if cortar: break
                    if max_notas_total and total_scraped >= max_notas_total:
                        log(f"Max notas total alcanzado: {max_notas_total}")
                        cortar = True
                        break

                    url_abs = art["url"]
                    try:
                        row, fecha = scrap_articulo_requests(url_abs, art["titulo"])
                    except Exception as e:
                        log(f"Error nota {url_abs} ({i}/{prev_count}): {e}", "warning")
                        continue

                    if fecha and fecha < fecha_corte:
                        log(f"Corte por fecha: {fecha.date()} < {fecha_corte.date()} (sección terminada)")
                        cortar = True
                        break

                    resultados.append(row)
                    total_scraped += 1

                    if i % 20 == 0 or i == prev_count:
                        log(f"Notas procesadas (sección actual): {i}/{prev_count}")

                    if save_every and (len(resultados) % save_every == 0):
                        tmp_df = pd.DataFrame(resultados).drop_duplicates(subset=["id"])
                        tmp_path = os.path.join(TMP_DIR, f"apf_partial_{total_scraped}.csv")
                        tmp_df.to_csv(tmp_path, index=False)
                        log(f"Checkpoint guardado ({len(tmp_df)} filas) -> {tmp_path}")

                if cortar:
                    break

                if not click_next_page(drv, prev_count=prev_count):
                    break

    finally:
        drv.quit()

    df = pd.DataFrame(resultados)
    if df.empty:
        log("No se obtuvieron resultados nuevos.", "warning")
        return df

    df.drop_duplicates(subset=["id"], inplace=True)
    save_incremental(df, out_path)
    return df

if __name__ == "__main__":
    fecha_corte = datetime.strptime(FECHA_CORTE_STR, "%Y-%m-%d")
    df_new = run_full_apf(
        secciones_inicio=SECCIONES_INICIO,
        fecha_corte=fecha_corte,
        out_path=OUT_PATH,
        headless=HEADLESS,
        max_notas_total=MAX_NOTAS_TOTAL
    )
    print("Nuevas filas:", len(df_new))
