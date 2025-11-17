# ===========================================
# Scraper FULL - APFDigital (Entre Ríos)
# ===========================================
# - Sección: Municipales
# - Selenium para listado, Requests para detalle
# - Filtra por intendentes y ciudades clave
# - Estandarización homogénea de salida
# ===========================================

import os, re, time, random, logging
from datetime import datetime, timedelta
from hashlib import md5
from urllib.parse import urljoin
import sys

import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    NoSuchElementException, StaleElementReferenceException, TimeoutException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# -------- CONFIG --------
MEDIO              = "apfdigital"
BASE_URL           = "https://www.apfdigital.com.ar"
SECCIONES_INICIO   = [f"{BASE_URL}/municipales"]
OUT_FINAL          = "main/data/raw/apfdigital.csv"
TMP_DIR            = "tmp"
LOG_PATH           = f"logs/{MEDIO}_{datetime.now().date()}.log"
HEADLESS           = True
MAX_NOTAS_TOTAL    = None
SAVE_EVERY         = 100

WAIT_SELECTOR_LIST = (
    "article.listado-noticias-relacionadas, article.listado-noticias, "
    "article.noticia-relacionada, article.listado-noticias-simple"
)

CLAVES_RELEVANTES = [
    "rosario romero", "romero",
    "francisco azcué", "azcué", "azcue",
    "mauricio davico", "davico",
    "jose eduardo lauritto", "jose lauritto", "lauritto",
    "dora bogdan", "bogdan",
    "claudia monjo", "monjo",
    "paraná", "concordia", "gualeguaychú", "gualeguaychu",
    "concepción del uruguay", "concepcion del uruguay",
    "gualeguay", "villaguay"
]

# Fecha de corte
if len(sys.argv) > 1:
    FECHA_CORTE_STR = sys.argv[1]
    FECHA_CORTE_DT = datetime.strptime(FECHA_CORTE_STR, "%Y-%m-%d")
else:
    FECHA_CORTE_DT = datetime.today() - timedelta(days=7)
    FECHA_CORTE_STR = FECHA_CORTE_DT.strftime("%Y-%m-%d")
print(f"Fecha de corte: {FECHA_CORTE_STR}")

# Logging
os.makedirs("logs", exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

def log(msg, level="info"):
    getattr(logging, level)(msg)
    print(msg)

# Funciones
def menciona_relevante(texto):
    texto_limpio = texto.lower()
    return any(clave in texto_limpio for clave in CLAVES_RELEVANTES)

def parse_fecha_apf(txt):
    meses = {
        "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
        "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12
    }
    m = re.search(r'(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})', txt.lower())
    if not m: return None
    d, mes_str, y = int(m.group(1)), m.group(2), int(m.group(3))
    mes = meses.get(mes_str.strip(" ."))
    if not mes: return None
    try: return datetime(y, mes, d)
    except: return None

def setup_driver(headless=True):
    opts = Options()
    if headless: opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def make_hash(v: str) -> str:
    return md5(v.encode("utf-8")).hexdigest()

session = requests.Session()
retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[500,502,503,504])
session.mount("https://", HTTPAdapter(max_retries=retries))
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_articles_on_page(driver):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    cards = soup.select(WAIT_SELECTOR_LIST)
    out = []
    for c in cards:
        a_tag = c.select_one("a[href]")
        h2_tag = c.select_one("h2.text-noticia-simple-titulo, h2")
        if not a_tag: continue
        href = urljoin(BASE_URL + "/", a_tag.get("href"))
        titulo = h2_tag.get_text(strip=True) if h2_tag else ""
        out.append({"url": href, "titulo": titulo})
    return out

def click_next_page(driver, prev_count, timeout=10):
    try:
        sel = "//a[contains(translate(.,'SIGUIENTE','siguiente'),'siguiente')]"
        nxt = driver.find_element(By.XPATH, sel)
        driver.execute_script("arguments[0].scrollIntoView();", nxt)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", nxt)
        WebDriverWait(driver, timeout).until(
            lambda d: len(BeautifulSoup(d.page_source, "html.parser").select(WAIT_SELECTOR_LIST)) > prev_count
        )
        return True
    except Exception:
        log("No hay botón de siguiente o no cargó correctamente.", "info")
        return False

def scrap_articulo_requests(url_abs, titulo_listado):
    r = session.get(url_abs, headers=HEADERS, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    titulo = soup.select_one("h1.titulo-nota")
    titulo = titulo.get_text(strip=True) if titulo else titulo_listado

    copete = soup.select_one("h2.bajada")
    copete = copete.get_text(strip=True) if copete else ""

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
        texto = " ".join(p.get_text(strip=True) for p in cuerpo_div.find_all(["p", "h3"]))
    else:
        texto = ""

    contenido_completo = (copete + " " + texto).strip()

    if not (menciona_relevante(titulo) or menciona_relevante(contenido_completo)):
        raise ValueError("nota no relevante")

    return {
        "medio": MEDIO,
        "fecha": fecha.strftime("%Y-%m-%d") if fecha else None,
        "titulo": titulo,
        "copete": copete,
        "contenido": contenido_completo,
        "enlace": url_abs,
        "seccion": "municipales"
    }, fecha

def run_full_apf(secciones_inicio, fecha_corte, max_notas_total=None):
    driver = setup_driver(headless=HEADLESS)
    resultados = []
    total_scraped = 0
    cortar = False

    try:
        for start_url in secciones_inicio:
            if cortar: break
            driver.get(start_url)
            time.sleep(2)

            while True:
                articles = get_articles_on_page(driver)
                prev_count = len(articles)
                if prev_count == 0: break

                for i, art in enumerate(articles, start=1):
                    if cortar: break
                    if max_notas_total and total_scraped >= max_notas_total:
                        cortar = True; break

                    url_abs = art["url"]
                    try:
                        row, fecha = scrap_articulo_requests(url_abs, art["titulo"])
                    except Exception as e:
                        log(f"Error nota {url_abs}: {e}", "warning")
                        continue

                    if fecha and fecha < fecha_corte:
                        log(f"Corte por fecha: {fecha.date()} < {fecha_corte.date()}")
                        cortar = True
                        break

                    resultados.append(row)
                    total_scraped += 1

                if cortar or not click_next_page(driver, prev_count): break

    finally:
        driver.quit()

    return pd.DataFrame(resultados)

# -------- MAIN --------
if __name__ == "__main__":
    df_new = run_full_apf(
        secciones_inicio=SECCIONES_INICIO,
        fecha_corte=FECHA_CORTE_DT,
        max_notas_total=MAX_NOTAS_TOTAL
    )

    if df_new.empty:
        log("No se obtuvieron resultados nuevos.", "warning")
        sys.exit()

    # --- 🧹 ESTANDARIZACIÓN FINAL ---
    df_new.columns = df_new.columns.str.lower().str.strip()
    for col in ['id', 'medio', 'fecha', 'titulo', 'copete', 'contenido', 'enlace', 'seccion']:
        if col not in df_new.columns:
            df_new[col] = pd.NA

    df_new['copete'] = df_new['copete'].fillna('')
    df_new['contenido'] = df_new['contenido'].fillna('') + '. ' + df_new['copete']
    df_new['contenido'] = df_new['contenido'].str.strip()
    df_new['fecha'] = pd.to_datetime(df_new['fecha'], errors='coerce').dt.strftime("%Y-%m-%d")

    df_new['id'] = df_new.apply(
        lambda row: md5((str(row['titulo']) + str(row['enlace'])).encode('utf-8')).hexdigest(), axis=1
    )
    df_new = df_new.drop_duplicates(subset=["id"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUT_FINAL), exist_ok=True)
    if os.path.exists(OUT_FINAL):
        old = pd.read_csv(OUT_FINAL)
        antes = len(old)
        combined = pd.concat([old, df_new], ignore_index=True).drop_duplicates(subset=["id"])
        combined.to_csv(OUT_FINAL, index=False, encoding='utf-8')
        log(f"Incremental: {antes} -> {len(combined)} filas (+{len(combined)-antes})")
    else:
        df_new = df_new.drop(columns=['keyword'], errors='ignore')
        df_new.to_csv(OUT_FINAL, index=False, encoding='utf-8')
        log(f"Archivo nuevo guardado: {len(df_new)} filas")

    print("Nuevas filas:", len(df_new))
