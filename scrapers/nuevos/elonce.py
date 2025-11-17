# ================================
# Scraper FULL Elonce (municipales)
# ================================
# - Búsqueda por palabras clave (intendentes y localidades)
# - Selenium para scroll y recolección de links
# - Requests para parseo de contenido
# - Corte por fecha, incremental CSV, dedupe por id
# ================================

import os, re, time, random, logging
from datetime import datetime, timedelta
from collections import OrderedDict
from hashlib import md5
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
import sys

# ---------- FECHA CORTE ----------
if len(sys.argv) > 1:
    FECHA_CORTE_STR = sys.argv[1]
    FECHA_CORTE_DT = datetime.strptime(FECHA_CORTE_STR, "%Y-%m-%d")
else:
    FECHA_CORTE_DT = datetime.today() - timedelta(days=7)
    FECHA_CORTE_STR = FECHA_CORTE_DT.strftime("%Y-%m-%d")
print(f"Fecha de corte: {FECHA_CORTE_STR}")

# ---------- CONFIG ----------
CANDIDATOS = [
    "rosario romero", "romero",
    "francisco azcué", "azcue", "azcué",
    "mauricio davico", "davico",
    "jose eduardo lauritto", "lauritto",
    "dora bogdan", "bogdan",
    "claudia monjo", "monjo",
    "paraná",
    "concordia",
    "gualeguaychú", "gualeguaychu",
    "concepción del uruguay", "concepcion del uruguay",
    "gualeguay",
    "villaguay"
]

OUT_PATH              = "../data/raw/elonce_municipales.csv"
LOG_PATH              = f"logs/elonce_{datetime.now().date()}.log"
HEADLESS              = True
MAX_NOTAS_POR_CAND    = None
FILTRAR_SECCIONES     = False
SECCIONES_OK          = {"política", "economía"}
BASE_URL              = "https://www.elonce.com"
HEADERS               = {"User-Agent": "Mozilla/5.0"}
TMP_DIR               = "tmp"

os.makedirs("logs", exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

# ---------- LOGGING ----------
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

# ---------- FUNCIONES AUXILIARES ----------
MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
}
def parse_fecha_es(fecha_str: str):
    if not fecha_str:
        return None
    f = fecha_str.lower().strip()
    m = re.search(r"(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})", f)
    if not m:
        return None
    d, mes_str, y = int(m.group(1)), m.group(2), int(m.group(3))
    mes = MESES_ES.get(mes_str.strip(" ."))
    if not mes:
        return None
    try:
        return datetime(y, mes, d)
    except Exception:
        return None

def make_hash(value: str) -> str:
    return md5(value.encode("utf-8")).hexdigest()

def setup_driver(headless=True):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(60)
    return driver

def parse_fecha_data_attr(fecha_raw: str):
    try:
        return datetime.strptime(fecha_raw, "%Y/%m/%d %H:%M:%S")
    except Exception:
        return None

def scroll_and_collect_links(driver, fecha_corte: datetime, max_links=None):
    total_links = OrderedDict()
    pagina = 1
    while True:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        articulos = soup.select("article.en-bandera--listado")
        if not articulos:
            logging.warning("Sin artículos en la página %s", pagina)

        fechas_visibles = []
        for art in articulos:
            header = art.select_one("header.en-bandera__header")
            if not header:
                continue
            enlace_tag = header.select_one("a.en-bandera__ancla-title")
            fecha_tag  = header.select_one("span.en-bandera__fecha")
            if not (enlace_tag and fecha_tag):
                continue

            href = enlace_tag.get("href")
            fecha_dt = parse_fecha_data_attr(fecha_tag.get("data-fecha"))
            if not fecha_dt:
                continue

            fechas_visibles.append(fecha_dt)
            if fecha_dt >= fecha_corte and href not in total_links:
                total_links[href] = fecha_dt
                if max_links and len(total_links) >= max_links:
                    break

        logging.info("Página %s - Links acumulados: %s", pagina, len(total_links))

        if fechas_visibles and min(fechas_visibles) < fecha_corte:
            logging.info("Fecha menor al corte detectada (%s). Deteniendo scroll.", min(fechas_visibles).date())
            break
        if max_links and len(total_links) >= max_links:
            break

        try:
            boton = driver.find_element(By.CLASS_NAME, "ver-mas")
            driver.execute_script("arguments[0].scrollIntoView();", boton)
            time.sleep(0.4)
            driver.execute_script("arguments[0].click();", boton)
            time.sleep(random.uniform(1.2, 2.2))
            pagina += 1
        except NoSuchElementException:
            logging.info("Fin del scroll (no hay botón 'ver más').")
            break
        except Exception as e:
            logging.info("Fin del scroll (error click ver más): %s", e)
            break

    return total_links

def scrap_articulo_requests(url_abs: str, filtrar_secciones=False):
    r = requests.get(url_abs, headers=HEADERS, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    seccion_tag = soup.select_one("div.cont-volanta a.etiqueta")
    seccion = seccion_tag.get_text(strip=True).lower() if seccion_tag else None
    if filtrar_secciones and seccion not in SECCIONES_OK:
        return None, None

    h1 = soup.select_one("h1.titulo-nota") or soup.select_one("h1")
    titulo = h1.get_text(strip=True) if h1 else "Sin título"
    bajada_tag = soup.select_one("h2.bajada")
    copete = bajada_tag.get_text(strip=True) if bajada_tag else ""

    fecha_dt = None
    fecha_tag = soup.select_one("span.fecha-nota")
    if fecha_tag:
        txt = fecha_tag.get_text(strip=True)
        fecha_dt = parse_fecha_es(txt)

    if not fecha_dt:
        tag_data = soup.select_one("[data-fecha]")
        if tag_data and tag_data.get("data-fecha"):
            try:
                fecha_dt = datetime.strptime(tag_data["data-fecha"], "%Y/%m/%d %H:%M:%S")
            except:
                pass

    if not fecha_dt:
        tag_txt = soup.select_one("div[class*='fecha'], span[class*='fecha'], time")
        txt = tag_txt.get_text(strip=True) if tag_txt else ""
        fecha_dt = parse_fecha_es(txt)

    cuerpo_div = (soup.select_one("div.texto")
                  or soup.select_one("div.noticia-contenido")
                  or soup.select_one("div.cuerpo-nota"))
    if cuerpo_div:
        texto = "\n".join(p.get_text(strip=True) for p in cuerpo_div.find_all(["p","h3"]) if p.get_text(strip=True))
    else:
        texto = ""

    row = {
        "id": make_hash(url_abs),
        "medio": "elonce",
        "fecha": fecha_dt.strftime("%Y-%m-%d") if fecha_dt else None,
        "seccion": seccion,
        "titulo": titulo,
        "url": url_abs,
        "contenido": (copete + " " + texto).strip()
    }
    return row, fecha_dt

def save_incremental(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        prev = pd.read_csv(path)
        before = len(prev)
        df_all = pd.concat([prev, df], ignore_index=True)
        df_all.drop_duplicates(subset=["id"], inplace=True)
        df_all.to_csv(path, index=False)
        logging.info("Guardado incremental: %s -> %s filas (+%s nuevas)",
                     before, len(df_all), len(df_all) - before)
    else:
        df.to_csv(path, index=False)
        logging.info("Archivo nuevo guardado: %s filas", len(df))

def run_full(candidatos, fecha_corte, out_path,
             headless=True, max_notas_por_cand=None, filtrar_secciones=False):
    resultados = []
    drv_scroll = setup_driver(headless=headless)

    try:
        for kw in candidatos:
            logging.info("===== Keyword: %s =====", kw)
            url_busqueda = f"{BASE_URL}/buscador/?q={kw}&enviar=Buscar&ord=desc"
            drv_scroll.get(url_busqueda)
            time.sleep(2)

            links = scroll_and_collect_links(drv_scroll, fecha_corte=fecha_corte, max_links=max_notas_por_cand)

            total = len(links)
            logging.info("Voy a scrapear %s notas", total)

            for i, (rel, fdt) in enumerate(links.items(), start=1):
                if max_notas_por_cand and i > max_notas_por_cand:
                    break
                url_abs = urljoin(BASE_URL + "/", rel)
                try:
                    row, fecha_dt = scrap_articulo_requests(url_abs, filtrar_secciones=filtrar_secciones)
                except Exception as e:
                    logging.warning("Error en nota %s (%s/%s): %s", url_abs, i, total, e)
                    continue

                if row:
                    row["keyword"] = kw
                    resultados.append(row)

                if i % 20 == 0 or i == total:
                    logging.info("Notas procesadas: %s/%s", i, total)

    finally:
        drv_scroll.quit()

    df = pd.DataFrame(resultados)
    if df.empty:
        logging.warning("No se obtuvieron resultados nuevos.")
        return df

    df.drop_duplicates(subset=["id"], inplace=True)
    save_incremental(df, out_path)
    return df

# ---------- RUN ----------
if __name__ == "__main__":
    fecha_corte = datetime.strptime(FECHA_CORTE_STR, "%Y-%m-%d")

    df_new = run_full(
        candidatos=CANDIDATOS,
        fecha_corte=fecha_corte,
        out_path=OUT_PATH,
        headless=HEADLESS,
        max_notas_por_cand=MAX_NOTAS_POR_CAND,
        filtrar_secciones=FILTRAR_SECCIONES
    )

    print("Nuevas filas:", len(df_new))
    try:
        display(df_new.head())
    except:
        print(df_new.head())
