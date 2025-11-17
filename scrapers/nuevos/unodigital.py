# =====================================
# Scraper UNO Digital (intendentes y ciudades)
# =====================================
# - Busca solo notas que mencionan personas o localidades clave
# - Paginación sin Selenium
# - Incremental + deduplicación
# =====================================

import os, re, time, logging, random
from datetime import datetime, timedelta
from hashlib import md5
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup
import sys

# -------- CONFIG GLOBAL --------
MEDIO = "unodigital"
SECCIONES = {
    "provinciales": "https://www.unoentrerios.com.ar/contenidos/provincia.html",
    "economia":     "https://www.unoentrerios.com.ar/economia",
}
OUT_PATH = "../data/raw/unodigital_municipales.csv"
TMP_DIR = "tmp"
BACKUP_LISTADO = f"{TMP_DIR}/tmp_listado_{MEDIO}.csv"
BACKUP_DETALLE = f"{TMP_DIR}/tmp_detalle_{MEDIO}.csv"

# -------- FILTRO RELEVANTE --------
CLAVES_RELEVANTES = [
    "rosario romero", "romero",
    "francisco azcué", "azcue", "azcué",
    "mauricio davico", "davico",
    "jose eduardo lauritto", "lauritto",
    "dora bogdan", "bogdan",
    "claudia monjo", "monjo",
    "paraná", "concordia", "gualeguaychú", "gualeguaychu",
    "concepción del uruguay", "concepcion del uruguay",
    "gualeguay", "villaguay"
]
def menciona_relevante(texto):
    texto = texto.lower()
    return any(k in texto for k in CLAVES_RELEVANTES)

# -------- FECHA CORTE --------
if len(sys.argv) > 1:
    FECHA_CORTE_STR = sys.argv[1]
    FECHA_CORTE_DT = datetime.strptime(FECHA_CORTE_STR, "%Y-%m-%d")
else:
    FECHA_CORTE_DT = datetime.today() - timedelta(days=7)
    FECHA_CORTE_STR = FECHA_CORTE_DT.strftime("%Y-%m-%d")

# -------- LOGGING --------
os.makedirs("logs", exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
LOG_PATH = f"logs/{MEDIO}_{datetime.now().date()}.log"
logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s - %(message)s")

# -------- SELECTORES HTML --------
SEL_CARD = "article.standard-entry-box, article.big-entry-box"
SEL_TITLE = ".entry-data h2.entry-title"
SEL_LINK = "a.cover-link"
SEL_FECHA_DET = ".fecha-container .nota-fecha"
SEL_COPETE = "p.ignore-parser"
SEL_CONTENIDO = ".article-body p"
EXCLUIR_SMALL = ".small-entry"

# -------- FUNCIONES AUX --------
HEADERS = {"User-Agent": "Mozilla/5.0"}
MESES_ES = {
    "enero":"01","febrero":"02","marzo":"03","abril":"04","mayo":"05","junio":"06",
    "julio":"07","agosto":"08","septiembre":"09","octubre":"10","noviembre":"11","diciembre":"12"
}
def parse_fecha_es(txt):
    if not txt:
        return None
    m = re.search(r'(\d{1,2})\s+de\s+([a-záéíóú]+)\s+(\d{4})', txt.lower())
    if not m:
        return None
    d, mes_str, y = int(m.group(1)), m.group(2), int(m.group(3))
    mes = MESES_ES.get(mes_str.strip(" ."))
    if not mes:
        return None
    return datetime(y, int(mes), d)

def make_hash(v):
    return md5(v.encode("utf-8")).hexdigest()

def get_soup(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def scrape_detalle(url):
    soup = get_soup(url)
    fecha_txt = soup.select_one(SEL_FECHA_DET)
    fecha = parse_fecha_es(fecha_txt.get_text(strip=True)) if fecha_txt else None

    h1 = soup.find("h1")
    titulo = h1.get_text(strip=True) if h1 else ""

    copete = " ".join(p.get_text(strip=True) for p in soup.select(SEL_COPETE))
    contenido = " ".join(p.get_text(strip=True) for p in soup.select(SEL_CONTENIDO))
    texto = f"{copete} {contenido}".strip()

    if not (menciona_relevante(titulo) or menciona_relevante(texto)):
        return None, fecha  # no relevante

    return {
        "titulo": titulo,
        "contenido": texto,
        "fecha": fecha.strftime("%Y-%m-%d") if fecha else None,
        "id": make_hash(url),
        "enlace": url,
        "medio": MEDIO
    }, fecha

def run():
    noticias = []
    for seccion, base_url in SECCIONES.items():
        for n in range(1, 400):
            url = f"{base_url.rstrip('/')}/{n}"
            try:
                soup = get_soup(url)
            except:
                break

            cards = [c for c in soup.select(SEL_CARD) if not c.select_one(EXCLUIR_SMALL)]
            if not cards:
                break

            for c in cards:
                href = c.select_one(SEL_LINK)
                if not href:
                    continue
                enlace = urljoin("https://www.unoentrerios.com.ar/", href.get("href"))
                try:
                    nota, fecha = scrape_detalle(enlace)
                    if nota and (not fecha or fecha >= FECHA_CORTE_DT):
                        nota["seccion"] = seccion
                        noticias.append(nota)
                except Exception as e:
                    continue

            time.sleep(random.uniform(0.7, 1.4))

    df = pd.DataFrame(noticias)
    if df.empty:
        print("No se encontraron notas relevantes.")
        return

    if os.path.exists(OUT_PATH):
        prev = pd.read_csv(OUT_PATH)
        df = pd.concat([prev, df], ignore_index=True).drop_duplicates(subset=["id"])
    df.to_csv(OUT_PATH, index=False)
    print(f"Total guardado: {len(df)} notas")

if __name__ == "__main__":
    run()
