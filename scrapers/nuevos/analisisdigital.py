# =====================================
# Scraper LOCAL - Análisis Digital
# =====================================
# - Enfocado en intendentes y ciudades locales
# - Filtrado por nombres y localidades clave
# - Backup incremental durante la corrida
# - Guardado final incremental + dedupe
# - Estandarización final de columnas
# =====================================

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import random
import logging
from datetime import datetime, timedelta
from hashlib import md5
import sys
import re

# ------------ CONFIGURACIÓN -----------
MEDIO             = "analisisdigital"
SECCIONES         = {
    'locales': 'https://www.analisisdigital.com.ar/locales'
}
OUT_FINAL         = f"main/data/raw/{MEDIO}.csv"
BACKUP_PATH       = "tmp_analisisdigital_locales.csv"
HEADERS           = {'User-Agent': 'Mozilla/5.0'}
ORDEN_CRONOLOGICO = True
MAX_PAGINAS       = 200
SLEEP_PAGE        = (0.8, 1.6)
SLEEP_ART         = (0.3, 0.8)
CHECKPOINT_EVERY  = 20

# ----------- INTENDENTES Y LOCALIDADES -----------
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

def menciona_relevante(texto):
    texto_limpio = texto.lower()
    return any(clave in texto_limpio for clave in CLAVES_RELEVANTES)

# ----------- FECHA DE CORTE -----------
if len(sys.argv) > 1:
    FECHA_CORTE_STR = sys.argv[1]
    FECHA_CORTE_DT = datetime.strptime(FECHA_CORTE_STR, "%Y-%m-%d")
else:
    FECHA_CORTE_DT = datetime.today() - timedelta(days=7)
    FECHA_CORTE_STR = FECHA_CORTE_DT.strftime("%Y-%m-%d")
print(f"Fecha de corte: {FECHA_CORTE_STR}")

# ----------- LOGGING -----------
os.makedirs("logs", exist_ok=True)
LOG_PATH = f"logs/{MEDIO}_{datetime.now().date()}.log"
logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

def log(msg, level="info"):
    getattr(logging, level)(msg)
    print(msg)

def make_hash(s: str) -> str:
    return md5(s.encode("utf-8")).hexdigest()

def robust_request(url, headers=None, timeout=15, max_retries=3, wait=2):
    for i in range(max_retries):
        try:
            return requests.get(url, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            log(f"Intento {i+1} fallido para {url}: {e}", "warning")
            time.sleep(wait)
    log(f"ERROR persistente para {url} tras {max_retries} reintentos.", "error")
    return None

def parse_fecha_analisis(fecha_str):
    meses = {
        'enero': '01', 'febrero': '02', 'marzo': '03',
        'abril': '04', 'mayo': '05', 'junio': '06',
        'julio': '07', 'agosto': '08', 'septiembre': '09',
        'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }
    if not fecha_str or not isinstance(fecha_str, str):
        return pd.NaT
    try:
        partes = fecha_str.lower().split(' de ')
        dia = int(partes[0].strip())
        mes = meses.get(partes[1].strip(), None)
        anio_hora = partes[2].split(' - ')
        anio = anio_hora[0].strip()
        hora = anio_hora[1].strip() if len(anio_hora) > 1 else "00:00"
        return datetime.strptime(f"{anio}-{mes}-{dia:02d} {hora}", "%Y-%m-%d %H:%M")
    except Exception:
        try:
            m = re.search(r'(\d{1,2}) de ([a-záéíóú]+) de (\d{4})', fecha_str.lower())
            if m:
                dia, mes_str, anio = int(m.group(1)), m.group(2), int(m.group(3))
                mes = meses.get(mes_str, None)
                if mes:
                    return datetime.strptime(f"{anio}-{mes}-{dia:02d} 00:00", "%Y-%m-%d %H:%M")
        except Exception as e:
            log(f"Error parseando fecha: {fecha_str} -> {e}", "warning")
        return pd.NaT

def scrapear_seccion(seccion, fecha_corte_dt, backup_path):
    log(f"Iniciando scraping: {MEDIO} - {seccion}")
    resultados = []
    omitidas_sin_fecha = 0
    if os.path.exists(backup_path):
        df_old = pd.read_csv(backup_path)
        resultados = df_old.to_dict('records')
        log(f"Cargadas {len(resultados)} noticias de backup")

    enlaces_vistos = {r['enlace'] for r in resultados if 'enlace' in r}

    for n_pag in range(1, MAX_PAGINAS + 1):
        url = SECCIONES[seccion] if n_pag == 1 else f"{SECCIONES[seccion]}?page={n_pag-1}"
        log(f"Página {n_pag}: {url}")
        t0 = time.time()

        try:
            res = robust_request(url, headers=HEADERS)
            if res is None:
                continue
            soup = BeautifulSoup(res.text, "html.parser")
            main_content = soup.find('div', class_='body')
            items = main_content.find_all('div', class_='views-row') if main_content else []

            for item in items:
                try:
                    a_tag = item.find('a', href=True)
                    if not a_tag:
                        continue
                    enlace = a_tag['href']
                    if f"/{seccion}/" not in enlace:
                        continue
                    if not enlace.startswith('http'):
                        enlace = "https://www.analisisdigital.com.ar" + enlace
                    if enlace in enlaces_vistos:
                        continue

                    h2_tag = item.find('h2')
                    h3_tag = item.find('h3')
                    titulo = h2_tag.get_text(strip=True) if h2_tag else (h3_tag.get_text(strip=True) if h3_tag else '')

                    res_nota = robust_request(enlace, headers=HEADERS)
                    if res_nota is None:
                        continue
                    soup_nota = BeautifulSoup(res_nota.text, "html.parser")
                    fecha_tag = soup_nota.find('div', class_=lambda x: x and 'field--name-node-post-date' in x)
                    fecha_raw = fecha_tag.get_text(strip=True) if fecha_tag else ''
                    fecha_parseada = parse_fecha_analisis(fecha_raw)

                    if not fecha_parseada or pd.isna(fecha_parseada):
                        body_txt = soup_nota.get_text(separator=" ", strip=True)
                        fecha_regex = re.search(r'(\d{1,2}) de ([a-záéíóú]+) de (\d{4})', body_txt, re.I)
                        if fecha_regex:
                            fecha_parseada = parse_fecha_analisis(fecha_regex.group(0))
                    if not fecha_parseada or pd.isna(fecha_parseada):
                        log(f"Nota omitida por no parsear fecha: {enlace}", "warning")
                        omitidas_sin_fecha += 1
                        continue

                    if ORDEN_CRONOLOGICO and fecha_parseada.date() < fecha_corte_dt.date():
                        log(f"Corte por fecha: {fecha_parseada} < {fecha_corte_dt}", "info")
                        return resultados

                    cuerpo_div = soup_nota.find('div', class_=lambda x: x and 'body-noticia' in x)
                    parrafos = [p.get_text(strip=True) for p in cuerpo_div.find_all('p')] if cuerpo_div else []
                    contenido = "\n".join(parrafos)

                    if not (menciona_relevante(titulo) or menciona_relevante(contenido)):
                        continue

                    row = {
                        'medio': MEDIO,
                        'fecha': fecha_parseada.strftime("%Y-%m-%d"),
                        'titulo': titulo,
                        'copete': pd.NA,
                        'contenido': contenido,
                        'enlace': enlace,
                        'seccion': seccion
                    }
                    resultados.append(row)
                    enlaces_vistos.add(enlace)

                    if len(resultados) % CHECKPOINT_EVERY == 0:
                        pd.DataFrame(resultados).to_csv(backup_path, index=False)
                        log(f"Backup parcial guardado: {len(resultados)} filas")

                    time.sleep(random.uniform(*SLEEP_ART))
                except Exception as e:
                    log(f"Error scrapeando item en página {n_pag}: {e}", "warning")

            pd.DataFrame(resultados).to_csv(backup_path, index=False)

            if not items:
                break

        except Exception as e:
            log(f"ERROR en página {n_pag}: {e}", "warning")

        t1 = time.time()
        log(f"Tiempo: {t1 - t0:.1f} segundos")
        time.sleep(random.uniform(*SLEEP_PAGE))

    log(f"Total de noticias omitidas por problemas de fecha: {omitidas_sin_fecha}", "warning")
    return resultados

# ----------- MAIN -----------
if __name__ == "__main__":
    os.makedirs(os.path.dirname(OUT_FINAL), exist_ok=True)
    all_notas = []
    for sec in SECCIONES:
        notas = scrapear_seccion(sec, FECHA_CORTE_DT, BACKUP_PATH)
        all_notas.extend(notas)

    df = pd.DataFrame(all_notas)
    if df.empty:
        log("No se obtuvieron resultados.", "warning")
        sys.exit()

    # --- 🧹 ESTANDARIZACIÓN FINAL ---
    df.columns = df.columns.str.lower().str.strip()
    for col in ['id', 'medio', 'fecha', 'titulo', 'copete', 'contenido', 'enlace', 'seccion']:
        if col not in df.columns:
            df[col] = pd.NA

    df['copete'] = df['copete'].fillna('')
    df['contenido'] = df['contenido'].fillna('') + '. ' + df['copete']
    df['contenido'] = df['contenido'].str.strip()

    df['id'] = df.apply(lambda row: md5((str(row['titulo']) + str(row['enlace'])).encode('utf-8')).hexdigest(), axis=1)
    df['fecha'] = pd.to_datetime(df['fecha']).dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates(subset=["id"]).reset_index(drop=True)

    if os.path.exists(OUT_FINAL):
        old = pd.read_csv(OUT_FINAL)
        antes = len(old)
        combined = pd.concat([old, df], ignore_index=True).drop_duplicates(subset=["id"])
        combined.to_csv(OUT_FINAL, index=False, encoding='utf-8')
        log(f"Incremental: {antes} -> {len(combined)} filas (+{len(combined)-antes})")
    else:
        df.to_csv(OUT_FINAL, index=False, encoding='utf-8')
        log(f"Archivo nuevo guardado: {len(df)} filas")
