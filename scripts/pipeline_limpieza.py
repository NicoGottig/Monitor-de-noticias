import os
import pandas as pd
import numpy as np
import re
import unicodedata
from collections import Counter
from pysentimiento import create_analyzer
import spacy
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# --- RUTAS ---
RAW_PATH = os.path.join("..", "data", "raw")
OUTPUT_PATH = os.path.join("..", "app", "data")
os.makedirs(OUTPUT_PATH, exist_ok=True)

CODIFICACIONES = ['utf-8', 'latin1', 'windows-1252']
ARCHIVOS = [f for f in os.listdir(RAW_PATH) if f.endswith('.csv') and not f.startswith('~')]

# --- FUNCIONES ---
def cargar_csv_multi_encoding(path, codificaciones):
    for cod in codificaciones:
        try:
            return pd.read_csv(path, encoding=cod)
        except UnicodeDecodeError:
            continue
    return None

def parse_fecha_flexible(fecha):
    if pd.isnull(fecha): return np.nan
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return pd.to_datetime(fecha, format=fmt)
        except: continue
    return pd.to_datetime(fecha, errors='coerce')

def limpiar_y_estandarizar_df(df, medio):
    if df is None: return None
    if 'titulo_y' in df.columns:
        df.drop(columns=['titulo_y'], inplace=True)
    df.rename(columns={'url': 'enlace', 'titulo_x': 'titulo'}, inplace=True)
    if 'contenido' not in df.columns:
        df['contenido'] = ''
    if 'copete' in df.columns:
        df['contenido'] += '. ' + df['copete'].fillna('')
    if 'descripcion' in df.columns:
        df['contenido'] += '. ' + df['descripcion'].fillna('')
    for col in ['id', 'medio', 'seccion', 'fecha', 'titulo', 'contenido', 'enlace']:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[['id', 'medio', 'seccion', 'fecha', 'titulo', 'contenido', 'enlace']]
    df['medio'] = medio
    df['fecha'] = df['fecha'].apply(parse_fecha_flexible)
    return df

def etiquetar_parrafos(texto, regex):
    if pd.isna(texto): return []
    parrafos = [p.strip() for p in re.split(r'\.\s+', str(texto)) if p.strip()]
    return [p for p in parrafos if re.search(regex, p.lower())]

def analizar_sentimiento(parrafos, figura, analyzer):
    resultados = []
    for p in parrafos:
        if len(p.strip()) < 15: continue
        resultado = analyzer.predict(p)
        resultados.append({
            'figura': figura,
            'parrafo': p,
            'sentimiento': resultado.output,
            'probabilidad': round(resultado.probas[resultado.output], 3)
        })
    return resultados

def normalizar_palabra(palabra):
    return ''.join(c for c in unicodedata.normalize('NFD', palabra) if unicodedata.category(c) != 'Mn').lower()

def procesar_texto_y_bigrams(texto, nlp, stopwords, excluir):
    doc = nlp(texto)
    tokens = [normalizar_palabra(t.lemma_) for t in doc if t.is_alpha]
    bigramas = [f"{a} {b}" for a, b in zip(tokens, tokens[1:])]
    palabras_limpias = [w for w in tokens if w not in stopwords and w not in excluir and 3 <= len(w) <= 20]
    bigramas_limpios = [b for b in bigramas if all(w not in stopwords and w not in excluir for w in b.split())]
    return palabras_limpias + bigramas_limpios

def contar_frecuencias(df):
    registros = []
    for sent in df['sentimiento'].unique():
        subset = df[df['sentimiento'] == sent]
        tokens = []
        for texto in subset['parrafo']:
            tokens.extend(procesar_texto_y_bigrams(texto, nlp, stopwords, excluir))
        for palabra, freq in Counter(tokens).items():
            registros.append({'palabra': palabra, 'sentimiento': sent, 'frecuencia': freq})
    return pd.DataFrame(registros)

def construir_grafo_semantico(frecuencias_df):
    model = SentenceTransformer('distiluse-base-multilingual-cased-v2')
    grafo_rows = []
    for sent in frecuencias_df['sentimiento'].unique():
        top = frecuencias_df[frecuencias_df['sentimiento'] == sent].nlargest(100, 'frecuencia')
        palabras = top['palabra'].tolist()
        embeddings = model.encode(palabras)
        sims = cosine_similarity(embeddings)
        for i in range(len(palabras)):
            for j in range(i+1, len(palabras)):
                sim = sims[i, j]
                if sim > 0.5:
                    grafo_rows.append({
                        'palabra_1': palabras[i],
                        'palabra_2': palabras[j],
                        'peso': round(sim, 3),
                        'sentimiento': sent
                    })
    return pd.DataFrame(grafo_rows)

# --- EJECUCIÓN PRINCIPAL ---
dfs = []
for archivo in ARCHIVOS:
    medio = archivo.replace('.csv', '')
    path = os.path.join(RAW_PATH, archivo)
    df = cargar_csv_multi_encoding(path, CODIFICACIONES)
    df = limpiar_y_estandarizar_df(df, medio)
    if df is not None:
        dfs.append(df)

noticias = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=['enlace'])

# Regex
FRIGERIO_REGEX = r'\b(rogelio\s+frigerio|frigerio|gobernador\s+frigerio)\b'
ROMERO_REGEX = r'\b(rosario\s+romero|intendenta\s+romero)\b'

noticias['parrafos_frigerio'] = noticias['contenido'].apply(lambda t: etiquetar_parrafos(t, FRIGERIO_REGEX))
noticias['parrafos_romero'] = noticias['contenido'].apply(lambda t: etiquetar_parrafos(t, ROMERO_REGEX))

parrafos_f = sum(noticias['parrafos_frigerio'].tolist(), [])
parrafos_r = sum(noticias['parrafos_romero'].tolist(), [])
parrafos_f = [p for p in parrafos_f if len(p) > 15]
parrafos_r = [p for p in parrafos_r if len(p) > 15]

analyzer = create_analyzer(task="sentiment", lang="es")
sentimientos_f = analizar_sentimiento(parrafos_f, "frigerio", analyzer)
sentimientos_r = analizar_sentimiento(parrafos_r, "romero", analyzer)
df_sentimientos = pd.DataFrame(sentimientos_f + sentimientos_r)

nlp = spacy.load("es_core_news_sm")
stopwords = nlp.Defaults.stop_words
excluir = {normalizar_palabra(w) for w in ['frigerio','rogeli','romero','rosario','gobernador','intendenta','gobierno','provincia','nacional','milei']}

frecuencias_df = contar_frecuencias(df_sentimientos)
grafo_df = construir_grafo_semantico(frecuencias_df)

# Guardar
noticias.to_csv(os.path.join(OUTPUT_PATH, "noticias_limpias.csv"), index=False)
df_sentimientos.to_csv(os.path.join(OUTPUT_PATH, "sentimiento_comentarios.csv"), index=False)
frecuencias_df.to_csv(os.path.join(OUTPUT_PATH, "frecuencia_palabras_sentimiento.csv"), index=False)
grafo_df.to_csv(os.path.join(OUTPUT_PATH, "grafo_palabras.csv"), index=False)
