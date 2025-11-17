# -*- coding: utf-8 -*-
"""
Consolidación semanal:
- Lee la última carpeta data/tmp/week_YYYY-MM-DD/ (o --week-dir)
- Valida columnas mínimas y fechas ISO (cuando existen)
- Mergea cada medio a data/raw/*.csv con dedupe por 'enlace'
- Construye data/noticias_unidas.csv (histórico completo)
- Genera insumos para Shiny en data/tablas/
    * frecuencias_por_dia.csv
    * sentimiento_diario_largo.csv
    * sentimiento_titulos_semana.csv
    * (opc) bertopic_nodes.csv, bertopic_edges.csv  [--bertopic]  ← siempre sobre el ÚLTIMO TRIMESTRE (90 días)
"""

import os, re, argparse, sys
from datetime import datetime
from glob import glob

import pandas as pd

# -------- Opcionales pesados (se cargan sólo si se usan) ----------
def _lazy_import_spacy():
    import spacy, unicodedata
    return spacy, unicodedata

def _lazy_import_pysentimiento():
    from pysentimiento import create_analyzer
    return create_analyzer

def _lazy_import_bertopic():
    from bertopic import BERTopic
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    from sklearn.cluster import KMeans
    return BERTopic, SentenceTransformer, cosine_similarity, np, KMeans

# ---------------- Paths ----------------
ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
RAW  = os.path.join(DATA, "raw")
TMP  = os.path.join(DATA, "tmp")
TAB  = os.path.join(DATA, "tablas")
os.makedirs(RAW, exist_ok=True)
os.makedirs(TAB, exist_ok=True)

HOY = datetime.now().strftime("%Y-%m-%d")
RANDOM_STATE = 42  # estabilidad entre corridas

# -------------- Utilidades --------------
def log(m): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)

REQ_COLS = ["medio","fecha","titulo","enlace"]  # 'contenido' puede faltar (fallback a titulo en frec.)
OUT_UNIFICADO = os.path.join(DATA, "noticias_unidas.csv")

MEDIOS = {
    "analisisdigital": ("analisisdigital_provinciales.tmp.csv", "analisisdigital_provinciales.csv"),
    "apfdigital":     ("apfdigital_provinciales.tmp.csv",       "apfdigital_provinciales.csv"),
    "elargentino":    ("elargentino_provincia.tmp.csv",         "elargentino_provincia.csv"),
}

def latest_week_dir():
    paths = sorted(glob(os.path.join(TMP, "week_*")))
    return paths[-1] if paths else None

def read_csv_safe(path):
    return pd.read_csv(path, encoding="utf-8")

def write_csv_safe(df, path):
    df.to_csv(path, index=False, encoding="utf-8-sig")

def dedupe_by_enlace(df):
    if df.empty: return df
    if "enlace" in df.columns:
        return df.drop_duplicates(subset=["enlace"]).reset_index(drop=True)
    keys = [c for c in ["titulo","medio","fecha"] if c in df.columns]
    return df.drop_duplicates(subset=keys).reset_index(drop=True)

def validate_df(name, df):
    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: faltan columnas requeridas: {missing}")
    if "fecha" in df.columns:
        ok = pd.to_datetime(df["fecha"], errors="coerce")
        na = ok.isna().sum()
        if na:
            log(f"[VALID] {name}: {na} filas con fecha no parseable (se mantienen).")

# -------------- 1) Cargar TMP semanal --------------
def load_week(week_dir):
    per_medio = {}
    total = 0
    for medio, (tmp_name, _) in MEDIOS.items():
        path = os.path.join(week_dir, tmp_name)
        if os.path.exists(path):
            df = read_csv_safe(path)
            validate_df(f"{medio}.tmp", df)
            per_medio[medio] = df
            total += len(df)
        else:
            per_medio[medio] = pd.DataFrame(columns=["medio","fecha","fecha_texto","fuente_fecha","titulo","contenido","enlace","seccion","fecha_de_extraccion"])
    log(f"[TMP] Semana: {week_dir} | total filas tmp={total}")
    return per_medio

# -------------- 2) Merge → RAW (histórico) --------------
def merge_into_raw(per_medio):
    added_stats = {}
    for medio, (_, raw_name) in MEDIOS.items():
        tmp_df = per_medio.get(medio, pd.DataFrame())
        raw_path = os.path.join(RAW, raw_name)
        if os.path.exists(raw_path):
            base = read_csv_safe(raw_path)
        else:
            base = pd.DataFrame(columns=tmp_df.columns if not tmp_df.empty else ["medio","fecha","fecha_texto","fuente_fecha","titulo","contenido","enlace","seccion","fecha_de_extraccion"])
        uni = pd.concat([base, tmp_df], ignore_index=True)
        uni = dedupe_by_enlace(uni)
        write_csv_safe(uni, raw_path)
        added = max(0, len(uni) - len(base))
        added_stats[medio] = {"agregadas": added, "total": len(uni)}
        log(f"[RAW] {medio}: +{added} (total={len(uni)}) → {raw_path}")
    return added_stats

# -------------- 3) Unificado global --------------
def build_unificado():
    dfs = []
    for _, (_, raw_name) in MEDIOS.items():
        path = os.path.join(RAW, raw_name)
        if os.path.exists(path):
            dfs.append(read_csv_safe(path))
    if not dfs:
        raise RuntimeError("No hay históricos en data/raw/*.csv para unificar.")
    uni = pd.concat(dfs, ignore_index=True)
    cols = uni.columns.tolist()
    if "medio" not in cols: uni["medio"] = ""
    if "titulo" not in cols: uni["titulo"] = ""
    if "contenido" not in cols and "titulo" in cols:
        uni["contenido"] = uni["titulo"]
    write_csv_safe(uni, OUT_UNIFICADO)
    d = pd.to_datetime(uni.get("fecha"), errors="coerce")
    fmin, fmax = (str(d.min().date()) if d.notna().any() else "-"), (str(d.max().date()) if d.notna().any() else "-")
    log(f"[UNI] {OUT_UNIFICADO} | filas={len(uni)} | rango={fmin} → {fmax}")
    return OUT_UNIFICADO

# -------------- 4) Frecuencia de palabras --------------
def gen_frecuencias_por_dia(path_unificado, out_path):
    spacy, unicodedata = _lazy_import_spacy()
    nlp = spacy.load("es_core_news_sm", disable=["ner"])
    df = read_csv_safe(path_unificado)
    df.columns = [c.lower().strip() for c in df.columns]
    if "contenido" not in df.columns and "titulo" in df.columns:
        df["contenido"] = df["titulo"]
    df["fecha"] = pd.to_datetime(df.get("fecha"), errors="coerce").dt.floor("D")
    df = df.dropna(subset=["fecha"]).copy()

    def _norm(s: str) -> str:
        s = (s or "").lower().strip()
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        return s

    stop_extra = {
        "año","años","día","días","mes","meses","entre","ser","estar","haber",
        "a","ante","bajo","con","contra","de","desde","en","hacia","hasta",
        "para","por","segun","sin","sobre","tras","un","una","unos","unas",
        "el","la","los","las","lo","y","o","u","e","que","como","mas","menos",
        "no","si","tambien","pero","porque","provincia","rio","río"
    }
    stop = {_norm(w) for w in (nlp.Defaults.stop_words | stop_extra)}

    from collections import Counter
    registros = []
    texts = df["contenido"].astype(str).tolist()
    fechas = df["fecha"].tolist()
    for fecha, doc in zip(fechas, nlp.pipe(texts, batch_size=50)):
        lemmas = []
        for t in doc:
            if not t.is_alpha: continue
            lem = _norm(t.lemma_)
            if len(lem) <= 2 or lem in stop: continue
            lemmas.append(lem)
        if lemmas:
            c = Counter(lemmas)
            for lem, freq in c.items():
                registros.append((fecha, lem, freq))

    if registros:
        out = pd.DataFrame(registros, columns=["fecha","lemma","frecuencia"])
        out = (out.groupby(["fecha","lemma"], as_index=False)["frecuencia"].sum()
                   .sort_values(["fecha","frecuencia"], ascending=[True, False]))
        out["fecha"] = pd.to_datetime(out["fecha"]).dt.strftime("%Y-%m-%d")
    else:
        out = pd.DataFrame(columns=["fecha","lemma","frecuencia"])

    write_csv_safe(out, out_path)
    log(f"[TAB] frecuencias_por_dia.csv → filas={len(out)}")

# -------------- 5) Sentimiento --------------
def gen_sentimientos(path_unificado, out_dia, out_tit):
    create_analyzer = _lazy_import_pysentimiento()
    df = read_csv_safe(path_unificado)
    df.columns = [c.lower().strip() for c in df.columns]
    if "titulo" not in df.columns:
        raise ValueError("No existe columna 'titulo' en noticias_unidas.csv")
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.floor("D")
    df = df.dropna(subset=["fecha","titulo"]).reset_index(drop=True)

    analyzer = create_analyzer(task="sentiment", lang="es")
    def get_sentiment(texto):
        try:
            r = analyzer.predict(str(texto))
            return r.output  # 'POS'|'NEG'|'NEU'
        except Exception:
            return "error"

    df["sentimiento"] = df["titulo"].astype(str).apply(get_sentiment)
    df = df[df["sentimiento"].isin(["NEG","NEU","POS"])].copy()

    diario = (df.groupby([df["fecha"].dt.date, "sentimiento"], as_index=False)
                .size().rename(columns={"fecha":"fecha","size":"cantidad"}))
    diario["fecha"] = pd.to_datetime(diario["fecha"]).dt.strftime("%Y-%m-%d")
    diario = diario.sort_values(["fecha","sentimiento"])
    write_csv_safe(diario, out_dia)
    log(f"[TAB] sentimiento_diario_largo.csv → filas={len(diario)}")

    df["semana_inicio"] = df["fecha"].dt.to_period("W-MON").apply(lambda p: p.start_time.normalize())
    df["semana_fin"]    = df["semana_inicio"] + pd.Timedelta(days=6)

    import re as _re
    def limpiar_titulo(s):
        s = str(s)
        s = _re.sub(r"\bprovincia\b","", s, flags=_re.IGNORECASE)
        s = _re.sub(r"\brio\b","", s, flags=_re.IGNORECASE)
        s = " ".join(s.split())
        return s.strip()

    tit = df[["fecha","semana_inicio","semana_fin","sentimiento","titulo"]].copy()
    tit["enlace"] = df["enlace"] if "enlace" in df.columns else pd.NA
    tit["medio"]  = df["medio"]  if "medio"  in df.columns else pd.NA
    tit["titulo_limpio"] = tit["titulo"].apply(limpiar_titulo)

    tit["fecha"]         = pd.to_datetime(tit["fecha"]).dt.strftime("%Y-%m-%d")
    tit["semana_inicio"] = pd.to_datetime(tit["semana_inicio"]).dt.strftime("%Y-%m-%d")
    tit["semana_fin"]    = pd.to_datetime(tit["semana_fin"]).dt.strftime("%Y-%m-%d")
    tit = tit.sort_values(["fecha"], ascending=False)

    write_csv_safe(tit, out_tit)
    log(f"[TAB] sentimiento_titulos_semana.csv → filas={len(tit)}")

# -------------- 6) BERTopic (trimestral) --------------
def gen_bertopic(path_unificado, out_nodes, out_edges):
    """
    Entrena BERTopic sólo con el ÚLTIMO TRIMESTRE (90 días) de noticias_unidas.csv.
    Si no hay fechas parseables, sigue sin filtrar. Exporta nodes/edges.
    """
    BERTopic, SentenceTransformer, cosine_similarity, np, KMeans = _lazy_import_bertopic()

    df = read_csv_safe(path_unificado)
    df.columns = [c.lower().strip() for c in df.columns]
    if "contenido" not in df.columns:
        if "titulo" in df.columns:
            df["contenido"] = df["titulo"]
        else:
            raise ValueError("No hay columnas 'contenido' ni 'titulo' en noticias_unidas.csv")
    df = df.dropna(subset=["contenido"]).reset_index(drop=True)

    # --- Ventana: último trimestre (90 días) ---
    tri_label = "SIN_FECHA"
    if "fecha" in df.columns:
        fecha_dt = pd.to_datetime(df["fecha"], errors="coerce")
        if fecha_dt.notna().any():
            tri_end = fecha_dt.max().normalize()
            tri_start = tri_end - pd.Timedelta(days=89)
            mask = (fecha_dt >= tri_start) & (fecha_dt <= tri_end)
            df = df.loc[mask].copy()
            tri_label = f"{tri_start.date()} – {tri_end.date()}"
    log(f"[BERTopic] Ventana aplicada: {tri_label} | filas={len(df)}")

    # Preprocesado simple
    import spacy, re
    nlp = spacy.load("es_core_news_sm", disable=["ner"])
    mis_stop = {"provincia","rio","río","entre","ríos","rios"}
    stop = nlp.Defaults.stop_words | mis_stop

    def limpiar_texto(txt):
        txt = str(txt).lower()
        txt = re.sub(r"http\S+|www\.\S+", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def to_lemmas(txt):
        doc = nlp(txt)
        lemas = []
        for t in doc:
            if not t.is_alpha: continue
            lem = t.lemma_.strip()
            if len(lem) <= 2 or lem in stop: continue
            lemas.append(lem)
        return " ".join(lemas)

    df["contenido_proc"] = df["contenido"].astype(str).apply(limpiar_texto).apply(to_lemmas)
    docs = df["contenido_proc"].tolist()

    if not docs:
        # Exportar vacíos pero con columnas correctas
        pd.DataFrame(columns=["id","label","size","keywords","community","community_rank"]).to_csv(out_nodes, index=False, encoding="utf-8-sig")
        pd.DataFrame(columns=["from","to","weight"]).to_csv(out_edges, index=False, encoding="utf-8-sig")
        log("[BERTopic] Sin documentos en ventana. Archivos vacíos exportados.")
        return

    sbert = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    emb = sbert.encode(docs, show_progress_bar=True, normalize_embeddings=True)

    model = BERTopic(language="multilingual",
                     calculate_probabilities=True,
                     verbose=True,
                     nr_topics="auto",
                     random_state=RANDOM_STATE)
    topics, probs = model.fit_transform(docs, emb)

    info = model.get_topic_info()
    info = info[info["Topic"] != -1].copy().sort_values("Count", ascending=False)

    def top_keywords(topic_id, k=10):
        rows = model.get_topic(topic_id) or []
        return ", ".join([w for (w, wgt) in rows[:k]])

    info["keywords"] = info["Topic"].apply(top_keywords)
    nodes = info.rename(columns={"Topic":"id","Count":"size"})
    nodes["label"] = nodes["keywords"]

    emb_topics = model.topic_embeddings_
    if emb_topics is None or (isinstance(emb_topics, list) and len(emb_topics) == 0):
        edges = pd.DataFrame(columns=["from","to","weight"])
        nodes["community"] = -1
        nodes["community_rank"] = -1
    else:
        freq = model.get_topic_freq().sort_values("Topic")
        freq = freq[freq["Topic"] != -1]
        emb_ids = freq["Topic"].tolist()
        topic_ids = nodes["id"].tolist()
        emb_mat = np.vstack([emb_topics[emb_ids.index(t)] for t in topic_ids])

        sim = cosine_similarity(emb_mat)
        rows = []
        for i in range(len(topic_ids)):
            for j in range(i+1, len(topic_ids)):
                rows.append((topic_ids[i], topic_ids[j], float(sim[i, j])))
        edges = pd.DataFrame(rows, columns=["from","to","weight"])

        kmeans = KMeans(n_clusters=4, random_state=RANDOM_STATE, n_init="auto")
        comm = kmeans.fit_predict(emb_mat)
        nodes["community"] = comm
        peso = nodes.groupby("community")["size"].sum().sort_values(ascending=False)
        rank_map = {c: r+1 for r, c in enumerate(peso.index)}
        nodes["community_rank"] = nodes["community"].map(rank_map)

    nodes[["id","label","size","keywords","community","community_rank"]].to_csv(out_nodes, index=False, encoding="utf-8-sig")
    edges.to_csv(out_edges, index=False, encoding="utf-8-sig")
    log(f"[TAB] bertopic_nodes.csv ({len(nodes)}) / bertopic_edges.csv ({len(edges)})")

# -------------- Main --------------
def main():
    ap = argparse.ArgumentParser(description="Consolidar semana → histórico + tablas Shiny")
    ap.add_argument("--week-dir", default="", help="Ruta a data/tmp/week_YYYY-MM-DD/ (si se omite, usa la última)")
    ap.add_argument("--bertopic", action="store_true", help="Recalcular BERTopic (último trimestre).")
    args = ap.parse_args()

    week_dir = args.week_dir or latest_week_dir()
    if not week_dir or not os.path.isdir(week_dir):
        raise SystemExit("No se encontró carpeta semanal en data/tmp/week_YYYY-MM-DD/. Corré primero el scrapper.")

    log(f"=== Inicio process_week | semana: {os.path.basename(week_dir)} ===")

    # 1) Cargar TMP
    per_medio = load_week(week_dir)

    # 2) Merge a RAW
    stats = merge_into_raw(per_medio)

    # 3) Unificado global
    path_unificado = build_unificado()

    # 4) Tablas Shiny
    gen_frecuencias_por_dia(path_unificado, os.path.join(TAB, "frecuencias_por_dia.csv"))
    gen_sentimientos(path_unificado,
                     os.path.join(TAB, "sentimiento_diario_largo.csv"),
                     os.path.join(TAB, "sentimiento_titulos_semana.csv"))

    if args.bertopic:
        gen_bertopic(path_unificado,
                     os.path.join(TAB, "bertopic_nodes.csv"),
                     os.path.join(TAB, "bertopic_edges.csv"))

    # Resumen
    log("--- Resumen ---")
    for medio, s in stats.items():
        log(f"{medio:14s} +{s['agregadas']}  total={s['total']}")
    log("Tablas generadas en data/tablas/")
    log("=== Fin process_week ===")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrumpido por usuario.")
        sys.exit(1)
