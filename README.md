<h1>📰 Monitor de noticias</h1>

<p>
  Este repositorio implementa un <strong>pipeline completo para recolectar, procesar y visualizar noticias locales</strong> orientadas realizar seguimiento de medios en Entre Ríos, Argentina.  
  El proyecto fue creado a modo experimental para aprender web scraping, procesamiento de texto y visualización con R/Shiny.
</p>

<hr />

<h2>🔧 Tecnologías utilizadas</h2>
<ul>
  <li><strong>Python</strong> – requests, BeautifulSoup, Selenium, pandas, numpy, scikit‑learn, sentence-transformers, pysentimiento</li>
  <li><strong>R</strong> – Shiny, dplyr, plotly, visNetwork y tidyverse para la app interactiva</li>
  <li><strong>Jupyter Notebooks</strong> para exploración, limpieza y análisis</li>
  <li><strong>Selenium + webdriver-manager</strong> para scraping dinámico</li>
  <li><strong>BERTopic</strong> y <strong>SentenceTransformers</strong> para análisis de temas </li>
</ul>

<hr />

<h2>📁 Estructura del proyecto</h2>

<h3><code>documentacion/</code></h3>
<p>
  Carpeta para la documentación del proyecto. Contiene un documento <code>protocolo_validacion.docx</code> con pautas de validación y metodología.
</p>

<h3><code>notebooks/</code></h3>
<p>
  Conjunto de notebooks de desarrollo y experimentación:
</p>
<ul>
  <li><strong>00_extraccion.ipynb</strong> – prototipo inicial de extracción manual de noticias.</li>
  <li><strong>00_extracción_automatizada.ipynb</strong> – pruebas de extracción automatizada.</li>
  <li><strong>01_limpieza.ipynb</strong> – rutina de limpieza y normalización de datos.</li>
  <li><strong>02_temas.ipynb</strong> – análisis exploratorio de temas con modelos de lenguaje.</li>
  <li><strong>03_candidatos.ipynb</strong> – filtrado y análisis de menciones a candidatos.</li>
</ul>

<h3><code>scrapers/</code></h3>
<p>
  Scripts de scraping para distintos medios de Entre Ríos, con filtros por nombres de candidatos y localidades. Generan archivos CSV en <code>data/raw/</code> con backups incrementales.
</p>
<ul>
  <li><strong>analisisdigital.py</strong> – raspador para <em>Análisis Digital</em>, con paginación y guardado incremental.</li>
  <li><strong>apfdigital.py</strong> – raspador completo para APF Digital; combina Selenium para navegar listados y requests para detalles; utiliza fechas de corte y filtros de palabras.</li>
  <li><strong>elonce.py</strong> – scraper para el portal Elonce, con scroll automatizado y deduplicación:contentReference.</li>
  <li><strong>unodigital.py</strong> – scraping sin Selenium para Uno Entre Ríos, iterando por páginas y extrayendo títulos, copetes y contenido:contentReference.</li>
  <li><strong>scraper_semanal.py</strong> – orquestador semanal que ejecuta todos los scrapers, controla ventanas de fechas y guarda resultados.</li>
  <li><strong>process_week.py</strong> – consolida los CSV semanales en históricos, construye unificado global y genera tablas para la app Shiny (frecuencias diarias, sentimiento, co‑ocurrencias, etc.).</li>
  <li><strong>process_week.py</strong> y <strong>pipeline_limpieza.py</strong> cuentan con funciones para limpieza, deduplicación, análisis de sentimiento y construcción de grafos semánticos usando embeddings.</li>
</ul>

<h3><code>version final app/</code></h3>
<p>
  Contiene la versión final de la <strong>app interactiva en Shiny</strong> que permite explorar las noticias procesadas. El archivo principal <code>app.R</code> monta una interfaz con visualizaciones interactivas (gráficos de frecuencias diarias, sentimiento, tablas de títulos, grafo de co‑ocurrencias, etc.) y define temas oscuros y helpers de UI.  
  La subcarpeta <code>www/</code> incluye <code>styles.css</code> con la paleta de colores y estilos oscuros personalizados.  
  La carpeta <code>rsconnect/shinyapps.io/</code> almacena metadatos de despliegue a shinyapps.io.
</p>

<h3>Otros archivos</h3>
<ul>
  <li><strong>logs.txt</strong> – registro de ejecución de los scrapers.</li>
  <li><strong>seguimiento-de-noticias.Rproj</strong> – proyecto de RStudio para la app.</li>
  <li><strong>.gitignore</strong> – ignora datos crudos, logs y archivos temporales.</li>
</ul>

<hr />

<h2>✨ Objetivo</h2>
<p>
  El objetivo fue desarrollar un sistema que permita <strong>monitorear la cobertura mediática de figuras y ciudades clave</strong>, realizando:
</p>
<ul>
  <li>Extracción automatizada de noticias desde varios portales locales.</li>
  <li>Limpieza, unificación y enriquecimiento con análisis de sentimiento.</li>
  <li>Exploración de temas y co‑ocurrencias mediante modelos de lenguaje.</li>
  <li>Visualización en una app Shiny para consultar frecuencias, sentimientos y conexiones semánticas de manera interactiva.</li>
</ul>

<hr />

<h2>🏃 Cómo usar</h2>
<p>
  Acceso a la app: </br>
<a href = "https://mj8qpg-nicolas-gottig.shinyapps.io/app-monitor-noticias/">Monitor de noticias </a>
</p>
