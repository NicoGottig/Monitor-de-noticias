

# Shiny: monitor de noticias


# app.R
options(shiny.trace = FALSE)

# ==== Librerías ====
library(shiny)
library(plotly)
library(dplyr)
library(readr)
library(lubridate)
library(bslib)
library(tidyr)
library(shinyWidgets)
library(stringr)
library(htmltools)
library(visNetwork)
library(purrr)
library(stringi)
library(waiter)
library(later)
library(scales)   # rescale()
library(tibble)

# ==== Paths ====
DIR_LOCAL_TABLAS <- "../seguimiento-de-noticias/scrapers/ultima version/tablas"
DIR_APP_DATA <- "data"

ensure_data <- function(files) {
  if (!dir.exists(DIR_APP_DATA)) dir.create(DIR_APP_DATA, recursive = TRUE)
  if (dir.exists(DIR_LOCAL_TABLAS)) {
    for (f in files) {
      src <- file.path(DIR_LOCAL_TABLAS, f)
      dst <- file.path(DIR_APP_DATA, f)
      if (!file.exists(dst) && file.exists(src)) file.copy(src, dst, overwrite = TRUE)
    }
  }
}

resolve_path <- function(fname, candidates = c(DIR_APP_DATA, ".")) {
  for (d in candidates) {
    p <- file.path(d, fname)
    if (file.exists(p)) return(p)
  }
  stop(sprintf("No encuentro '%s'. Colocalo en '%s' o definí DIR_LOCAL_TABLAS.", fname, DIR_APP_DATA), call. = FALSE)
}

ARCHIVOS_NECESARIOS <- c(
  "frecuencias_por_dia.csv",
  "sentimiento_diario_largo.csv",
  "sentimiento_titulos_semana.csv",
  # Explorador por palabra
  "term_cooc.csv",
  "terms.csv"
)
ensure_data(ARCHIVOS_NECESARIOS)

# ==== Stopwords personalizadas (para sección "Frecuencia de palabras") ====
USER_STOPWORDS <- c("río","rio","entre","ríos","provincia","provincial","gobierno","nacional","publico")
normalize_term <- function(x) stringi::stri_trans_general(tolower(x), "Latin-ASCII")
STOP_NORM <- normalize_term(USER_STOPWORDS)

limpiar_stop <- function(df, stop_norm = STOP_NORM) {
  df %>% mutate(lemma_norm = normalize_term(lemma)) %>% filter(!(lemma_norm %in% stop_norm))
}

# ==== Carga de datos base (palabras + sentimiento) ====
tabla_frec_dia <- read_csv(
  resolve_path("frecuencias_por_dia.csv"),
  locale = locale(encoding = "UTF-8"),
  col_types = cols(fecha = col_date(format = "%Y-%m-%d"))
)
min_fecha_pal <- min(tabla_frec_dia$fecha, na.rm = TRUE)
max_fecha_pal <- max(tabla_frec_dia$fecha, na.rm = TRUE)

sent_diario <- read_csv(
  resolve_path("sentimiento_diario_largo.csv"),
  locale = locale(encoding = "UTF-8"),
  col_types = cols(fecha = col_date(format = "%Y-%m-%d"),
                   sentimiento = col_character(),
                   cantidad = col_double())
)
min_fecha_sent <- min(sent_diario$fecha, na.rm = TRUE)
max_fecha_sent <- max(sent_diario$fecha, na.rm = TRUE)

sent_titulos <- read_csv(
  resolve_path("sentimiento_titulos_semana.csv"),
  locale = locale(encoding = "UTF-8"),
  col_types = cols(
    fecha = col_date("%Y-%m-%d"),
    semana_inicio = col_date("%Y-%m-%d"),
    semana_fin    = col_date("%Y-%m-%d"),
    sentimiento   = col_character(),
    titulo_limpio = col_character(),
    titulo        = col_character(),
    enlace        = col_character(),
    medio         = col_character()
  )
)

# ==== Carga co-ocurrencias globales (explorador por palabra) ====
term_cooc <- readr::read_csv(resolve_path("term_cooc.csv"), show_col_types = FALSE)
terms_df  <- readr::read_csv(resolve_path("terms.csv"), show_col_types = FALSE)

# Normalización
norma <- function(x) stringi::stri_trans_general(tolower(trimws(x)), "Latin-ASCII")
if (!"t1n" %in% names(term_cooc)) term_cooc$t1n <- norma(term_cooc$t1)
if (!"t2n" %in% names(term_cooc)) term_cooc$t2n <- norma(term_cooc$t2)
if (!"termn" %in% names(terms_df)) terms_df$termn <- norma(terms_df$term)

# Stopwords para el GRAFO (defensivo; no afecta "Frecuencia de palabras")
GRAPH_STOPWORDS <- c(
  "apfdigital","analisisdigital","elargentino","el once","elonce",
  "unoentrerios","uno entre rios","entre rios","entre ríos","entrerios",
  "provincia","provincial","gobierno","nacional","publico","público"
)
GRAPH_STOP_NORM <- norma(GRAPH_STOPWORDS)

# Filtrado defensivo por stopwords para el grafo
term_cooc <- term_cooc %>% filter(!(t1n %in% GRAPH_STOP_NORM | t2n %in% GRAPH_STOP_NORM))
terms_df  <- terms_df  %>% filter(!(termn %in% GRAPH_STOP_NORM))

# ==== Paleta ====
color <- list(
  fondo   = "#181A1B",
  texto   = "#D6D9E0",
  detalle = "#23262B",
  primario= "#69A5E6",
  neg     = "#E1604B",
  neu     = "#B8BFC7",
  pos     = "#69A5E6"
)

# ==== Helpers UI ====
header_ui <- function(title, descr_html) {
  tags$header(
    class = "section-wrap site-header",
    h1(class = "page-title", title),
    div(class = "descripcion lead-descr", HTML(descr_html)),
    hr()
  )
}

section_ui <- function(id, title, left, right = NULL, after = NULL) {
  tags$section(
    id = id, class = "section-wrap",
    h2(class = "section-title", title),
    fluidRow(
      column(8, left),
      if (!is.null(right)) column(4, right)
    ),
    if (!is.null(after)) after,
    hr()
  )
}

# ==== UI ====
ui <- fluidPage(
  theme = bs_theme(bg = color$fondo, fg = color$texto, primary = color$primario),
  
  # Waiter
  use_waiter(),
  waiter_show_on_load(
    html = tagList(
      div(style="display:flex;flex-direction:column;align-items:center;gap:16px;"),
      div(style="font-size:26px;font-weight:600;letter-spacing:.3px;",
          HTML("Iniciando <span style='color:#69A5E6;'>monitor de medios</span>")),
      spin_fading_circles()
    ),
    color = color$fondo
  ),
  autoWaiter(
    id    = c("plot_frecuencias", "plot_sent_dona", "plot_sent_semanal", "kw_graph"),
    html  = spin_fading_circles(),
    color = "rgba(24,26,27,0.88)"
  ),
  
  tags$head(
    tags$meta(charset = "utf-8"),
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1"),
    tags$script(HTML('document.documentElement.lang="es";')),
    tags$link(rel = "stylesheet", href = "styles.css")
  ),
  
  header_ui(
    "¿De qué se habla en Entre Ríos?",
    paste0(
      "Explorá la agenda informativa de los medios digitales entrerrianos: palabras más repetidas, 
      clima de opinión y evolución del sentimiento. La demo incluye noticias de Análisis Digital, APF Digital y Diario El Argentino, por su sección 'provinciales'. ",
      "Última actualización: 9 de agosto de 2025."
    )
  ),
  
  # ===== Frecuencia de palabras =====
  section_ui(
    id = "palabras",
    title = "Frecuencia de palabras",
    left  = plotlyOutput("plot_frecuencias", height = "480px"),
    right = tagList(
      div(class="descripcion",
          "Mirá las palabras más utilizadas en los medios relevados. Filtrá por período y elegí cuántas mostrar."),
      br(),
      sliderInput(
        "slider_palabras", "Rango de fechas",
        min = min_fecha_pal, max = max_fecha_pal,
        value = c(max_fecha_pal - 30, max_fecha_pal),
        timeFormat = "%Y-%m-%d", step = 1
      ),
      div(class = "nota-italica", "Seleccioná el rango de fechas de las noticias a analizar."),
      br(),
      numericInput("topn_pal", "Cantidad de palabras a mostrar (Top N)", value = 20, min = 5, max = 100, step = 5),
      div(class = "nota-italica", "Elegí la cantidad de palabras más frecuentes que querés visualizar.")
    )
  ),
  
  # ===== Sentimiento general =====
  section_ui(
    id = "sentimiento",
    title = "Sentimiento general",
    left  = plotlyOutput("plot_sent_dona", height = "360px"),
    right = tagList(
      div(class="descripcion",
          "Explorá el sentimiento general en un rango de fechas. 
          Los titulares se clasifican automáticamente con un modelo de análisis de sentimientos en español 
          en 'positivo', 'negativo' o 'neutro'."), br(),
      sliderInput(
        "slider_sent", "Rango de fechas (dona)",
        min = min_fecha_sent, max = max_fecha_sent,
        value = c(max_fecha_sent - 30, max_fecha_sent),
        timeFormat = "%Y-%m-%d", step = 1
      ),
      div(class="nota-italica", "La dona muestra la proporción en el período elegido. La clasificación de sentimientos es automática y puede sesgarse en titulares editorializados.")
    )
  ),
  
  # ===== Evolución diaria del sentimiento + títulos =====
  # ===== Evolución diaria del sentimiento + títulos =====
  tags$section(
    class="section-wrap",
    h2(class="section-title", "Evolución diaria del sentimiento"),
    fluidRow(
      # --- Izquierda: gráfico + controles debajo ---
      column(
        width = 8,
        div(class="descripcion", 
            "Evolución diaria de sentimientos en titulares, con barras apiladas por día. 
            También consultá títulos dentro del rango y por tipo de sentimiento."),
        br(),
        plotlyOutput("plot_sent_semanal", height = "420px"),
        br(),
        # Selector abajo del gráfico
        radioGroupButtons(
          inputId = "modo_semana",
          label   = NULL,
          choices = c("Recuento", "Porcentaje"),
          selected = "Recuento",
          justified = FALSE,
          size = "sm"
        ),
        div(class="nota-italica",
            "Alterná entre recuento absoluto y % del total diario. 
            Si un día tiene pocas noticias, las variaciones pueden verse más bruscas.")
      ),
      
      # --- Derecha: deslizador + lista de títulos ---
      column(
        width = 4,
        tags$aside(
          # Deslizador movido a la derecha
          sliderInput(
            "slider_sent_evo", "Rango de fechas (evolución)",
            min = min_fecha_sent, max = max_fecha_sent,
            value = c(max_fecha_sent - 30, max_fecha_sent),
            timeFormat = "%Y-%m-%d", step = 1
          ),
          div(class="nota-italica", "Este control afecta el gráfico y el listado de títulos."),
          br(),
          radioGroupButtons(
            inputId = "sent_sel",
            label   = "Sentimiento a listar",
            choices = c("NEG","NEU","POS"),
            selected = "NEG",
            justified = TRUE,
            size = "sm"
          ),
          div(class="box-titulos", htmlOutput("lista_titulos"))
        )
      )
    ),
    hr()
  )
  ,
  
  # ===== Explorador por palabra clave =====
  section_ui(
    id = "keyword_graph",
    title = "Explorador por palabra clave",
    left  = tagList(
      # Descripción breve del grafo
      div(class="descripcion",
          "¿Qué muestra? Un grafo de co-ocurrencias en notas: cada ",
          tags$b("nodo"), " es una palabra; su tamaño refleja cuán frecuente es. ",
          tags$b("Las líneas"), " conectan términos que aparecen juntos y su grosor ",
          "representa la fuerza de asociación (NPMI o recuento). ",
          "Sirve para ver con qué temas se vincula la palabra elegida."),
      br(),
      visNetworkOutput("kw_graph", height = "560px"),
      br(),
      div(class = "nota-italica", textOutput("kw_notice", inline = TRUE))
    ),
    right = tagList(
      textInput(
        "kw", "Palabra clave", value = "salud",
        placeholder = "ej.: hospital / paritaria / salario"
      ),
      div(class="nota-italica",
          "Escribí una palabra para explorar sus asociaciones en las noticias."),
      br(),
      numericInput("kw_k", "Vecinos a mostrar (Top K)",
                   value = 12, min = 5, max = 50, step = 1),
      div(class="nota-italica",
          "Ajustá cuántos términos relacionados querés incluir en el grafo."),
      br(),
      
      # (La opción de enlazar vecinos ya no se muestra: queda siempre activada)
      radioGroupButtons(
        inputId = "kw_metric",
        label   = "Métrica para ordenar/aristas",
        choices = c("NPMI","co"),
        selected = "NPMI",
        justified = TRUE,
        size = "sm"
      ),
      div(class="nota-italica",
          "NPMI resalta vínculos más específicos aunque poco frecuentes; co muestra las conexiones más comunes en número de apariciones."),
      sliderInput("kw_npmi_min", "Umbral NPMI (enlaces)", 
                  min = -0.2, max = 0.8, value = 0.0, step = 0.05),
      div(class="nota-italica",
          tags$b("NPMI"), " mide qué tan asociadas están dos palabras: ",
          "valores más altos ⇒ asociación más fuerte. Subí el umbral para mostrar ",
          "solo vínculos más relevantes."),
      br(),
      
      numericInput("kw_min_co", "Mínimo co-ocurrencias (enlaces)",
                   value = 5, min = 1, max = 100, step = 1),
      numericInput("kw_max_edges_per_node", "Máx. aristas por vecino",
                   value = 4, min = 1, max = 20, step = 1),
      
      # Renombrado
      checkboxInput("kw_use_physics", "Editar posición", value = TRUE)
      
      # (El checkbox 'Usar fallback...' se elimina de la UI)
    )
  )
  ,
  
  # Footer
  tags$footer(
    class="section-wrap",
    style="width:100%;color:#B6BAC5;text-align:center;font-size:15px;padding:16px 0 9px 0;margin-top:20px;border-top:1px solid #23262B;letter-spacing:0.5px;",
    HTML('Hecho con <span style="font-size:18px;">💙</span> por Nico')
  )
)

# ==== Server ====
server <- function(input, output, session) {
  # splash mínimo
  MIN_SPLASH <- 1.2
  session$onFlushed(function() {
    later::later(function() { withReactiveDomain(session, { waiter_hide() }) }, delay = MIN_SPLASH)
  }, once = TRUE)
  
  # ---------- Frecuencia de palabras ----------
  tabla_frec_filtrada <- reactive({
    req(input$slider_palabras)
    tabla_frec_dia %>%
      filter(
        fecha >= as.Date(input$slider_palabras[1]),
        fecha <= as.Date(input$slider_palabras[2])
      ) %>%
      limpiar_stop(STOP_NORM) %>%
      group_by(lemma) %>%
      summarise(frecuencia = sum(frecuencia), .groups = "drop") %>%
      arrange(desc(frecuencia)) %>%
      slice_head(n = input$topn_pal)
  })
  
  output$plot_frecuencias <- renderPlotly({
    df <- tabla_frec_filtrada()
    if (nrow(df) == 0) {
      return(
        plot_ly() %>%
          layout(
            annotations = list(list(
              text = "Sin datos en el período seleccionado",
              x = 0.5, y = 0.5, showarrow = FALSE,
              font = list(color="#ECECEC", size=18)
            )),
            xaxis = list(visible = FALSE),
            yaxis = list(visible = FALSE),
            plot_bgcolor = color$fondo, paper_bgcolor = color$fondo,
            margin = list(l=45, r=5, t=10, b=40)
          )
      )
    }
    plot_ly(
      df,
      x = ~frecuencia, y = ~reorder(lemma, frecuencia),
      type = "bar", orientation = "h",
      marker = list(color = color$primario),
      hoverinfo = "text",
      hovertext = ~paste0(lemma, ": ", frecuencia)
    ) %>%
      layout(
        xaxis = list(title = "Frecuencia", gridcolor=color$detalle,
                     tickfont=list(color=color$texto), titlefont=list(color=color$texto)),
        yaxis = list(title = "", tickfont=list(color=color$texto)),
        plot_bgcolor = color$fondo, paper_bgcolor = color$fondo,
        margin = list(l=110, r=20, t=10, b=40)
      )
  })
  
  # ---------- Sentimiento (dona) ----------
  sent_diario_filtrado <- reactive({
    req(input$slider_sent)
    sent_diario %>%
      filter(
        fecha >= as.Date(input$slider_sent[1]),
        fecha <= as.Date(input$slider_sent[2])
      )
  })
  
  output$plot_sent_dona <- renderPlotly({
    df <- sent_diario_filtrado() %>% group_by(sentimiento) %>% summarise(cantidad = sum(cantidad), .groups = "drop")
    df$sentimiento <- factor(df$sentimiento, levels = c("NEG","NEU","POS"))
    colores <- c("NEG"=color$neg, "NEU"=color$neu, "POS"=color$pos)
    nombres <- c("NEG"="Negativo","NEU"="Neutro","POS"="Positivo")
    
    if (nrow(df) == 0 || sum(df$cantidad) == 0) {
      return(
        plot_ly(labels="Sin datos", values=1, type="pie", hole=0.63,
                marker=list(colors="#888", line=list(color=color$fondo,width=2)),
                textinfo="none", hoverinfo="label", sort=FALSE) %>%
          layout(autosize=TRUE, showlegend=FALSE,
                 paper_bgcolor=color$fondo, plot_bgcolor=color$fondo,
                 margin=list(t=16,b=6,l=10,r=10),
                 annotations=list(list(text="Sin datos",x=0.5,y=0.5,
                                       font=list(color=color$texto,size=24),
                                       showarrow=FALSE)))
      )
    }
    
    mayor <- df$sentimiento[which.max(df$cantidad)]
    pct   <- round(100*max(df$cantidad)/sum(df$cantidad))
    
    plot_ly(labels = nombres[df$sentimiento], values = df$cantidad,
            type="pie", hole=0.63,
            marker=list(colors=colores[df$sentimiento], line=list(color=color$fondo,width=2)),
            textinfo="none", hoverinfo="label+percent", sort=FALSE) %>%
      layout(autosize=TRUE, showlegend=FALSE,
             paper_bgcolor=color$fondo, plot_bgcolor=color$fondo,
             margin=list(t=16,b=6,l=10,r=10),
             annotations=list(list(
               text=paste0(pct,"%<br>", nombres[as.character(mayor)]),
               x=0.5,y=0.5,font=list(color=color$texto,size=24),
               showarrow=FALSE)))
  })
  
  # ---------- Evolución diaria ----------
  sent_diario_dia <- reactive({
    req(input$slider_sent_evo)
    sent_diario %>%
      filter(fecha >= as.Date(input$slider_sent_evo[1]),
             fecha <= as.Date(input$slider_sent_evo[2])) %>%
      group_by(fecha, sentimiento) %>%
      summarise(cantidad = sum(cantidad), .groups = "drop") %>%
      mutate(sentimiento = factor(sentimiento, levels = c("NEG","NEU","POS")))
  })
  
  output$plot_sent_semanal <- renderPlotly({
    dfl <- sent_diario_dia()
    if (nrow(dfl) == 0) {
      return(
        plot_ly() %>%
          layout(
            annotations = list(list(
              text = "Sin datos en el período seleccionado",
              x = 0.5, y = 0.5, showarrow = FALSE,
              font = list(color = color$texto, size = 18)
            )),
            xaxis = list(visible = FALSE),
            yaxis = list(visible = FALSE),
            plot_bgcolor = color$fondo, paper_bgcolor = color$fondo
          )
      )
    }
    
    colores <- c("NEG"=color$neg, "NEU"=color$neu, "POS"=color$pos)
    
    if (identical(input$modo_semana, "Porcentaje")) {
      dflp <- dfl %>%
        group_by(fecha) %>%
        mutate(total = sum(cantidad), pct = ifelse(total > 0, 100 * cantidad/total, 0)) %>%
        ungroup()
      
      plot_ly(
        dflp,
        x = ~fecha, y = ~pct, color = ~sentimiento, colors = colores,
        type = "bar",
        hoverinfo = "text",
        hovertext = ~paste0(format(fecha, "%Y-%m-%d"), "<br>", as.character(sentimiento), ": ",
                            sprintf('%.1f%%', pct), " (", cantidad, ")")
      ) %>%
        layout(
          barmode = "stack",
          xaxis = list(title = "Día", tickfont = list(color = color$texto)),
          yaxis = list(title = "Porcentaje (%)", range = c(0,100),
                       gridcolor = color$detalle, tickfont = list(color = color$texto)),
          legend = list(orientation = "h", x = 0.5, y = 1.1, xanchor = "center",
                        font = list(color = color$texto)),
          plot_bgcolor = color$fondo, paper_bgcolor = color$fondo,
          margin = list(t=40, b=60, l=54, r=12)
        )
      
    } else {
      plot_ly(
        dfl,
        x = ~fecha, y = ~cantidad, color = ~sentimiento, colors = colores,
        type = "bar",
        text = ~cantidad, hoverinfo = "text",
        hovertext = ~paste0(format(fecha, "%Y-%m-%d"), "<br>", as.character(sentimiento), ": ", cantidad)
      ) %>%
        layout(
          barmode = "stack",
          xaxis = list(title = "Día", tickfont = list(color = color$texto)),
          yaxis = list(title = "Cantidad de noticias",
                       gridcolor = color$detalle, zerolinecolor = "#3B4251",
                       tickfont = list(color = color$texto)),
          legend = list(orientation = "h", x = 0.5, y = 1.1, xanchor = "center",
                        font = list(color = color$texto)),
          plot_bgcolor = color$fondo, paper_bgcolor = color$fondo,
          margin = list(t=40, b=60, l=54, r=12)
        )
    }
  })
  
  # ---------- Títulos por sentimiento (lista a la derecha) ----------
  titulos_filtrados <- reactive({
    req(input$slider_sent_evo, input$sent_sel)
    s <- toupper(input$sent_sel)
    sent_titulos %>%
      filter(
        !is.na(fecha),
        fecha >= as.Date(input$slider_sent_evo[1]),
        fecha <= as.Date(input$slider_sent_evo[2]),
        toupper(sentimiento) == s
      ) %>%
      arrange(desc(fecha)) %>%
      slice_head(n = 80)   # podés ajustar el tope
  })
  
  output$lista_titulos <- renderUI({
    df <- titulos_filtrados()
    if (nrow(df) == 0) {
      return(HTML("<div class='nota-italica'>No hay títulos para ese período/sentimiento.</div>"))
    }
    
    items <- lapply(seq_len(nrow(df)), function(i) {
      titulo <- htmlEscape(df$titulo[i] %||% df$titulo_limpio[i] %||% "")
      enlace <- df$enlace[i] %||% "#"
      medio  <- htmlEscape(df$medio[i] %||% "")
      fecha  <- if (!is.na(df$fecha[i])) format(df$fecha[i], "%Y-%m-%d") else ""
      
      tags$li(
        class = "item-titulo",               # <<--- clase que tu CSS ya estilaba
        style = "list-style:none; margin:0 0 10px 0; padding:0;",
        tags$a(titulo, href = enlace, target = "_blank", rel = "noopener noreferrer"),
        tags$div(class = "item-meta", sprintf("%s · %s", fecha, medio))  # <<--- idem
      )
    })
    
    tags$ul(style = "padding-left:0; margin:0;", items)
  })
  
  
  # ---------- Explorador por palabra clave (mejorado) ----------
  kw_graph_data <- reactive({
    req(input$kw)
    kw_norm <- norma(input$kw)
    k <- input$kw_k
    if (is.null(k) || length(k) != 1 || is.na(k) || k < 1) k <- 12
    
    # Validación: keyword filtrada por stopwords o inexistente en vocab
    if (kw_norm %in% GRAPH_STOP_NORM) {
      return(list(nodes = tibble(id = input$kw, label = input$kw, value = 28, group = "keyword"),
                  edges = tibble(from=character(), to=character(), width=numeric()),
                  fallback = FALSE,
                  msg = "La palabra está filtrada por stopwords del grafo.",
                  empty = TRUE))
    }
    kw_in_vocab <- any(terms_df$termn == kw_norm)
    
    # Co-ocurrencias de la keyword
    dfk <- term_cooc %>%
      filter(t1n == kw_norm | t2n == kw_norm)
    
    if (nrow(dfk) == 0) {
      # Sin co-ocurrencias: por defecto NO hacemos fallback
      if (!isTRUE(input$kw_use_fallback) || !kw_in_vocab) {
        return(list(
          nodes = tibble(id = input$kw, label = input$kw, value = 28, group = "keyword"),
          edges = tibble(from=character(), to=character(), width=numeric()),
          fallback = FALSE,
          msg = if (!kw_in_vocab)
            "La palabra no está en el corpus (o fue filtrada)."
          else
            "No hay co-ocurrencias registradas para esa palabra.",
          empty = TRUE
        ))
      }
      # Fallback explícito (si lo activan): top-K por DF (sin aristas reales)
      vecinos <- terms_df %>%
        filter(termn != kw_norm) %>%
        arrange(desc(df)) %>%
        slice_head(n = k) %>%
        pull(term)
      dfk <- tibble(vecino = vecinos, co = NA_real_, npmi = NA_real_)
      use_fallback <- TRUE
    } else {
      # Preparar ranking por métrica
      dfk <- dfk %>%
        mutate(
          vecino = ifelse(t1n == kw_norm, t2, t1),
          metric = if (identical(input$kw_metric, "co")) co else npmi
        ) %>%
        arrange(desc(metric), desc(co)) %>%
        slice_head(n = k)
      use_fallback <- FALSE
    }
    
    # Armar nodos
    df_terms <- terms_df %>% select(term, df, termn)
    kw_df <- df_terms %>% filter(termn == kw_norm) %>% slice_head(n = 1)
    kw_label <- if (nrow(kw_df)) kw_df$term[1] else input$kw
    kw_size  <- if (nrow(kw_df)) kw_df$df[1] else max(1, round(median(df_terms$df, na.rm = TRUE)))
    
    vec_df <- df_terms %>% filter(term %in% dfk$vecino)
    df_vals <- vec_df$df[match(dfk$vecino, vec_df$term)]
    df_vals[is.na(df_vals)] <- 1  # vectorizado
    
    nodes <- tibble(
      id    = c(kw_label, dfk$vecino),
      label = c(kw_label, dfk$vecino),
      df    = c(kw_size, df_vals),
      group = c("keyword", rep("vecino", nrow(dfk)))
    )
    rng <- range(nodes$df, na.rm = TRUE)
    nodes$value <- if (diff(rng) == 0) rep(24, nrow(nodes)) else 12 + 36 * (nodes$df - rng[1]) / diff(rng)
    
    # Aristas keyword-vecino
    if (use_fallback) {
      edges_k <- tibble(from = kw_label, to = dfk$vecino, width = 1.5)
    } else {
      # ancho por métrica elegida
      if (identical(input$kw_metric, "co")) {
        # escalar 'co'
        co_range <- range(dfk$co, na.rm = TRUE); if (!all(is.finite(co_range))) co_range <- c(0, 1)
        widths <- 1 + 4 * rescale(dfk$co, to = c(0, 1), from = co_range)
      } else {
        # escalar 'npmi'
        w_ok <- dfk$npmi[is.finite(dfk$npmi)]; w_range <- if (length(w_ok)) range(w_ok, na.rm = TRUE) else c(0, 1)
        widths <- ifelse(is.finite(dfk$npmi), 1 + 4 * rescale(dfk$npmi, to = c(0, 1), from = w_range), 2)
      }
      edges_k <- tibble(from = kw_label, to = dfk$vecino, width = widths)
    }
    
    # Aristas entre vecinos (subgrafo inducido) — opcional
    edges_nn <- tibble(from=character(), to=character(), width=numeric())
    if (!use_fallback && nrow(dfk) > 1) {
      S_norm <- norma(c(kw_label, dfk$vecino))
      # Pares en term_cooc con ambos términos en S_norm
      sub_edges <- term_cooc %>%
        filter(t1n %in% S_norm, t2n %in% S_norm) %>%
        # umbrales
        filter(
          co >= input$kw_min_co,
          npmi >= input$kw_npmi_min
        ) %>%
        mutate(
          a = t1n, b = t2n,
          # ancho por métrica elegida
          width = if (identical(input$kw_metric, "co")) {
            co_r <- range(co, na.rm = TRUE); if (!all(is.finite(co_r))) co_r <- c(0, 1)
            1 + 3.5 * rescale(co, to = c(0, 1), from = co_r)
          } else {
            npmi_r <- range(npmi, na.rm = TRUE); if (!all(is.finite(npmi_r))) npmi_r <- c(0, 1)
            1 + 3.5 * rescale(npmi, to = c(0, 1), from = npmi_r)
          }
        )
      
      # Mapear nombres normalizados -> labels para mostrar
      name_map <- setNames(nm = norma(nodes$label), object = nodes$label)
      # Filtrar para que no conecte la keyword consigo misma (no hay loops)
      sub_edges <- sub_edges %>% filter(!(a == norma(kw_label) & b == norma(kw_label)))
      # Map a labels
      from_lbl <- name_map[sub_edges$a]; to_lbl <- name_map[sub_edges$b]
      # Quitar NAs
      ok <- !is.na(from_lbl) & !is.na(to_lbl)
      sub_edges <- sub_edges[ok, , drop = FALSE]
      from_lbl <- from_lbl[ok]; to_lbl <- to_lbl[ok]
      
      # Limitar aristas por vecino (top por width)
      if (nrow(sub_edges)) {
        sub_tbl <- tibble(from = as.character(from_lbl), to = as.character(to_lbl), width = sub_edges$width)
        # quitar aristas duplicadas (no dirigidas)
        sub_tbl <- sub_tbl %>%
          mutate(a = pmin(from, to), b = pmax(from, to)) %>%
          group_by(a, b) %>%
          summarise(width = max(width), .groups = "drop") %>%
          rename(from = a, to = b)
        
        # cap por nodo
        cap <- max(1, input$kw_max_edges_per_node)
        sub_tbl <- bind_rows(
          sub_tbl %>% group_by(from) %>% slice_max(order_by = width, n = cap, with_ties = FALSE) %>% ungroup(),
          sub_tbl %>% group_by(to)   %>% slice_max(order_by = width, n = cap, with_ties = FALSE) %>% ungroup()
        ) %>%
          distinct(from, to, .keep_all = TRUE)
        
        # remover aristas duplicadas con las de la keyword
        kw_pairs <- tibble(a = pmin(kw_label, dfk$vecino), b = pmax(kw_label, dfk$vecino))
        sub_tbl <- sub_tbl %>%
          anti_join(kw_pairs %>% rename(from = a, to = b), by = c("from","to"))
        
        edges_nn <- sub_tbl
      }
    }
    
    edges <- bind_rows(edges_k, edges_nn)
    list(nodes = nodes, edges = edges, fallback = use_fallback, msg = NULL, empty = FALSE)
  })
  
  output$kw_notice <- renderText({
    gd <- kw_graph_data()
    if (isTRUE(gd$empty)) {
      return(gd$msg %||% "No hay datos para dibujar el grafo con esa palabra.")
    }
    if (isTRUE(gd$fallback)) {
      "Sin co-ocurrencias: mostrando vecinos por frecuencia global (sin relación directa)."
    } else {
      if (isTRUE(input$kw_link_neighbors)) {
        "Aristas ponderadas por la métrica elegida. También se enlazan vecinos entre sí según NPMI/co y umbrales."
      } else {
        "Aristas ponderadas por la métrica elegida."
      }
    }
  })
  
  output$kw_graph <- renderVisNetwork({
    gd <- kw_graph_data()
    nodes <- gd$nodes; edges <- gd$edges
    
    visNetwork(
      nodes = nodes %>% transmute(id, label, value, group),
      edges = edges %>% transmute(from, to, width)
    ) %>%
      visNodes(
        shape = "dot",
        font  = list(color = color$texto),
        color = list(border = "#BBBBBB",
                     highlight = list(background = "#2A2F35", border = color$primario))
      ) %>%
      visOptions(highlightNearest = list(enabled = TRUE, degree = 1),
                 nodesIdSelection = FALSE) %>%
      visLegend(enabled = FALSE) %>%
      visPhysics(
        enabled = isTRUE(input$kw_use_physics),
        solver  = "forceAtlas2Based",
        forceAtlas2Based = list(gravitationalConstant = -40,
                                centralGravity = 0.012,
                                springLength = 120,
                                springConstant = 0.08,
                                avoidOverlap = 0.2),
        stabilization = list(enabled = TRUE, iterations = 800)
      ) %>%
      visInteraction(hover = TRUE, dragNodes = TRUE, multiselect = TRUE) %>%
      visLayout(randomSeed = 11)
  })
}

# ==== Run ====
shinyApp(ui, server)
