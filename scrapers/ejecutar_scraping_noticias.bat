@echo off
REM --- Batch para correr todos los scrapers con fecha de corte 2025-01-01 ---
REM --- Guardará logs por cada script en la carpeta logs (asegurate que existe) ---

chcp 65001

SET FECHA_CORTE=2025-01-01
SET SCRAPER_DIR=C:\Users\Lenovo\Documents\github\seguimiento-de-noticias\scrapers

cd /d %SCRAPER_DIR%

REM -------- Analisis Digital --------
echo Ejecutando AnalisisDigital...
python analisisdigital.py %FECHA_CORTE% > logs\analisisdigital_%FECHA_CORTE%.log 2>&1

REM -------- APF Digital --------
echo Ejecutando APFDigital...
python apfdigital.py %FECHA_CORTE% > logs\apfdigital_%FECHA_CORTE%.log 2>&1

REM -------- El Diario --------
echo Ejecutando ElDiario...
python eldiario.py %FECHA_CORTE% > logs\eldiario_%FECHA_CORTE%.log 2>&1

REM -------- El Heraldo --------
echo Ejecutando ElHeraldo...
python elheraldo.py %FECHA_CORTE% > logs\elheraldo_%FECHA_CORTE%.log 2>&1

REM -------- El Once --------
echo Ejecutando ElOnce...
python elonce.py %FECHA_CORTE% > logs\elonce_%FECHA_CORTE%.log 2>&1

REM -------- Uno Digital --------
echo Ejecutando UnoDigital...
python unodigital.py %FECHA_CORTE% > logs\unodigital_%FECHA_CORTE%.log 2>&1

echo ============================
echo  ¡Scraping terminado!
echo ============================

pause
