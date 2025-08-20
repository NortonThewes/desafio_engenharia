#!/bin/bash
set -e

# Define o caminho do JAR para ser reutilizado
JAR_PATH="/opt/spark/jars/postgresql-42.7.3.jar"

# Define o caminho de saída para o arquivo flat
FLAT_FILE_OUTPUT_PATH="/home/jovyan/work/data/output"

echo "================================================="
echo ">>> INICIANDO PIPELINE DE ETL COMPLETO <<<"
echo "================================================="

echo ">>> [ETAPA 1/5] Executando: 1_ingestion_db_to_dw.py"
spark-submit \
    --driver-class-path ${JAR_PATH} \
    /home/jovyan/work/scripts/ingestion_db_to_dw.py

echo ">>> [ETAPA 2/5] Executando: 2_load_rz_to_tz.py"
spark-submit \
    --driver-class-path ${JAR_PATH} \
    /home/jovyan/work/scripts/load_rz_to_tz.py

echo ">>> [ETAPA 3/5] Executando: 3_load_dim_associados.py"
spark-submit \
    --driver-class-path ${JAR_PATH} \
    /home/jovyan/work/scripts/load_dim_associados.py

echo ">>> [ETAPA 4/5] Executando: load_fato_movimentacoes.py"
spark-submit \
    --driver-class-path ${JAR_PATH} \
    /home/jovyan/work/scripts/load_fato_movimentacoes.py

echo ">>> [ETAPA 5/5] Executando: extract_flat_file.py"
spark-submit \
    --driver-class-path ${JAR_PATH} \
    /home/jovyan/work/scripts/extract_flat_file.py ${FLAT_FILE_OUTPUT_PATH}

echo "================================================="
echo ">>> PIPELINE DE ETL CONCLUÍDO COM SUCESSO! <<<"
echo "================================================="
