# Usa a imagem oficial do jupyter/pyspark-notebook como base
FROM jupyter/pyspark-notebook:latest

# Define uma variável de ambiente com a URL de download do driver
ENV POSTGRES_JDBC_URL="https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"

# Define o diretório onde o driver será salvo
ENV SPARK_JARS_DIR="/opt/spark/jars/"

# Mude para o usuário root para ter permissão de instalar pacotes e criar pastas
USER root

# Baixa o driver JDBC do PostgreSQL
RUN mkdir -p ${SPARK_JARS_DIR} \
    && wget -P ${SPARK_JARS_DIR} ${POSTGRES_JDBC_URL}

# Instala dependências Python
RUN pip install psycopg2-binary

# <<< CORREÇÃO FINAL AQUI >>>
# Primeiro, cria o diretório de configuração. Depois, cria o arquivo com a configuração do driver.
RUN mkdir -p /opt/spark/conf \
    && echo "spark.driver.extraClassPath /opt/spark/jars/postgresql-42.7.3.jar" > /opt/spark/conf/spark-defaults.conf

# Define o diretório de trabalho
WORKDIR /home/jovyan/work

# Copia o conteúdo da pasta 'src' local
COPY ./src/ /home/jovyan/work/

# Dá permissão de execução para o script orquestrador
RUN chmod +x run_pipeline.sh

# Define o proprietário correto para os arquivos copiados
RUN chown -R jovyan:users /home/jovyan/work

# Volta para o usuário padrão 'jovyan'
USER jovyan

# Define o comando de inicialização do container
CMD ["./run_pipeline.sh"]