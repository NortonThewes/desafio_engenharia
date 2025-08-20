#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
import re
import psycopg2 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_date, xxhash64, abs, max
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, LongType, IntegerType

# Define o caminho do JAR como uma constante para ser reutilizado
JAR_PATH = "/opt/spark/jars/postgresql-42.7.3.jar"

def get_spark_session():
    """
    Cria e retorna uma SparkSession com a configuração JDBC para PostgreSQL.
    A configuração do driver extra foi removida pois não é mais necessária.
    """
    return (
        SparkSession.builder.appName("ETL Refined Layer - Dimension Creation with Spark SQL")
        .config("spark.jars", JAR_PATH)
        #.config("spark.driver.extraClassPath", JAR_PATH) 
        .getOrCreate()
    )

def execute_postgres_update(db_details: dict, temp_table_name: str):
    """
    Conecta-se ao PostgreSQL usando psycopg2 e executa um UPDATE
    para desativar registros antigos. Esta abordagem é mais simples e robusta
    do que usar Py4J para comandos DML.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=db_details["dbname"],
            user=db_details["user"],
            password=db_details["password"],
            host=db_details["host"],
            port=db_details["port"]
        )
        cur = conn.cursor()
        
        # Query de UPDATE
        update_query = f"""
            UPDATE dim_associado
            SET fim_vigencia_registro_associado = current_timestamp,
                registro_ativo_associado = false
            WHERE sk_associado IN (SELECT sk_associado FROM {temp_table_name});
        """
        
        print("Executando o UPDATE no banco de dados...")
        cur.execute(update_query)
        rows_affected = cur.rowcount
        print(f"Comando executado com sucesso. Linhas afetadas: {rows_affected}")
        
        # Efetiva a transação
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ocorreu um erro ao executar o UPDATE via psycopg2: {error}")
        if conn is not None:
            conn.rollback() 
        raise error
    finally:
        if conn is not None:
            conn.close()

def process_dim_associado_sql(spark: SparkSession):
    """
    Lê a tabela trusted_associado e atualiza a dimensão dim_associado
    aplicando a lógica de SCD Tipo 2 (INSERT/UPDATE) usando Spark SQL.
    """

    # Configuração de Conexão com o DW
    db_url = "jdbc:postgresql://db:5432/SiCooperativeDW"
    db_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver",
    }
    
    # Leitura dos dados
    print("Lendo a tabela trusted_associado...")
    df_trusted_associado = spark.read.jdbc(url=db_url, table="trusted_associado", properties=db_properties)
    
    try:
        print("Lendo a dimensão dim_associado existente...")
        df_dim_associado = spark.read.jdbc(url=db_url, table="dim_associado", properties=db_properties)
    except Exception as e:
        print(f"Tabela dim_associado não encontrada. A primeira carga será feita como 'novas_inclusoes'.")
        dim_schema = StructType([
            StructField("sk_associado", LongType(), False), StructField("id_associado", StringType(), True),
            StructField("nome_associado", StringType(), True), StructField("sobrenome_associado", StringType(), True), 
            StructField("data_nascimento_associado", DateType(), True),
            StructField("idade_atual_associado", IntegerType(), True), StructField("estado_civil_associado", StringType(), True), 
            StructField("escolaridade_associado", StringType(), True), StructField("inicio_vigencia_registro_associado", DateType(), True),
            StructField("fim_vigencia_registro_associado", DateType(), True), StructField("registro_ativo_associado", BooleanType(), True)
        ])
        df_dim_associado = spark.createDataFrame([], schema=dim_schema)
        
    # Criação das views temporarias
    df_trusted_associado.createOrReplaceTempView("source_trusted_associado")
    df_dim_associado.createOrReplaceTempView("target_dim_associado")
    
    # CTE para identificar os registros a serem inseridos (novos e alterados)
    records_to_insert_sql = """
        WITH
        novas_inclusoes AS (
            SELECT 
            CAST(abs(xxhash64(id_associado, current_timestamp())) % 10000000000 AS BIGINT) AS sk_associado, 
            id_associado, 
            nome as nome_associado, 
            sobrenome as sobrenome_associado, 
            data_nascimento as data_nascimento_associado, 
            idade_atual_associado, 
            estado_civil as estado_civil_associado, 
            escolaridade as escolaridade_associado, 
            current_timestamp() AS inicio_vigencia_registro_associado, 
            CAST(NULL AS TIMESTAMP) AS fim_vigencia_registro_associado,
            true AS registro_ativo_associado
            FROM source_trusted_associado ta
            WHERE NOT EXISTS (
                                SELECT 1
                                FROM target_dim_associado da
                                WHERE
                                    da.id_associado = ta.id_associado
                                    AND da.estado_civil_associado = ta.estado_civil
                                    AND da.escolaridade_associado = ta.escolaridade
                                    AND da.idade_atual_associado = ta.idade_atual_associado
            ))
            SELECT * FROM novas_inclusoes 
        """
    
    # CTE para identificar os registros antigos que precisam ser desativados (UPDATE)
    records_to_update_sql = """
        WITH atualizados AS (
            SELECT 
                da.sk_associado
            FROM source_trusted_associado ta 
            INNER JOIN target_dim_associado da ON ta.id_associado = da.id_associado AND da.registro_ativo_associado = TRUE
             WHERE NOT EXISTS (
                                SELECT 1
                                FROM target_dim_associado da
                                WHERE
                                    da.id_associado = ta.id_associado
                                    AND da.estado_civil_associado = ta.estado_civil
                                    AND da.escolaridade_associado = ta.escolaridade
                                    AND da.idade_atual_associado = ta.idade_atual_associado
        ))
        SELECT sk_associado FROM atualizados
    """

    print("Calculando registros para INSERT e UPDATE...")
    df_to_insert = spark.sql(records_to_insert_sql)
    df_to_update = spark.sql(records_to_update_sql)

    print("Criando cache para otimização...")
    df_to_insert.cache()
    df_to_update.cache()

    if not df_to_update.rdd.isEmpty():
        temp_update_table = "staging_updates_associado"
        print(f"Encontrados {df_to_update.count()} registros para desativar (UPDATE)...")
        print(f"Escrevendo chaves em uma tabela temporária: {temp_update_table}")
        df_to_update.select("sk_associado").write.jdbc(
            url=db_url, table=temp_update_table, mode="overwrite", properties=db_properties
        )
        
        # Extrai detalhes da URL para a conexão
        match = re.search(r"postgresql://(.*?):(.*?)/(.*)", db_url)
        host, port, dbname = match.groups()
        
        db_details_for_update = {
            "user": db_properties["user"],
            "password": db_properties["password"],
            "host": host,
            "port": port,
            "dbname": dbname
        }
        
        execute_postgres_update(db_details_for_update, temp_update_table)
        print("UPDATEs concluídos.")
    else:
        print("Nenhum registro para atualizar.")
        
    # --- ETAPA DE INSERT ---
    if df_to_insert.count() > 0:
        print(f"Encontrados {df_to_insert.count()} novos registros para inserir (INSERT)...")
        df_to_insert.write.jdbc(
            url=db_url, table="dim_associado", mode="append", properties=db_properties
        )
        print("INSERTs concluídos.")
    else:
        print("Nenhum registro novo para inserir.")

    # Limpa o cache
    df_to_insert.unpersist()
    df_to_update.unpersist()

    print("Processamento da dimensão Associado concluído com sucesso.")


if __name__ == "__main__":
    spark_session = get_spark_session()
    # A chamada para addJar foi removida daqui pois não é mais necessária
    process_dim_associado_sql(spark_session)
    spark_session.stop()


# In[ ]:




