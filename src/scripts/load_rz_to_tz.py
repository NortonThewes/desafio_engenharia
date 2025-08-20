#!/usr/bin/env python
# coding: utf-8

# In[4]:


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, trim, coalesce, datediff, current_date, upper, floor, lit, split, when, size, array_join, slice

def get_spark_session():
    """Cria e retorna uma SparkSession com a configuração JDBC para PostgreSQL."""
    JAR_PATH = "/opt/spark/jars/postgresql-42.7.3.jar" 
    return (
        SparkSession.builder.appName("Load to Trusted Layer")
        .config("spark.jars", JAR_PATH)
        #.config("spark.driver.extraClassPath", JAR_PATH) 
        .getOrCreate()
    )

def transform_to_trusted(spark: SparkSession):
    """
    Lê os dados brutos da camada Raw (Bronze), aplica transformações de
    qualidade usando Spark SQL e escreve na camada Trusted (Silver).
    """
    
    # Configuração de Conexão com o DW
    db_url = "jdbc:postgresql://db:5432/SiCooperativeDW"
    db_properties = {
        "user": "user", 
        "password": "password", 
        "driver": "org.postgresql.Driver",
    }
    
    # Leitura e Transformação das Tabelas
    print("Processando a tabela de Associado...")
    df_raw_associado = spark.read.jdbc(url=db_url, table="raw_associado", properties=db_properties)

    # Obter o Sobrenome
    nome_completo = upper(coalesce(trim(df_raw_associado["nome"]), lit("")))
    nome_parts = split(nome_completo, " ")
    
    df_trusted_associado = df_raw_associado.select(
        "id_associado",
        nome_parts.getItem(0).alias("nome"),
        when(size(nome_parts) > 1, array_join(slice(nome_parts, 2, size(nome_parts)), " ")).otherwise(lit(None)).alias("sobrenome"),
        "data_nascimento",
        upper(coalesce(trim(df_raw_associado["estado_civil"]), lit(""))).alias("estado_civil"),
        upper(coalesce(trim(df_raw_associado["escolaridade"]), lit(""))).alias("escolaridade"),
        floor(datediff(current_date(), df_raw_associado["data_nascimento"]) / 365.25).alias("idade_atual_associado")
    )
    df_trusted_associado.write.jdbc(url=db_url, table="trusted_associado", mode="overwrite", properties=db_properties)
    print("Tabela trusted_associado criada com sucesso.")
    
    print("Processando a tabela de Conta...")
    df_raw_conta = spark.read.jdbc(url=db_url, table="raw_conta", properties=db_properties)
    df_trusted_conta = df_raw_conta.select(
        "id_conta",
        "id_associado",
        upper(coalesce(trim(df_raw_conta["tipo_conta"]), lit(""))).alias("tipo_conta"),
        upper(coalesce(trim(df_raw_conta["agencia"]), lit(""))).alias("agencia"),
        upper(coalesce(trim(df_raw_conta["situacao_conta"]), lit(""))).alias("situacao_conta"),
        "data_criacao_conta"
    )
    df_trusted_conta.write.jdbc(url=db_url, table="trusted_conta", mode="overwrite", properties=db_properties)
    print("Tabela trusted_conta criada com sucesso.")
    
    print("Processando a tabela de Cartão...")
    df_raw_cartao = spark.read.jdbc(url=db_url, table="raw_cartao", properties=db_properties)
    df_trusted_cartao = df_raw_cartao.select(
        "id_cartao",
        "id_conta",
        "numero_cartao",
        upper(coalesce(trim(df_raw_cartao["nome_impresso_cartao"]), lit(""))).alias("nome_impresso_cartao"),
        upper(coalesce(trim(df_raw_cartao["bandeira_cartao"]), lit(""))).alias("bandeira_cartao"),
        upper(coalesce(trim(df_raw_cartao["tipo_cartao"]), lit(""))).alias("tipo_cartao"),
        "data_emissao"
    )
    df_trusted_cartao.write.jdbc(url=db_url, table="trusted_cartao", mode="overwrite", properties=db_properties)
    print("Tabela trusted_cartao criada com sucesso.")
    
    print("Processando a tabela de Movimentação de Cartão...")
    df_raw_mov = spark.read.jdbc(url=db_url, table="raw_movimentacao_cartao", properties=db_properties)
    df_trusted_mov = df_raw_mov.select(
        "id_movimentacao_cartao",
        "id_cartao",
        upper(coalesce(trim(df_raw_mov["descricao_movimentacao"]), lit(""))).alias("descricao_movimentacao"),
        "valor_movimentacao",
        "data_movimentacao"
    )
    df_trusted_mov.write.jdbc(url=db_url, table="trusted_movimentacao_cartao", mode="overwrite", properties=db_properties)
    print("Tabela trusted_movimentacao_cartao criada com sucesso.")

    print("Processamento camada Trusted concluída com sucesso.")

if __name__ == "__main__":
    spark_session = get_spark_session()
    transform_to_trusted(spark_session)
    spark_session.stop()


# In[ ]:




