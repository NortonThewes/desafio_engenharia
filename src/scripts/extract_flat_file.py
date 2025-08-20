#!/usr/bin/env python
# coding: utf-8

# In[7]:


from pyspark.sql import SparkSession
import sys 

JAR_PATH = "/opt/spark/jars/postgresql-42.7.3.jar"
DB_URL = "jdbc:postgresql://db:5432/SiCooperativeDW"
DB_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver",
}

# Dicionário com as tabelas do DW
TABLES_TO_REGISTER = {
    "fato_movimentacao_cartao": "fato",
    "dim_associado": "dim_associado",
    "dim_cartao": "dim_cartao",
    "dim_conta": "dim_conta"
}

if len(sys.argv) < 2:
    print("Erro: O caminho do diretório de saída não foi fornecido.")
    print("Uso: spark-submit seu_script.py /caminho/de/saida/no/container")
    sys.exit(1) 

OUTPUT_PATH = sys.argv[1]


def get_spark_session():
    """
    Cria e retorna uma SparkSession configurada.
    """
    return (
        SparkSession.builder.appName("DW to Flat File Exporter - Spark SQL")
        .config("spark.jars", JAR_PATH)
        #.config("spark.driver.extraClassPath", JAR_PATH) 
        .getOrCreate()
    )


def main():
    """
    Função principal que executa o processo de exportação.
    """
    spark = get_spark_session()
    print("Spark Session criada com sucesso.")

    # Leitura das Tabelas e Registro das Views
    print("Iniciando a leitura das tabelas do Data Warehouse...")
    for db_table, view_name in TABLES_TO_REGISTER.items():
        print(f"Lendo '{db_table}' e registrando como view '{view_name}'...")
        df = spark.read.jdbc(url=DB_URL, table=db_table, properties=DB_PROPERTIES)
        df.createOrReplaceTempView(view_name)
    
    print("Views temporárias criadas com sucesso.")

    # Transformação com uma Única Query
    
    flatten_sql_query = """
        SELECT
            -- Colunas da dim_associado
            da.nome_associado,
            da.sobrenome_associado,
            da.idade_atual_associado as idade_associado,
            
            -- Colunas da fato
            f.valor_movimentacao    AS vlr_transacao_movimento,
            f.id_movimentacao_cartao  AS des_transacao_movimento, -- Usando o ID como descrição
            f.data_movimentacao,

            -- Colunas da dim_cartao
            dc.numero_cartao,
            dc.nome_impresso_cartao,
            dc.data_emissao_cartao as data_criacao_cartao,

            -- Colunas da dim_conta
            dcn.tipo_conta,
            dcn.data_criacao_conta
        FROM
            fato AS f
        INNER JOIN
            dim_associado AS da ON f.SK_Associado = da.SK_Associado
        INNER JOIN
            dim_cartao AS dc ON f.SK_Cartao = dc.SK_Cartao
        INNER JOIN
            dim_conta AS dcn ON f.SK_Conta = dcn.SK_Conta
    """

    print("Executando a query SQL para gerar a visão flat...")
    movimento_flat_df = spark.sql(flatten_sql_query)

    print("Amostra dos dados gerados:")
    movimento_flat_df.show(5, truncate=False)

    # --- Escrita do Arquivo de Saída
    print(f"Iniciando a escrita do arquivo CSV em: {OUTPUT_PATH}")
    movimento_flat_df.coalesce(1).write.format("csv").option(
        "header", "true"
    ).option(
        "sep", ";"
    ).mode(
        "overwrite"
    ).save(OUTPUT_PATH)
    print("Arquivo flat gerado com sucesso!")
    
    spark.stop()


if __name__ == "__main__":
    main()


# In[ ]:




