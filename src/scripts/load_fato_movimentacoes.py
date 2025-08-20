#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession

# Define o caminho do JAR como uma constante para ser reutilizado
JAR_PATH = "/opt/spark/jars/postgresql-42.7.3.jar"

# Configuração de Conexão com o Data Warehouse
DB_URL = "jdbc:postgresql://db:5432/SiCooperativeDW"
DB_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver",
}

# Nomes das tabelas que serão lidas
TABLES_TO_READ = {
    # Tabelas Trusted
    "trusted_movimentacao_cartao": "source_trusted_movimentacao",
    "trusted_cartao": "source_trusted_cartao",
    "trusted_conta": "source_trusted_conta",
    "trusted_associado": "source_trusted_associado",
    # Tabelas de Dimensão 
    "dim_cartao": "target_dim_cartao",
    "dim_conta": "target_dim_conta",
    "dim_associado": "target_dim_associado",
    "dim_data": "target_dim_data"
}

# Tabela de destino
FATO_MOVIMENTACAO_TABLE = "fato_movimentacao_cartao"


def get_spark_session():
    """
    Cria e retorna uma SparkSession com a configuração JDBC para PostgreSQL.
    """
    return (
        SparkSession.builder.appName("ETL Fato Movimentacao - Full Load with Spark SQL")
        .config("spark.jars", JAR_PATH)
        #.config("spark.driver.extraClassPath", JAR_PATH) 
        .getOrCreate()
    )

def run_data_quality_tests(spark, fato_df):
    """
    Executa testes de qualidade de dados no DataFrame da fato antes da carga.
    Levanta uma exceção se algum teste falhar.
    """
    print("\n--- INICIANDO TESTES DE QUALIDADE DE DADOS ---")
    
    # Contagem de Registros ---
    print("\nTeste 1: Verificando se a contagem de registros da fato bate com a origem...")
    
    # Contagem de registros da tabela trusted
    source_count = spark.table("source_trusted_movimentacao").count()
    # Contagem de registros do DataFrame da fato que foi gerada
    target_count = fato_df.count()

    # Se as contagens não baterem, o teste falha.
    if source_count != target_count:
        error_message = (
            f"FALHA NO TESTE DE CONTAGEM! "
            f"Origem ('trusted_movimentacao_cartao') tem {source_count} registros. "
            f"Destino ('fato_movimentacao_cartao') gerou {target_count} registros."
        )
        print(error_message)
        raise ValueError(error_message)
    else:
        print(f"SUCESSO! Contagem de registros validada: {source_count} registros.")
        
    # Verificação de Chave Primária Única
    print("\nTeste 2: Verificando se não há duplicidade na chave primária (id_movimentacao_cartao)...")
    
    # Conta o total de registros
    total_records = target_count
    # Conta o total de registros distintos pela chave primária
    distinct_pk_count = fato_df.select("id_movimentacao_cartao").distinct().count()

    # Se a contagem total for diferente da contagem de chaves únicas, há duplicatas.
    if total_records != distinct_pk_count:
        duplicates = total_records - distinct_pk_count
        error_message = (
            f"FALHA NO TESTE DE DUPLICIDADE! "
            f"A tabela fato gerada tem {duplicates} chave(s) primária(s) duplicada(s)."
        )
        print(error_message)
        raise ValueError(error_message)
    else:
        print(f"SUCESSO! Nenhuma chave primária duplicada encontrada.")
    
    print("\n--- TODOS OS TESTES DE QUALIDADE DE DADOS PASSARAM COM SUCESSO! ---\n")

def main():
    """
    Função principal que executa o processo de ETL.
    """
    spark = get_spark_session()
    print("Spark Session criada com sucesso.")

    # Carrega todas as tabelas necessárias e as registra como views temporárias
    print("Iniciando a leitura das tabelas e criação das views temporárias...")
    for table_name, view_name in TABLES_TO_READ.items():
        print(f"Lendo tabela '{table_name}' e registrando como view '{view_name}'...")
        df = spark.read.jdbc(url=DB_URL, table=table_name, properties=DB_PROPERTIES)
        df.createOrReplaceTempView(view_name)
    
    print("Todas as tabelas foram carregadas como views temporárias.")
    
    # Query lógica da contrução tabela fato
    fato_movimentacao_sql = """
        WITH movimentacoes_enriquecidas AS (
            SELECT 
                tmc.id_movimentacao_cartao,
                tcr.id_cartao,
                tcn.id_conta,
                tas.id_associado,
                tmc.valor_movimentacao,
                tmc.data_movimentacao 
            FROM 
                source_trusted_movimentacao AS tmc 
            INNER JOIN 
                source_trusted_cartao AS tcr ON tmc.id_cartao = tcr.id_cartao 
            INNER JOIN 
                source_trusted_conta AS tcn ON tcr.id_conta = tcn.id_conta 
            INNER JOIN 
                source_trusted_associado AS tas ON tcn.id_associado = tas.id_associado
        )
        SELECT 
            men.id_movimentacao_cartao,
            das.sk_associado,
            dcr.sk_cartao,
            dco.sk_conta,
            ddt.sk_data,
            men.valor_movimentacao,
            men.data_movimentacao,
            current_timestamp as data_hora_atualizacao_registro
        FROM 
            movimentacoes_enriquecidas AS men
        LEFT JOIN
            target_dim_cartao AS dcr ON men.id_cartao = dcr.id_cartao
        LEFT JOIN
            target_dim_conta AS dco ON men.id_conta = dco.id_conta
        LEFT JOIN
            target_dim_associado AS das ON men.id_associado = das.id_associado
                                AND men.data_movimentacao >= das.inicio_vigencia_registro_associado
                                AND (men.data_movimentacao <= das.fim_vigencia_registro_associado OR das.fim_vigencia_registro_associado IS NULL)
        LEFT JOIN
            target_dim_data AS ddt ON CAST(men.data_movimentacao AS DATE) = ddt.`data`
    """
    
    print("Executando a query de transformação com Spark SQL...")
    fato_df = spark.sql(fato_movimentacao_sql)

    # Antes de consolidar os dados, executa os testes.
    run_data_quality_tests(spark, fato_df)

    # Escrevendo o DataFrame final na tabela fato, sobrescrevendo os dados existentes.
    print(f"Iniciando a carga full na tabela '{FATO_MOVIMENTACAO_TABLE}'...")
    
    fato_df.write.jdbc(
        url=DB_URL,
        table=FATO_MOVIMENTACAO_TABLE,
        mode="overwrite", 
        properties=DB_PROPERTIES
    )
    
    print(f"Carga de {fato_df.count()} registros na tabela '{FATO_MOVIMENTACAO_TABLE}' concluída com sucesso!")

    spark.stop()


if __name__ == "__main__":
    main()


# In[ ]:




