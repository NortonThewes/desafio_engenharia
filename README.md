Projeto Engenharia de Dados: Análise de Movimentações Financeiras

1. Introdução
Este projeto foi desenvolvido como parte de um desafio técnico de engenharia de dados. O objetivo é criar uma solução completa de ETL (Extract, Transform, Load) para a empresa fictícia SiCooperative LTDA, que enfrenta dificuldades na agregação de informações para tomada de decisões e na criação de modelos preditivos.
A solução implementa um fluxo de dados desde a ingestão de dados transacionais de cartões e clientes até a criação de um Data Warehouse com modelagem dimensional, permitindo análises mais rápidas e precisas.

2. O Problema de Negócio
A SiCooperative LTDA precisa de uma forma eficiente para consolidar dados de diferentes fontes para melhorar a velocidade e a assertividade de suas decisões. Relatórios manuais consomem muito tempo e dificultam a correlação de informações. Além disso, uma nova equipe de Data Science necessita de dados estruturados para desenvolver modelos preditivos que ofereçam soluções financeiras personalizadas aos associados.
O desafio foca em modelar uma estrutura mais funcional e ágil, que incluem informações de valor, data, e relacionamentos com as tabelas de cartão, conta corrente e associado.

3. Arquitetura da Solução
Para resolver o problema, foi implementado um pipeline de dados em três etapas, seguindo o conceito de um Data Lakehouse.

Para simular o ambiente, o projeto utiliza dois bancos de dados distintos no PostgreSQL:
SiCooperativeDB: Simula o banco de dados transacional de origem.
SiCooperativeDW: Representa a estrutura do Data Warehouse.

Para simplificar a organização das camadas de dados dentro do DW, em vez de usar schemas individuais, a separação é feita através da nomenclatura das tabelas. Por exemplo:
Camada Raw: Ingestão dos dados diretamente das tabelas de origem, preservando a forma e a estrutura originais. As tabelas aqui recebem o prefixo raw_, como raw_cartao.
Camada Trusted: Dados tratados, limpos e enriquecidos. Nessa etapa, a idade do associado é calculada. As tabelas aqui recebem o prefixo trusted_, como trusted_cartao.
Camada Consolidada (dim/fato): Os dados são modelados em um Star Schema para facilitar análises e o desenvolvimento de relatórios.

PIPELINE: O processo de carga utiliza o conceito ELT, onde os dados são extraídos do DB de origem (SiCooperativeDB) e gravados em uma primeira camada de dados brutos no DB de destino (SiCooperativeDW), passando por todas as etapas do modelo "medalhão" até ser disponibilizado para consumo. Todo o pipeline está construído encadeando 5 diferentes arquivos:
ingestion_db_to_dw.py, load_rz_to_tz.py, load_dim_associados.py, load_fato_movimentacoes.py, extract_flat_file.py
A ideia aqui foi quebrar o processo em pequenas partes, focando individualmente na entrega que cada etapa do pipeline deveria fazer. assim, em um processo de orquestração real, a gestão de dependências e reprocessamento de apenas parte do processo (se necessário), fica mais flexível.
OBS.: Estes arquivos também existem como jupyter notebooks dentro do container, caso seja necessários executá-los de forma individual. Existe um arquivo .png desta arquitetura na pasta 'documentacoes' aqui do projeto.

3.1. Modelagem Dimensional
A modelagem de dados foi projetada com o objetivo de otimizar consultas analíticas. A estrutura escolhida foi o Star Schema, composto por:

Tabela Fato:
FATO_MOVIMENTACAO_CARTAO: Esta é a tabela central da nossa modelagem, contendo as métricas de negócio essenciais como valor_movimentacao. Ela se conecta às tabelas de dimensão por meio de surrogate keys. Para suportar cargas incrementais, a coluna data_hora_atualizacao_registro registra a timestamp de inserção de cada novo registro. O processo de ingestão inclui uma etapa de validação para verificar a unicidade dos dados e evitar duplicidades.

Tabelas de Dimensão:
DIM_DATA: Representa a data da movimentação.
DIM_ASSOCIADO: Contém informações do cliente, campos como idade, escolaridade e estado civil são historizados para capturar mudanças ao longo do tempo.
DIM_CARTAO: Dados sobre o cartão.
DIM_CONTA: Informações da conta corrente.

**IMPORTANTE: O processo implementado no projeto funciona de ponta a ponta, portando se for realizado algum update na tabela associado da base SiCooperativeDB em algum dos campos historizados, ao executar o pipeline o dado será atualizado conforme descrito acima. Como o objetivo era focar nos desafios propostos e o prazo de entrega precisava ser considerado, o pipeline não incluí a carga da camada trusted para as dins das tabelas dim_cartao e dim_conta, já que estas seriam cargas simples, sem lógicas complexas a serem tratadas, porém seus scripts estão na documentação.

OBS.: Existe um arquivo .png desta modelagem na pasta 'documentacoes' aqui do projeto.

4. Tecnologias Utilizadas
Linguagem de Programação: Python
Banco de Dados: PostgreSQL
Framework de Processamento Distribuído: Apache Spark
Orquestração/Containerização: Docker

5. Estrutura do Projeto
O repositório está organizado da seguinte forma:

data/db: Contém os arquivos diversos da estrutura postgres.
documentacoes: Arquivos de imagem da modelagem e arquitetura do processo, também contém os arquivos com os scripts utilzados para criação das tabelas nos DB e suas cargas de dados.
src/data/output: Caminho parametrizado para gravação do arquivo flat gerado pelo processo.
src/notebooks: Arquivos contendo os notebooks funcionais do projeto.
src/scripts: Contém os arquivos .py que são utilizados durante a execução do pipeline.
src/run_pipeline.sh: Arquivo orquestrador da automação do pipeline.

6. Como Executar o Projeto
Para rodar o projeto localmente, siga os passos abaixo:

Clone este repositório:
git clone https://github.com/NortonThewes/desafio_engenharia

Para subir os containers e ter acesso a todo o ambiente (postgres + notebooks):
docker-compose up -d

Para subir os containers e startar automaticamente a execução do pipeline:
docker-compose run --rm spark-notebook /home/jovyan/work/run_pipeline.sh

Ao final da execução, o arquivo flat .csv será gerado no diretório src/data/output/.

7. Desafios Encontrados e Oportunidades de evolução
Desafios: O maior desafio para mim foi trabalhar com Spark e Docker, ferramentas que não uso no meu dia a dia. Como o prazo estipulado e o fato de que eu só podia me dedicar no meu tempo livre, acabei focando em resolver os problemas principais. Isso teve um custo: a organização e a padronização do código não ficaram como eu entregaria em um ambiente produtivo.
Sobre a abordagem técnica, decidi misturar as coisas. Usei Python para a ingestão de dados, mas preferi o Spark SQL para a manipulação, já que é uma linguagem com a qual me sinto mais à vontade e que tem mais a ver com as demandas que enfrentamos no trabalho.
Outro ponto que me fez refletir foi a orquestração do pipeline. Por não estar muito familiarizado com o ambiente (docker, spark) no início, não tinha certeza de como faria isso, então preferi construir o projeto etapa por etapa, sem dependências. Em um ambiente de produção, eu já faria isso de forma diferente, pensando em uma estrutura mais organizada e interligada desde o começo.


Oportunidades de evolução:

Orquestração: Implementar uma ferramenta de orquestração como Apache Airflow.
Monitoramento: Adicionar logging e alertas para rastrear o status e possíveis falhas do processo.
Qualidade de Dados: Aumentar a cobertura de testes de qualidade para garantir a integridade dos dados em cada etapa, aqui acabei abordando apenas na etapa da carga da tbela fato.
Streaming: Migrar a ingestão para um modelo de processamento em tempo real (streaming), utilizando tecnologias como Apache Kafka e Spark Streaming para lidar com dados de forma contínua.




Informações auxiliares que podem ajudar no acesso ao ambiente:

Acesso ao Jupyter Notebook:
http://localhost:8888

PostgreSQL URL de conexão: 
jdbc:postgresql://db:5432/

Database: SiCooperativeDB
user: user
senha: password