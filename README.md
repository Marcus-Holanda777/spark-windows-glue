# Data Pipeline: SQL Server to Apache Iceberg (Local Windows Cluster)

Este projeto demonstra a implementação de um pipeline de dados robusto e distribuído, utilizando um cluster local **Apache Spark 3.5.7** no Windows para extração de dados de um **SQL Server** e carga em tabelas **Apache Iceberg** na **AWS (S3/Glue)**.

![Estrutura do projeto](img/projeto_spark.png)

## 🏗️ Arquitetura e Especificações

* **Ambiente de Execução:** Windows 10/11 (Standalone Cluster: 1 Master + 2 Workers).
* **Engine de Processamento:** Apache Spark 3.5.7.
* **Linguagem:** Python 3.11 (Obrigatório para evitar instabilidade no Spark 3.5).
* **Orquestração de Metadados:** AWS Glue Data Catalog.
* **Storage Final:** AWS S3 (Formato Apache Iceberg).
* **Monitoramento:** Spark History Server com logs de eventos locais.

---

## 🛠️ Pré-requisitos no Windows

Para reproduzir este ambiente, siga estas etapas cruciais para o funcionamento no Windows:

1. **Hadoop Binaries:** Baixe o `winutils.exe` e `hadoop.dll` compatíveis com o Hadoop 3.3 e coloque-os na pasta `bin` dentro do seu `HADOOP_HOME`.
2. **Autenticação SQL Server:** Para utilizar `integratedSecurity=true` (autenticação Windows), você deve baixar o driver JDBC da Microsoft e copiar a biblioteca `mssql-jdbc_auth.dll` para a pasta `C:\Windows\System32`.
3. **Variáveis de Ambiente:**
* `SPARK_HOME`: Caminho da instalação do Spark.
* `HADOOP_HOME`: Caminho da pasta que contém os binários do Hadoop.
* `PYSPARK_PYTHON` & `PYSPARK_DRIVER_PYTHON`: Devem apontar para o executável do Python 3.11.



---

## ⚙️ Configuração do Spark (`spark-defaults.conf`)

O arquivo abaixo contém as otimizações de memória, paralelismo e integração com o ecossistema AWS/Iceberg:

```properties
# Performance & Serializer
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max  512m
spark.network.timeout            800s
spark.sql.adaptive.enabled       true

# Cluster Resources (Standalone)
spark.executor.instances         2
spark.executor.cores             4
spark.executor.memory            5g
spark.driver.memory              2g

# Iceberg & AWS Glue Catalog
spark.sql.defaultCatalog         dev
spark.sql.catalog.dev            org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.dev.type       glue
spark.sql.catalog.dev.io-impl    org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.dev.warehouse  s3://seu-bucket-data-warehouse/tables/
spark.sql.extensions             org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# Spark History Server & Logs
spark.eventLog.enabled           true
spark.eventLog.dir               file:///C:/spark/spark-events
spark.history.ui.port            18080
spark.history.fs.logDirectory    file:///C:/spark/spark-events

```

---

## 🚀 Fluxo de Processamento (Python)

O pipeline segue um padrão de engenharia focado em performance e escalabilidade:

### 1. Extração Paralela (JDBC)

O script utiliza `partitionColumn`, `lowerBound` e `upperBound` para dividir a carga de trabalho entre os workers. Isso permite que o Spark realize múltiplas conexões simultâneas ao SQL Server, acelerando drasticamente o download de tabelas grandes.

### 2. Staging Local em Parquet

Antes de enviar os dados para a nuvem, os dados são gravados em um storage local em formato **Parquet** com compressão **ZSTD**. Isso garante que, em caso de falha na rede durante o upload para o S3, os dados já foram extraídos da origem com sucesso.

### 3. Escrita Iceberg & Otimizações

A carga no Iceberg utiliza as seguintes propriedades:

* **Merge-on-Read:** Otimiza o desempenho de escrita e atualizações.
* **Compression ZSTD:** Alta taxa de compressão com baixo overhead de CPU.
* **File Size (128MB):** Configuração `write.target-file-size-bytes` para evitar o problema de "small files" no S3.

### 4. Manutenção de Tabela

Após a carga, o script executa automaticamente rotinas de manutenção para garantir a saúde do Lakehouse:

* `rewrite_data_files`: Compactação de arquivos pequenos.
* `expire_snapshots`: Limpeza de versões antigas dos dados.
* `remove_orphan_files`: Remoção de arquivos de dados que não estão mais referenciados.

---

## 📦 Bibliotecas Necessárias (JARs)

Certifique-se de que os seguintes pacotes estejam disponíveis (via `spark.jars.packages` ou na pasta `jars` do Spark):

* `iceberg-spark-runtime-3.5_2.12`
* `iceberg-aws-bundle`
* `hadoop-aws`
* `mssql-jdbc`

---

## 📈 Monitoramento

Para visualizar o progresso dos Jobs e analisar o histórico de execução:

1. Inicie o master e os workers locais.
2. Acesse a Spark UI em `http://localhost:8080`.
3. Acesse o History Server em `http://localhost:18080`.

---

**Nota:** Este projeto foi desenvolvido para ambientes de estudo e testes de carga local simulando pipelines produtivos de alta performance.
