# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

# Importando bibliotecas para manipulação de dados com PySpark
from pyspark.sql.functions import (
    regexp_extract, # Função para extrair uma substring usando expressão regular
    col,            # Função para referenciar uma coluna para operações
    date_format,    # Função para formatar uma coluna de data para um formato específico
    to_date,        # Função para converter uma string para o tipo de dado de data
    expr,           # Função para usar expressões SQL diretamente no código Spark
    avg,            # Função para calcular a média de uma coluna
    max,            # Função para calcular o valor máximo de uma coluna
    min,            # Função para calcular o valor mínimo de uma coluna
    sum,            # Função para calcular a soma dos valores de uma coluna
    when            # Função para aplicar condições (semelhante ao IF)
)

# Importando biblioteca para definição de tipos para schema do DataFrame
from pyspark.sql.types import IntegerType  

# Importando bibliotecas para operações Delta Tables (transações ACID no Spark)
from delta.tables import *

# Importando biblioteca para manipulação de datas
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração do Ambiente

# COMMAND ----------

# Inicializando a SparkSession
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

# Parâmetros do Blob Storage
# Nome da conta de armazenamento no Azure
storage_account_name = "cloudcasefirst"
# Nome do contêiner onde os dados estão armazenados
container_name = "accesslog"
# Caminho WABS (Windows Azure Storage Blob Service)
blob_storage_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
#Token SAS (Shared Access Signature) temporário para autenticação no Blob Storage
sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-04-10T21:15:56Z&st=2025-01-23T13:15:56Z&spr=https&sig=uKPFUqwDYzqfz4BGe2H7Cn1nq4mLmIna7OcCUKYK%2F0s%3D"

# Configuração do Blob Storage com SAS
# Associando o token SAS ao contêiner para permitir o acesso aos dados
spark.conf.set(f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net", sas_token)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Variáveis Paths**

# COMMAND ----------

# Variáveis de Ambiente
full_load = "full_load/"
access_log = "access_log.txt"
trigger = "trigger/trigger"
access_log_delta = "delta_table_accessLog"
log_delta = "log_delta"

# Definição de Paths (Caminhos de Acesso)
file_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{access_log}"
log_delta_path = f"{blob_storage_path}{log_delta}"
full_load_path = f"{blob_storage_path}{full_load}"
trigger_path = f"{blob_storage_path}{trigger}"
delta_table_acessLog_path = f"{blob_storage_path}{access_log_delta}"

# Lista Global para Armazenar Logs Processados
processing_log = []

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções

# COMMAND ----------

# Registrar Logs: Adiciona eventos à lista de rastreamento da pipeline.
def log_event(step, status, message="", record_count=None):
    """
    Registra um evento no log de processamento.

    Parâmetros:
    - step (str): Nome ou descrição da etapa do pipeline sendo registrada.
    - status (str): Status da execução na etapa (por exemplo, "Iniciado", "Sucesso", "Erro").
    - message (str, opcional): Mensagem adicional com detalhes do evento. Valor padrão é uma string vazia.
    - record_count (int, opcional): Número de registros processados na etapa. Valor padrão é None.

    Retorna:
    A função adiciona o evento registrado à lista global 'processing_log'.
    """
    event = Row(
        Timestamp=str(datetime.now()),  # Data e horário do evento
        PipelineStep=step,  # Etapa da pipeline associada ao evento
        Status=status,  # Status da execução na etapa
        Message=message,  # Mensagem adicional informativa (se fornecida)
        RecordCount=record_count  # Quantidade de registros processados (se fornecida)
    )
    processing_log.append(event)  # Adiciona o evento registrado à lista global de logs

# Ler o Arquivo de Log: Lê arquivo de log e retorna um DataFrame.
def read_logs(file_path):
    """
    Lê um arquivo de log e retorna um DataFrame com seu conteúdo.

    Parâmetros:
    - file_path (str): Caminho completo do arquivo de log a ser lido.

    Retorna:
    - logs_df (DataFrame): DataFrame contendo o conteúdo do arquivo de log.
    """
    step_name = "Read Access Logs"  # Nome da etapa do pipeline para identificar no log de execução
    try:
        log_event(step_name, "Inicio", f"Lendo arquivo de log de {file_path}")  # Registrando o início da leitura do arquivo de log
        logs_df = spark.read.text(file_path)  # Lendo o arquivo de log e criando um DataFrame
        log_event(step_name, "Sucesso", record_count=logs_df.count())  # Registrando o sucesso da leitura, incluindo a quantidade de registros lidos
        return logs_df  # Retornando o DataFrame com os dados lidos
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando o erro caso ocorra e levantando novamente a exceção
        raise

# Criar camada Silver: Extrair campos dos Logs com expressões regulares.
def bronze_to_silver(logs_df):
    """
    Extrai campos dos logs usando expressões regulares e os transforma para a camada Silver.

    Parâmetros:
    - logs_df (DataFrame): DataFrame contendo os logs em formato bruto.

    Retorna:
    - DataFrame: DataFrame contendo os campos extraídos e transformados para a camada Silver.
    """
    step_name = "Bronze to Silver"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", "Transformando logs para camada Silver")  # Registrando o início da transformação
        ip_regex = r'^(\S+)'  # Expressão regular para extrair o IP
        timestamp_regex = r'\[(.*?)\]'  # Expressão regular para extrair o timestamp
        method_uri_protocol_regex = r'\"(\S+)\s(\S+)\s*(\S*)\"'  # Expressão regular para extrair método, URI e protocolo
        status_regex = r'\s(\d{3})\s'  # Expressão regular para extrair o código de status
        bytes_regex = r'\s(\d+)$'  # Expressão regular para extrair o número de bytes
        log_event(step_name, "Sucesso")  # Registrando o sucesso da transformação
        return logs_df.select(
            regexp_extract('value', ip_regex, 1).alias('IP'),  # Extraindo o IP
            regexp_extract('value', timestamp_regex, 1).alias('Timestamp'),  # Extraindo o timestamp
            regexp_extract('value', method_uri_protocol_regex, 1).alias('Method'),  # Extraindo o método
            regexp_extract('value', method_uri_protocol_regex, 2).alias('URI'),  # Extraindo a URI
            regexp_extract('value', method_uri_protocol_regex, 3).alias('Protocol'),  # Extraindo o protocolo
            regexp_extract('value', status_regex, 1).alias('Status'),  # Extraindo o status
            regexp_extract('value', bytes_regex, 1).cast(IntegerType()).alias('Bytes')  # Extraindo e convertendo os bytes
        )
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando o erro caso ocorra
        raise  # Levantando novamente a exceção

# Criar camada Gold: Aadicionar e formatar colunas.
def silver_to_gold(logs_parsed_df):
    """
    Adiciona e formata as colunas de data, dia da semana e categoria de status, transformando os dados da camada Silver para a camada Gold.

    Parâmetros:
    - logs_parsed_df (DataFrame): DataFrame contendo os logs da camada Silver.

    Retorna:
    - DataFrame: DataFrame com colunas adicionais e transformadas para a camada Gold.
    """
    step_name = "Silver to Gold"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", "Transformando logs para camada Gold")  # Registrando o início da transformação
        logs_parsed_df = logs_parsed_df.withColumn(
            'Date',  # Adicionando a coluna 'Date'
            functions.regexp_extract('Timestamp', r'^(.*?)\:', 1)  # Extraindo a data do timestamp
        ).withColumn(
            'Date', functions.to_date('Date', 'dd/MMM/yyyy')  # Convertendo para formato de data
        ).withColumn(
            'Day_of_Week', functions.expr('EXTRACT(DAYOFWEEK FROM Date)')  # Extraindo o dia da semana
        ).withColumn(
            'Day_of_Week_Name',  # Adicionando o nome do dia da semana
            when(col('Day_of_Week') == 1, "Domingo")  # Mapeando os valores do dia da semana
            .when(col('Day_of_Week') == 2, "Segunda")
            .when(col('Day_of_Week') == 3, "Terça")
            .when(col('Day_of_Week') == 4, "Quarta")
            .when(col('Day_of_Week') == 5, "Quinta")
            .when(col('Day_of_Week') == 6, "Sexta")
            .when(col('Day_of_Week') == 7, "Sábado")
            .otherwise("desconhecido")  # Caso o dia da semana não seja reconhecido
        ).withColumn(
            'Categoria_Status',  # Adicionando a coluna 'Categoria_Status'
            functions.when((functions.col('Status') >= 200) & (functions.col('Status') < 300), "Sucesso")  # Mapeando status de sucesso
            .when((functions.col('Status') >= 300) & (functions.col('Status') < 400), "Redirecionamento")  # Mapeando status de redirecionamento
            .when((functions.col('Status') >= 400) & (functions.col('Status') < 500), "Erro do Cliente")  # Mapeando erro do cliente
            .when((functions.col('Status') >= 500) & (functions.col('Status') < 600), "Erro do Servidor")  # Mapeando erro do servidor
            .otherwise("Desconhecido")  # Caso o status não se enquadre em nenhum grupo
        ).withColumn(
            'Endpoint',  # Adicionando a coluna 'Endpoint'
            when(col('URI') == "/", "home-page")  # Se a URI for '/', o endpoint é 'home-page'
            .when(col('URI').rlike(r'\.\w+$'), "")  # Se a URI terminar com uma extensão, o endpoint fica vazio
            .otherwise(regexp_extract(col('URI'), r'^/([^/]+)(?:/|$)', 1))  # Extraindo o primeiro segmento da URI
        )
        log_event(step_name, "Sucesso")  # Registrando o sucesso da transformação
        return logs_parsed_df  # Retornando o DataFrame com as novas colunas
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando qualquer erro que ocorra
        raise  # Levantando novamente a exceção

# Salvar dados no Blob Storage: Salva dados processados em formato Parquet.
def save_to_blob(logs_parsed_df, path):
    """
    Salva os dados processados em formato Parquet no Blob Storage.

    Parâmetros:
    - logs_parsed_df (DataFrame): DataFrame contendo os dados processados a serem salvos.
    - path (str): Caminho de destino no Blob Storage onde os dados serão salvos.

    Retorna:
    - Um arquivo Parquet a ser consumido pelo Power Bi.
    """
    step_name = "Save to Blob"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", f"Salvando dados processados em {path}")  # Registrando o início da operação
        logs_parsed_df.coalesce(1).write.format("parquet").mode("overwrite").save(path)  # Salvando os dados em formato Parquet com sobrescrita
        log_event(step_name, "Sucesso")  # Registrando o sucesso da operação
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando qualquer erro que ocorra
        raise  # Levantando novamente a exceção

# Ativar o Trigger no Power Automate: Aciona trigger no Power Automate com DBUtils.
def trigger():
    """
    Manipula um trigger no Power Automate, escrevendo um conteúdo vazio no caminho especificado para iniciar o processo.

    Parâmetros:
    Nenhum.

    Retorna:
    Nenhum.
    """
    step_name = "Trigger"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", f"Acionando o Trigger")  # Registrando o início da operação
        content = ""  # Conteúdo vazio para ativar o trigger
        dbutils.fs.put(trigger_path, content, overwrite=True)  # Escrevendo o conteúdo no caminho
        log_event(step_name, "Sucesso")  # Registrando o sucesso da operação
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando erro no log
        raise  # Levantando novamente a exceção

# Salvar o Access Log estruturado em Delta: Salva DataFrame de logs como tabela Delta.
def save_as_delta_table(df, table_name, delta_path):
    """
    Salva o DataFrame como uma tabela Delta no caminho especificado para otimizar as análises.

    Parâmetros:
    - df (DataFrame): O DataFrame de logs a ser salvo.
    - table_name (str): O nome da tabela Delta a ser criada ou sobrescrita.
    - delta_path (str): O caminho onde os dados Delta serão armazenados.

    Retorna:
    Nenhum.
    """
    step_name = "Full Load to Delta"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", f"Salvando DataFrame como tabela Delta: {table_name}")  # Registrando o início da operação
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")  # Removendo a tabela existente, caso exista
        df.write.format("delta").mode("overwrite").save(delta_path)  # Salvando o DataFrame no formato Delta
        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_path}'")  # Criando a tabela Delta
        log_event(step_name, "Sucesso", record_count=df.count())  # Registrando o sucesso da operação com a contagem de registros
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando erro no log
        raise  # Levantando novamente a exceção

# Contar as 10 maiores origens de acesso.
def top_ip_count(logs_df):
    """
    Conta as 10 origens de acesso (IPs) mais frequentes nos logs.

    Parâmetros:
    - logs_df (DataFrame): DataFrame contendo os logs com a coluna "IP".

    Retorna:
    - top_ips (DataFrame): DataFrame com as 10 origens de acesso mais frequentes, incluindo a contagem.
    """
    step_name = "Top 10 - IP Count"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", "Contando as 10 maiores origens de acesso")  # Registrando o início da operação
        top_ips = logs_df.groupBy("IP").count().orderBy(col("count").desc()).limit(10)  # Calculando as 10 origens de acesso mais frequentes
        top_ips.show(truncate=False)  # Exibindo as 10 maiores origens de acesso
        log_event(step_name, "Sucesso", record_count=top_ips.count())  # Registrando o sucesso da operação com a contagem de registros
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando erro no log
        raise  # Levantando novamente a exceção

# Listar os 6 endpoints mais acessados. 
def top_endpoints_count(logs_df):
    """
    Conta os 6 endpoints mais acessados, desconsiderando registros de arquivos (Endpoint vazio).

    Parâmetros:
    - logs_df (DataFrame): DataFrame contendo os logs com a coluna "Endpoint".

    Retorna:
    - top_endpoints (DataFrame): DataFrame com os 6 endpoints mais acessados, incluindo a contagem de acessos.
    """
    step_name = "Top 6 - Endpoints Count"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", "Contando os 6 endpoints mais acessados")  # Registrando o início da operação
        non_empty_endpoints = logs_df.filter(col("Endpoint") != "")  # Filtrando os logs para desconsiderar os registros com Endpoint vazio
        top_endpoints = non_empty_endpoints.groupBy("Endpoint").count().orderBy(col("count").desc()).limit(6)  # Calculando os 6 endpoints mais acessados
        top_endpoints.show(truncate=False)  # Exibindo os 6 endpoints mais acessados
        log_event(step_name, "Sucesso", record_count=top_endpoints.count())  # Registrando o sucesso da operação com a contagem de registros
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando erro no log
        raise  # Levantando novamente a exceção

# Contar a quantidade de IPs distintos.
def distinct_ips(logs_df):
    """
    Conta a quantidade de IPs distintos nos logs.

    Parâmetros:
    - logs_df (DataFrame): DataFrame contendo os logs com a coluna "IP".

    Retorna:
    - distinct_ips_count (int): Quantidade de IPs distintos encontrados nos logs.
    """
    step_name = "Distinct IPs"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", "Contando a quantidade de IPs distintos")  # Registrando o início da operação
        distinct_ips_count = logs_df.select("IP").distinct().count()  # Contando os IPs distintos
        print(f"Quantidade de IPs distintos: {distinct_ips_count}")  # Exibindo o resultado
        log_event(step_name, "Sucesso", record_count=distinct_ips_count)  # Registrando o sucesso da operação com a contagem
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando erro no log
        raise  # Levantando novamente a exceção

# Contar os dias distintos representados no arquivo.
def distinct_days(logs_df):
    """
    Conta a quantidade de dias distintos representados nos logs.

    Parâmetros:
    - logs_df (DataFrame): DataFrame contendo os logs com a coluna "Date".

    Retorna:
    - distinct_days_count (int): Quantidade de dias distintos encontrados nos logs.
    """
    step_name = "Distinct Days"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", "Contando a quantidade de dias representados")  # Registrando o início da operação
        distinct_days_count = logs_df.select("Date").distinct().count()  # Contando os dias distintos
        print(f"Quantidade de dias representados: {distinct_days_count}")  # Exibindo o resultado
        log_event(step_name, "Sucesso", record_count=distinct_days_count)  # Registrando o sucesso da operação com a contagem
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando erro no log
        raise  # Levantando novamente a exceção

# Análise do tamanho das respostas: Calcula total, máximo, mínimo e média dos bytes.
def response_size_analysis(logs_df):
    """
    Realiza a análise do tamanho das respostas, calculando o total, máximo, mínimo e médio de bytes.

    Parâmetros:
    - logs_df (DataFrame): DataFrame contendo os logs com a coluna "Bytes" que representa o tamanho das respostas.

    Retorna:
    - Imprime os resultados no console e registra o evento no log.
    """
    step_name = "Byte das Respostas"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", "Analisando o tamanho das respostas")  # Registrando o início da operação
        total_volume = logs_df.select(sum("Bytes")).collect()[0][0]  # Calculando o total de bytes
        max_volume = logs_df.select(max("Bytes")).collect()[0][0]  # Calculando o valor máximo de bytes
        min_volume = logs_df.select(min("Bytes")).collect()[0][0]  # Calculando o valor mínimo de bytes
        avg_volume = logs_df.select(avg("Bytes")).collect()[0][0]  # Calculando a média de bytes
        # Exibindo os resultados no console
        print(f"Total de dados: {total_volume} bytes")
        print(f"Maior volume em uma resposta: {max_volume} bytes")
        print(f"Menor volume em uma resposta: {min_volume} bytes")
        print(f"Volume médio de dados: {avg_volume:.2f} bytes")
        log_event(step_name, "Sucesso", message=f"Total: {total_volume}, Máximo: {max_volume}, Mínimo: {min_volume}, Médio: {avg_volume:.2f}")  # Registrando o sucesso da operação
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando erro no log
        raise  # Levantando novamente a exceção

# Identificar o dia da semana com maior número de erros "HTTP Client Error".
def client_error_day_of_week(logs_df):
    """
    Identifica o dia da semana com maior número de erros HTTP do tipo "Client Error" (Status 400-499).

    Parâmetros:
    - logs_df (DataFrame): DataFrame contendo os logs com informações de status e dia da semana.

    Retorna:
    - Exibe os resultados no console e registra o evento no log.
    """
    step_name = "Dia das Semana com Mais Erros"  # Nome da etapa para rastrear o log de execução
    try:
        log_event(step_name, "Inicio", "Identificando o dia da semana com maior número de erros HTTP Client Error")  # Registrando o início da operação
        client_errors = logs_df.filter((col("Status") >= 400) & (col("Status") < 500))  # Filtrando os logs para os erros de cliente (Status 400-499)
        errors_by_day = client_errors.groupBy("Day_of_Week_Name").count().orderBy(col("count").desc())  # Agrupando por dia da semana e contando os erros
        errors_by_day.show(truncate=False)  # Exibindo os resultados no console
        log_event(step_name, "Sucesso", record_count=errors_by_day.count())  # Registrando o sucesso da operação
    except Exception as e:
        log_event(step_name, "Erro", message=str(e))  # Registrando erro no log
        raise  # Levantando novamente a exceção

# Converter logs para DataFrame Spark e salvar em Delta.
def log_to_delta(processing_log, log_delta_path):
    """
    Converte o log processado para um DataFrame e salva no formato Delta.

    Parâmetros:
    - processing_log (list): Lista contendo os logs processados.
    - log_delta_path (str): Caminho onde os logs serão salvos no formato Delta.

    Retorna:
    - DataFrame no formato Delta e imprime a confirmação no console.
    """
    log_df = spark.createDataFrame(processing_log)  # Criando o DataFrame a partir da lista de logs
    log_df.write.format("delta").mode("overwrite").save(log_delta_path)  # Salvando o DataFrame no Delta
    print(f"Logs salvos no Delta Lake no caminho: {log_delta_path}")  # Confirmando no console

# Consultar dados do Delta Lake com Spark SQL.
def query_delta_logs(log_delta_path, query):
    """
    Consulta os dados no Delta Lake usando uma query SQL.

    Parâmetros:
    - log_delta_path (str): Caminho para os dados armazenados no Delta Lake.
    - query (str): Consulta SQL a ser executada nos dados.

    Retorna:
    - result (DataFrame): DataFrame contendo os resultados da consulta.
    """
    spark.read.format("delta").load(log_delta_path).createOrReplaceTempView("logs_temp")  # Lê os dados do Delta Lake e cria uma view temporária para consulta
    result = spark.sql(query)  # Executa a consulta SQL no Delta
    result.show()  # Exibe o resultado da consulta

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestão e Tratamento por Camadas

# COMMAND ----------

# Ler arquivo Full Load: Carrega o arquivo de log e retorna um DataFrame.
logs_df = read_logs(file_path)

# Estruturação e Definição de Schema: Converte dados brutos para formato estruturado.
logs_parsed_df = bronze_to_silver(logs_df)

# Refina os dados para o formato (gold): Acrescentando colunas com informações adicionais.
logs_parsed_df = silver_to_gold(logs_parsed_df)

# Cria um Parquet no Blob Storage para ser consumido pelo Power BI.
save_to_blob(logs_parsed_df, full_load_path)

# Aciona um trigger no Power Automate para que o Power BI seja atualizado.
trigger()

# Salva o DataFrame processado como uma tabela Delta
save_as_delta_table(logs_parsed_df, "access_log_delta", delta_table_acessLog_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Desafios

# COMMAND ----------

# Carregar a Delta Table como DataFrame
logs_parsed_df = spark.read.format("delta").load(delta_table_acessLog_path)

# Realiza uma análise para contar as 10 maiores origens de acesso (IPs).
top_ip_count(logs_parsed_df)

# Realiza uma análise para contar os 6 endpoints mais acessados.
top_endpoints_count(logs_parsed_df)

# Conta a quantidade de IPs distintos presentes nos logs.
distinct_ips(logs_parsed_df)

# Conta a quantidade de dias distintos representados no arquivo de log.
distinct_days(logs_parsed_df)

# Realiza uma análise sobre o tamanho das respostas.
response_size_analysis(logs_parsed_df)

# Identifica o dia da semana com o maior número de erros "HTTP Client Error".
client_error_day_of_week(logs_parsed_df)


# COMMAND ----------

# MAGIC %md
# MAGIC # Armazenamento dos Logs

# COMMAND ----------

# Converte a lista 'processing_log' em um DataFrame do Spark e salva no Delta Lake.
log_to_delta(processing_log, log_delta_path)

# Executa e exibe uma consulta SQL no Spark para filtrar os registros da tabela Delta onde o status é 'Erro'.
query = "SELECT * FROM logs_temp WHERE Status = 'Erro' ORDER BY Timestamp ASC"
query_delta_logs(log_delta_path, query)

# Executa e exibe uma consulta SQL no Spark para filtrar os registros da tabela Delta onde o status é 'Sucesso'.
query = "SELECT * FROM logs_temp WHERE Status = 'Sucesso' ORDER BY Timestamp ASC"
query_delta_logs(log_delta_path, query)
