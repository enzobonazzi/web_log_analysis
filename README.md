# README: Web Log Analysis

---

# Sumário

1. **Introdução**
2. **Pré-requisitos**
3. **Objetivos do Projeto**
4. **Tecnologias Utilizadas**
5. **Estrutura de Dados**
6. **Automação com Power Automate**
7. **Configuração do Ambiente**
8. **Execução do Projeto**
9. **Pipeline do Projeto**
10. **Operações com Delta Lake**
11. **Visualização no Power BI**
12. **Resultados e Benefícios**
13. **Possíveis Melhorias Futuras**
14. **Observações**
15. **Navegação do repositório: web_logs**
16. **Documentação das Bibliotecas Utilizadas**
17. **Contato**

---

# Introdução
Este projeto implementa uma pipeline de análise de logs de acesso a servidores web usando PySpark e Delta Lake. A análise é realizada no Databricks, e os dados são armazenados e gerenciados usando Delta Tables para oferecer transações ACID, versionamento de dados e desempenho otimizado, integrando soluções para automação de atualização e visualização de dados em tempo real. O pipeline de dados foi projetado com foco em eficiência, escalabilidade e análise de insights para tomada de decisão.

---

# Pré-requisitos
Antes de iniciar, certifique-se de que você tem:
1. Uma conta no [Databricks Community Edition](https://community.cloud.databricks.com/).
2. Acesso ao Azure Blob Storage com as credenciais necessárias (como SAS token).
3. Familiaridade com Python, PySpark e SQL básico.
4. Conhecimento em Power Platform (Power BI e Power Automate).

---

# Objetivos do Projeto
- **Ingestão de Dados**: Ler logs de um arquivo full load no formato .txt  (armazenado em um Blob Storage no Azure).
- **Transformação**: Processar, limpar e enriquecer os dados em camadas Bronze, Silver e Gold utilizando o PySpark.
- **Armazenamento Estruturado**: Salvar os dados processados em um arquivo Parquet único na camada Gold, otimizando para visualização de dados.
- **Armazenamento Otimizado**: Salvar o DataFrame em tabela Delta para otimizar as consultas e cálculos.
- **Automação**: Configurar gatilhos no Power Automate para atualizar relatórios do Power BI automaticamente sempre que houver novos dados na camada Gold.
- **Visualização**: Criar relatórios no Power BI para análise de dados, incluindo insights detalhados sobre os logs processados.

---

# Tecnologias Utilizadas
- **Azure Blob Storage**: Ingestão e Armazenamento de dados.
- **Databricks / PySpark**: Processamento e transformação de dados.
- **Delta Lake**: Camada de armazenamento unificada no Databricks, garantindo transações ACID, versionamento e performance para leitura e escrita dos dados tratados.
- **Power Automate**: Automação de fluxos, configurando gatilhos (triggers) que atualizam automaticamente os relatórios do Power BI com base nos dados da camada Gold.
- **Power BI**: Visualização de dados e geração de insights.

---

# Estrutura de Dados
1. **Camada Bronze**:
   - Dados brutos extraídos do .txt.
   - O arquivo é lido e armazenado nesta camada sem transformação significativa.

2. **Camada Silver**:
   - Dados passam a ser estruturados e pré-processados.
   - Correção de formatos (Uso de expressões regulares para extrair partes específicas dos logs)

3. **Camada Gold**:
   - Dados prontos para consumo.
   - Criação de colunas adicionais e armazenamento em arquivo Parquet.
     
---

# Automação com Power Automate
A integração com o Power Automate permite que, sempre que novos dados sejam adicionados à camada Gold, um **gatilho (trigger)** seja acionado para atualizar automaticamente os relatórios do Power BI. Essa funcionalidade elimina a necessidade de atualizações manuais, garantindo que os dados estejam sempre atualizados para análise.

---

# Configuração do Ambiente

### 1. Configuração do Databricks
**Crie um cluster no Databricks Community Edition:**
   - Vá para a aba "Clusters" e clique em "Create Cluster".
   - Configure o cluster com as seguintes especificações mínimas:
     - Runtime: Databricks Runtime 12.2 LTS.
     - Node Type: Seleção padrão para Community Edition.

### 2. Inicialize a SparkSession
No código fornecido, execute o seguinte comando para usar a API do Spark.

```python
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()
```

### 3. Configuração do Azure Blob Storage
No código fornecido, configure os seguintes parâmetros no Databricks:
- Substitua `storage_account_name`, `container_name` e `sas_token` com as informações específicas do seu Azure Blob Storage.
- Adicione a configuração do SAS Token no Spark Config:

```python
spark.conf.set(f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net", sas_token)
```
---

# Execução do Projeto

### 1. Upload do Código
1. No Databricks, vá para "Workspace" > "Users" > *Seu Nome de Usuário*.
2. Clique com o botão direito e selecione "Create" > "Notebook".
3. Dê um nome ao notebook (por exemplo, `web_log_analysis`) e configure a linguagem para Python.
4. Copie e cole o código fornecido no notebook, de preferencia em formato de 'células'

### 2. Estrutura
O código realiza as seguintes etapas:
- **Leitura dos logs (Camada Bronze):** Lê os logs brutos de arquivos armazenados no Azure Blob Storage.
- **Transformação (Camada Silver):** Extrai campos úteis como IP, Timestamp, URI, Status e etc, usando expressões regulares.
- **Enriquecimento (Camada Gold):** Adiciona colunas com informações derivadas, como Dia da Semana, Categoria de Status e Endpoint.
- **Formação do arquivo .Parquet:** Cria um arquivo .parquet com base na camada gold para ser consumido pelo Power BI.
- **Trigger para o Power Automate:** Manipula um arquivo para acionar uma automação no Power Automate para atualizar o Power BI.
- **Conversão em Delta Tables:** Salva os dados transformados em Delta Tables para análise futura.
- **Cálculos e Análises:** A partir da Delta Table realiza cálculos e análise para cumprir o desafio proposto.
- **Armazenamento e Consulta de Log:** Os logs de eventos distribuídos nas função são armazenados e consultados em Spark SQL.

### 3. Execução
1. **Conecte-se ao Blob Storage:** Certifique-se de que as configurações de autenticação do Token SAS estão corretas.
2. **Execute cada célula do notebook:**
   - Comece pela inicialização da SparkSession.
   - Atribuições das variáveis e paths.
   - Criação das Funções.
   - Prossiga com a ingestão e leitura dos logs.
   - Realize os cálculos.
   - Realize as transformações e gravações em Delta Tables.

---

# Pipeline do Projeto

Clique na imagem para expandi-la.

![Image](https://github.com/user-attachments/assets/e5f28983-f5fd-41f7-978b-0624a6956c83)

---

# Operações com Delta Lake

### Escrita em Delta Table
No código, os dados processados são gravados em Delta Tables no caminho configurado:
```python
logs_parsed_df.write.format("delta").mode("overwrite").save(delta_table_acessLog_path)
```

### Consumo em Spark SQL da Delta Table
Para acessar os dados gravados via Spark SQL:
```python
 spark.read.format("delta").load(log_delta_path).createOrReplaceTempView("logs_temp")
 result = spark.sql(SELECT * FROM logs_temp WHERE Status = 'Erro' ORDER BY Timestamp ASC)
 result.show()
```

---

# Resultado
### Acessar o Notebook do Databricks
Confira os resultados do desafio e a estruturação do código realizado. Clique no link abaixo para acessar o notebook completo:
  - **[Notebook - Web Log Analysis](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3778539461350800/1478756486841749/6844958453960642/latest.html)**

---

---

# Visualização no Power BI
O relatório no Power BI foi projetado para fornecer os seguintes insights:

1. **Total de IP's**.
2. **IP's Distintos**.
3. **Média de Requisição por Dia**.
4. **Dias Distintos**.
5. **URI's Distintos**.
6. **Média de Bytes por Requisição**.
7. **Quantitativo de Requisições por Método**.
8. **Quantitativo de Requisições por Categoria de Status**.
9. **Top 10 IP's por Quantidade de Acessos**.
10. **Top 6 Endpoints Mais Acessados**.
11. **Comparação entre Tipos de Protocolos**.
12. **Quantitativos de Requisição por Data**.
13. **Dia da Semana com o Maior Número de 'Erros HTTP (Client Error)'**.

Além disso, o relatório inclui uma **tabela completa** com todas as requisições processadas, permitindo que os usuários apliquem filtros personalizados e exportem os dados em diversos formatos, como Excel e CSV.

### Acessar o Dashboard
Para acessar o dashboard clique no link abaixo:
  - **[relatorio_web](https://app.powerbi.com/view?r=eyJrIjoiYmQxZGU1MWUtM2Y0ZS00NDllLWE3ZTAtNjNkY2JjNGE5YjkzIiwidCI6IjEzYzIzNzQxLWE5NDItNGRiNy04MGJlLTc3MjcwZGYzMmJhMCJ9)**: Página web com o relatório do Power BI.

---

# Impactos e Benefícios
- **Automação**: Atualização em tempo real dos dados e relatórios, economizando tempo e reduzindo erros manuais.
- **Performance**: Processamento de grandes volumes de dados em alta velocidade utilizando PySpark.
- **Insights**: Relatórios detalhados no Power BI, proporcionando uma visão ampla e aprofundada dos logs processados.

---

# Possíveis Melhorias Futuras
- Aplicação de Key Vault para maior segurança da aplicação.
- Implementar monitoramento contínuo do pipeline para detecção de falhas em tempo real.
- Utilizar de teste unitário e da esteira de CI/CD para garantir confiabilidade nas mudanças do pipeline.
- Expandir as análises no Power BI para incluir previsões baseadas em séries temporais.
- Integrar Machine Learning para identificar padrões ou anomalias nos logs processados.

---

# Observações
- Este projeto foi projetado para rodar no Databricks Community Edition, mas pode ser facilmente adaptado para ambientes corporativos com clusters mais robustos.
- Certifique-se de renovar ou configurar corretamente o SAS Token do Azure antes da execução.

Este projeto demonstra a integração eficiente entre diferentes ferramentas de Big Data e visualização, resultando em uma solução robusta e escalável para análise de logs.

---

# Navegação do repositório: web_logs

Este repositório contém o script e o relatório utilizados no projeto **web_logs**.

### Estrutura de pastas

- **script**: Contém os arquivos relacionados ao script do Databricks.
  - **[script.py](https://github.com/enzobonazzi/web_log_analysis/blob/main/script.py)**: Script principal do Databricks em Python.
  - **[script.ipynb](https://github.com/enzobonazzi/web_log_analysis/blob/main/script.ipynb)**: Notebook do Databricks.
 
- **powerbi**: Contém o arquivo de Power BI utilizado para análise.
  - **[relatorio.pbip](https://github.com/enzobonazzi/web_log_analysis/blob/main/relatorio.pbip)**: Arquivo de relatório do Power BI.

### Como acessar os arquivos
Cada arquivo acima está vinculado ao seu respectivo link. 

---
# Documentação das Bibliotecas Utilizadas

Este bloco contém a descrição das bibliotecas usadas no projeto **web_logs**.

### Bibliotecas para Manipulação de Dados com PySpark

- **`pyspark.sql.functions`**: Conjunto de funções para operações em colunas de DataFrame.
  - **`regexp_extract`**: Função para extrair uma substring usando expressão regular.
  - **`col`**: Função para referenciar uma coluna em operações de transformação.
  - **`date_format`**: Função para formatar uma coluna de data para um formato específico.
  - **`to_date`**: Função para converter uma string em um tipo de dado de data.
  - **`expr`**: Função para executar expressões SQL diretamente no código Spark.
  - **`avg`**: Função para calcular a média de uma coluna.
  - **`max`**: Função para calcular o valor máximo de uma coluna.
  - **`min`**: Função para calcular o valor mínimo de uma coluna.
  - **`sum`**: Função para calcular a soma dos valores de uma coluna.
  - **`when`**: Função para aplicar condições (semelhante ao `IF`).
  
  Documentação oficial: [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)

- **`pyspark.sql.types`**: Biblioteca para definição de tipos de dados em DataFrames.
  - **`IntegerType`**: Tipo de dado para colunas inteiras.
  
  Documentação oficial: [PySpark SQL Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)

- **`delta.tables`**: Biblioteca para trabalhar com Delta Tables no Spark, oferecendo suporte a transações ACID.
  
  Documentação oficial: [Delta Lake Documentation](https://docs.delta.io/latest/index.html)

- **`datetime`**: Biblioteca do Python para manipulação de datas e horas.
  
  Documentação oficial: [Datetime — Basic date and time types](https://docs.python.org/3/library/datetime.html)

### Documentação do Power BI

- **Power BI Desktop**: Ferramenta de criação de relatórios e painéis interativos para análise de dados. Para saber mais sobre o uso e recursos, consulte a [documentação do Power BI](https://learn.microsoft.com/pt-br/power-bi/).

- **Power BI Service**: Plataforma online para compartilhamento de relatórios criados no Power BI Desktop. Saiba mais na [documentação do Power BI Service](https://learn.microsoft.com/pt-br/power-bi/report-server/).

### Documentação do Power Automate

- **Power Automate**: Ferramenta para automação de fluxos de trabalho, integrando diversos serviços e processos. A documentação completa está disponível em [Power Automate Documentation](https://learn.microsoft.com/pt-br/power-automate/).

### Documentação Databricks

- **Databricks**: Plataforma unificada para engenharia de dados, ciência de dados e aprendizado de máquina. Explore a [documentação do Databricks](https://docs.databricks.com/en/index.html) para mais detalhes sobre como usar a plataforma.

### Bibliotecas do Azure Blob Storage

- **Azure Blob Storage**: Serviço de armazenamento de objetos para dados não estruturados, como arquivos e imagens. Para aprender mais, consulte a [documentação do Azure Blob Storage](https://learn.microsoft.com/pt-br/azure/storage/blobs/).

---

# Contato
Para dúvidas ou melhorias, entre em contato pelo GitHub ou via e-mail: [enzopolisel@gmail.com](enzopolisel@gmail.com).
