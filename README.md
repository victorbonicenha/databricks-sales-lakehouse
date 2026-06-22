# Databricks Sales Lakehouse

Projeto de Engenharia de Dados usando **Databricks**, **PySpark**, **Delta Lake**, **SQL** e arquitetura **Medallion**.

A ideia do projeto é simular um pipeline real de vendas de e-commerce, com ingestão de arquivos brutos, tratamento dos dados, validações de qualidade e criação de tabelas analíticas para consumo em SQL/Dashboard.

---

## Objetivo

Construir um fluxo de dados dividido em camadas:

* **Bronze:** ingestão dos dados crus, mantendo o máximo de fidelidade ao arquivo original.
* **Silver:** limpeza, padronização, tipagem, tratamento de duplicados e separação de registros inválidos.
* **Gold:** criação de tabelas analíticas para responder perguntas de negócio.

---

## Stack utilizada

* Databricks
* PySpark
* Delta Lake
* SQL
* Python
* Arquitetura Medallion
* Data Quality Checks

---

## Estrutura do projeto

```text
.
├── data/
│   └── raw/
│       ├── customers/
│       ├── products/
│       ├── orders/
│       └── payments/
│
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_gold_analytics.py
│   └── 04_data_quality_checks.py
│
├── scripts/
│   └── generate_fake_data.py
│
├── sql/
│   └── dashboard_queries.sql
│
├── docs/
│   ├── architecture.md
│   └── data_dictionary.md
│
└── README.md
```

---

## Pipeline

```text
Arquivos CSV/JSON
        ↓
Bronze - dados crus
        ↓
Silver - dados limpos e padronizados
        ↓
Gold - métricas e tabelas analíticas
        ↓
SQL / Dashboard
```

---

## Datasets simulados

O projeto usa dados fictícios de:

* clientes
* produtos
* pedidos
* pagamentos

Os dados possuem propositalmente alguns problemas comuns em pipelines reais:

* registros duplicados
* valores nulos
* datas em texto
* status inconsistentes
* valores negativos
* pedidos sem cliente válido
* pagamentos com divergência de valor

---

## Tabelas criadas

### Bronze

* `bronze_customers`
* `bronze_products`
* `bronze_orders`
* `bronze_payments`

### Silver

* `silver_customers`
* `silver_products`
* `silver_orders`
* `silver_payments`
* `silver_order_details`
* `silver_rejected_orders`
* `silver_rejected_products`

### Gold

* `gold_daily_sales`
* `gold_sales_by_category`
* `gold_top_customers`
* `gold_payment_summary`
* `gold_order_status_summary`

### Data Quality

* `dq_results`

---

## Perguntas respondidas pela camada Gold

* Qual o faturamento diário?
* Qual o ticket médio por dia?
* Quais categorias mais vendem?
* Quais clientes mais compram?
* Qual a distribuição dos status dos pedidos?
* Quais métodos de pagamento têm maior volume?

---

## Como rodar no Databricks

### 1. Criar ou acessar um workspace Databricks

Pode ser um workspace Community/Free/Trial ou corporativo.

### 2. Subir os dados para o Databricks

Você pode subir a pasta `data/raw` para algum caminho como:

```text
/Volumes/workspace/default/sales_lakehouse/raw
```

ou ajustar o caminho nos widgets dos notebooks.

### 3. Importar os notebooks

Importe os arquivos da pasta `notebooks/` no Databricks.

### 4. Rodar na ordem

```text
00_setup.py
01_bronze_ingestion.py
02_silver_transformations.py
03_gold_analytics.py
04_data_quality_checks.py
```

### 5. Rodar as queries SQL

Use o arquivo:

```text
sql/dashboard_queries.sql
```

para criar visualizações no Databricks SQL.

---

## Como gerar novos dados localmente

Caso queira gerar uma nova massa de dados:

```bash
python scripts/generate_fake_data.py
```

O script vai recriar os arquivos em:

```text
data/raw/
```

---

## Possíveis evoluções

* Ingestão incremental com Auto Loader
* Orquestração com Databricks Workflows
* Integração com cloud storage
* Criação de testes automatizados
* Particionamento das tabelas Delta
* Uso de Change Data Feed
* Monitoramento com logs técnicos
* Criação de dashboard no Databricks SQL

---

## Autor

Projeto desenvolvido para estudo e portfólio em Engenharia de Dados.
