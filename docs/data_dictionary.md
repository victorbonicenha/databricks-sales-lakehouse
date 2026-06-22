# Dicionário de Dados

## customers.csv

| Coluna | Descrição |
|---|---|
| customer_id | Identificador único do cliente |
| customer_name | Nome do cliente |
| email | E-mail do cliente |
| country | País do cliente |
| signup_date | Data de cadastro |

## products.csv

| Coluna | Descrição |
|---|---|
| product_id | Identificador único do produto |
| product_name | Nome do produto |
| category | Categoria do produto |
| unit_price | Preço unitário cadastrado |

## orders.csv

| Coluna | Descrição |
|---|---|
| order_id | Identificador único do pedido |
| customer_id | Cliente que realizou o pedido |
| product_id | Produto comprado |
| order_date | Data do pedido |
| status | Status do pedido |
| quantity | Quantidade comprada |
| unit_price | Preço unitário no momento da compra |

## payments.json

| Campo | Descrição |
|---|---|
| payment_id | Identificador único do pagamento |
| order_id | Pedido relacionado ao pagamento |
| payment_method | Método de pagamento |
| payment_status | Status do pagamento |
| amount | Valor pago |
| paid_at | Data/hora do pagamento |

## gold_daily_sales

| Coluna | Descrição |
|---|---|
| order_date | Data da venda |
| orders_count | Quantidade de pedidos |
| items_sold | Quantidade de itens vendidos |
| gross_revenue | Receita bruta |
| avg_ticket | Ticket médio |

## gold_sales_by_category

| Coluna | Descrição |
|---|---|
| category | Categoria do produto |
| orders_count | Quantidade de pedidos |
| items_sold | Quantidade de itens vendidos |
| gross_revenue | Receita bruta |

## dq_results

| Coluna | Descrição |
|---|---|
| check_name | Nome da validação |
| table_name | Tabela validada |
| status | Resultado da validação |
| observed_value | Valor observado |
| expected_rule | Regra esperada |
| checked_at | Data/hora da validação |
