# Arquitetura do Projeto

Este projeto usa a arquitetura Medallion para organizar os dados em camadas de qualidade.

## Bronze

A camada Bronze representa os dados crus.

Características:

- mantém os dados próximos do formato original
- adiciona metadados técnicos de ingestão
- serve como histórico bruto para reprocessamento

Exemplos de metadados:

- `ingestion_timestamp`
- `source_file`

## Silver

A camada Silver representa os dados tratados e padronizados.

Transformações realizadas:

- conversão de tipos
- padronização de textos
- remoção de duplicados
- tratamento de valores nulos
- validação de regras básicas
- separação de registros rejeitados

## Gold

A camada Gold representa dados prontos para análise.

Tabelas criadas:

- vendas por dia
- vendas por categoria
- ranking de clientes
- resumo de pagamentos
- resumo de status dos pedidos

## Fluxo lógico

```text
CSV/JSON → Bronze → Silver → Gold → SQL/Dashboard
```

## Decisões técnicas

### Por que usar Delta Lake?

Delta Lake permite salvar tabelas com mais confiabilidade para pipelines analíticos, facilitando leitura, escrita, atualização e controle dos dados dentro do Lakehouse.

### Por que separar registros rejeitados?

Em pipelines reais, nem sempre o dado inválido deve simplesmente ser descartado. Separar registros rejeitados ajuda na rastreabilidade, análise de problemas na origem e reprocessamento futuro.

### Por que criar uma camada Gold?

A camada Gold evita que usuários de negócio precisem consultar tabelas transacionais ou aplicar regras complexas toda vez. Ela entrega dados já agregados e prontos para consumo.
