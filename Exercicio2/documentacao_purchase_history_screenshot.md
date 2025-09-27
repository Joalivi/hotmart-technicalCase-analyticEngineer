# Documentação - Tabela purchase_history_screenshot

## Visão Geral

A tabela `purchase_history_screenshot` é o snapshot histórico diário de todas as compras, contendo o estado mais atualizado de cada transação até uma determinada data de referência. Cada linha representa uma compra em um momento específico do tempo.

## Estrutura da Tabela

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `reference_date` | DATE | Data de referência do snapshot |
| `purchase_id` | STRING | Identificador único da compra |
| `buyer_id` | STRING | Identificador do comprador |
| `producer_id` | STRING | Identificador do produtor |
| `product_id` | STRING | Identificador do produto |
| `prod_item_id` | STRING | Identificador do item de produto |
| `order_date` | DATE | Data do pedido |
| `release_date` | DATE | Data de liberação |
| `purchase_transaction_date` | DATE | Data da transação de compra |
| `purchase_transaction_datetime` | TIMESTAMP | Data e hora da transação |
| `purchase_total_value` | DOUBLE | Valor total da compra |
| `purchase_value` | DOUBLE | Valor do item de produto |
| `item_quantity` | INT | Quantidade de itens |
| `gmv_value` | DOUBLE | Valor do GMV calculado |
| `is_gmv_valid` | INT | Flag de validade do GMV (1=válido, 0=inválido) |
| `purchase_status` | STRING | Status da compra |
| `subsidiary` | STRING | Subsidiária responsável |
| `purchase_partition` | BIGINT | Partição da tabela purchase |
| `prod_item_partition` | BIGINT | Partição da tabela product_item |
| `etl_processed_at` | TIMESTAMP | Data/hora do processamento ETL |

## Fluxo dos Dados

### 1. Tabelas de Origem
- **`purchase`**: Eventos de compra (CDC)
- **`product_item`**: Eventos de itens de produto (CDC)
- **`purchase_extra_info`**: Informações extras de compra (CDC)

### 2. Processamento ETL
```
Tabelas de Eventos → ETL purchase_history_screenshot.py → purchase_history_screenshot
```

### 3. Lógica de Processamento
1. **Extração**: Coleta dados das 3 tabelas de eventos
2. **Criação da Base**: Gera combinações únicas de `(reference_date, purchase_id)`
3. **Enriquecimento**: Para cada combinação, busca a última transação até a `reference_date`
4. **Validação**: Aplica regras de qualidade de dados
5. **Carga**: Salva na tabela final

## Funcionamento

### O que representa cada linha
Cada linha representa:
- **Uma compra** (`purchase_id`) 
- **Em uma data específica** (`reference_date`)
- **Com o estado mais atualizado** até aquela data

### Exemplo prático
```
reference_date: 2024-01-15
purchase_id: COMPRA_123
→ Dados da compra COMPRA_123 no estado mais atual até 15/01/2024
```

### Processamento diário
- **D-1**: Processa dados do dia anterior
- **Histórico**: Processa período específico (ex: 2023-01-01 a 2023-12-31)

## Regras de Negócio

### GMV (Gross Merchandising Value)
- **Cálculo**: `purchase_total_value` quando `release_date` preenchida e `purchase_status` diferente de CANCELADA/REEMBOLSADA
- **Validação**: `is_gmv_valid` = 1 para compras com pagamento efetuado e não canceladas
- **Invalidação**: `is_gmv_valid` = 0 para compras sem `release_date` ou com status CANCELADA/REEMBOLSADA

### Qualidade de Dados
- **Nulos removidos**: `purchase_id`, `transaction_date`
- **Valores padrão**: `subsidiary` = 'UNKNOWN' quando nulo
- **Valores negativos**: Convertidos para 0.0
- **Validação**: Completeness, accuracy, consistency

### Particionamento
- **Por data**: `reference_date` para otimização de consultas
- **Localização**: `s3://data-lake/hotmart/purchase_history_screenshot/`

## Casos de Uso

### 1. Análise Temporal
- **GMV diário**: Soma de `gmv_value` por `reference_date` e `subsidiary`
- **Tendências**: Evolução de compras ao longo do tempo
- **Comparações**: Períodos diferentes

### 2. Fechamento Contábil
- **Última ocorrência**: Busca a linha mais recente de cada `purchase_id`
- **Dados finais**: Estado definitivo de cada compra
- **Auditoria**: Rastreabilidade completa

### 3. Análise de Performance
- **Por subsidiária**: Agregações por `subsidiary`
- **Por produto**: Agregações por `product_id`
- **Por cliente**: Agregações por `buyer_id`

## Consultas Típicas

### GMV Diário por Subsidiária
```sql
SELECT 
    reference_date,
    subsidiary,
    SUM(gmv_value) as gmv_total
FROM purchase_history_screenshot
WHERE is_gmv_valid = 1
GROUP BY reference_date, subsidiary
ORDER BY reference_date DESC;
```

### Fechamento Mensal
```sql
SELECT 
    purchase_id,
    subsidiary,
    gmv_value,
    purchase_status
FROM purchase_history_screenshot
WHERE reference_date = '2024-01-31'
  AND ROW_NUMBER() OVER (PARTITION BY purchase_id ORDER BY reference_date DESC) = 1;
```

## Manutenção

### Atualizações
- **Diária**: Processamento automático D-1
- **Histórica**: Reprocessamento sob demanda
- **Correção**: Reprocessamento de períodos específicos

### Monitoramento
- **Qualidade**: Métricas de completude e consistência
- **Performance**: Tempo de processamento
- **Volume**: Número de registros processados

