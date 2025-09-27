-- =====================================================
-- Exemplo de Dados - Purchase History Screenshot
-- Desafio Técnico Analytics Engineer - Hotmart
-- Autor: João Vitor
-- Data: 2025
-- =====================================================

-- =====================================================
-- AMOSTRA DA TABELA PURCHASE_HISTORY_SCREENSHOT
-- =====================================================

/*
EXEMPLO DE COMO SERIA A TABELA PURCHASE_HISTORY_SCREENSHOT:

┌──────────────┬─────────────┬──────────┬─────────────┬───────────┬──────────────┬────────────┬─────────────┬─────────────────────┬─────────────────────────┬─────────────────────┬───────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────────────┬─────────────────────┬─────────────────────────┐
│reference_date│purchase_id  │buyer_id  │producer_id  │product_id │prod_item_id  │order_date  │release_date │purchase_transaction_│purchase_transaction_    │purchase_total_value │purchase_value│item_quantity│gmv_value    │is_gmv_valid│purchase_    │subsidiary   │purchase_    │prod_item_           │etl_processed_at     │                        │
│              │             │          │             │           │              │            │             │date                 │datetime                │                     │              │             │             │             │status       │             │partition    │partition            │                     │                        │
├──────────────┼─────────────┼──────────┼─────────────┼───────────┼──────────────┼────────────┼─────────────┼─────────────────────┼─────────────────────────┼─────────────────────┼───────────────┼─────────────┼─────────────┼─────────────┼─────────────┼─────────────┼─────────────┼─────────────────────┼─────────────────────┼─────────────────────────┤
│2024-01-14    │PURCH_001    │BUYER_001 │PROD_001     │PROD_001   │PROD_ITEM_001 │2024-01-10  │2024-01-12   │2024-01-14           │2024-01-14 10:30:00      │299.90                │299.90        │1            │299.90       │1            │APROVADA     │NACIONAL     │PART_001     │PART_ITEM_001        │2024-01-15 11:00:00   │                        │
│2024-01-14    │PURCH_002    │BUYER_002 │PROD_002     │PROD_002   │PROD_ITEM_002 │2024-01-11  │2024-01-13   │2024-01-14           │2024-01-14 14:20:00      │199.50                │199.50        │1            │0.0          │0            │CANCELADA    │NACIONAL     │PART_002     │PART_ITEM_002        │2024-01-15 11:00:00   │                        │
│2024-01-14    │PURCH_003    │BUYER_003 │PROD_003     │PROD_003   │PROD_ITEM_003 │2024-01-12  │2024-01-14   │2024-01-14           │2024-01-14 16:45:00      │599.00                │599.00        │2            │599.00       │1            │APROVADA     │INTERNACIONAL│PART_003     │PART_ITEM_003        │2024-01-15 11:00:00   │                        │
│2024-01-14    │PURCH_004    │BUYER_004 │PROD_004     │PROD_004   │PROD_ITEM_004 │2024-01-13  │2024-01-14   │2024-01-14           │2024-01-14 09:15:00      │149.90                │149.90        │1            │0.0          │0            │REEMBOLSADA  │NACIONAL     │PART_004     │PART_ITEM_004        │2024-01-15 11:00:00   │                        │
│2024-01-14    │PURCH_005    │BUYER_005 │PROD_005     │PROD_005   │PROD_ITEM_005 │2024-01-14  │2024-01-14   │2024-01-14           │2024-01-14 20:30:00      │1299.00               │1299.00       │1            │1299.00      │1            │APROVADA     │INTERNACIONAL│PART_005     │PART_ITEM_005        │2024-01-15 11:00:00   │                        │
│2024-01-15    │PURCH_001    │BUYER_001 │PROD_001     │PROD_001   │PROD_ITEM_001 │2024-01-10  │2024-01-12   │2024-01-15           │2024-01-15 10:30:00      │299.90                │299.90        │1            │299.90       │1            │APROVADA     │INTERNACIONAL│PART_001     │PART_ITEM_001        │2024-01-16 11:00:00   │                        │
│2024-01-15    │PURCH_002    │BUYER_002 │PROD_002     │PROD_002   │PROD_ITEM_002 │2024-01-11  │2024-01-13   │2024-01-15           │2024-01-15 14:20:00      │189.50                │189.50        │1            │0.0          │0            │CANCELADA    │NACIONAL     │PART_002     │PART_ITEM_002        │2024-01-16 11:00:00   │                        │
│2024-01-15    │PURCH_006    │BUYER_006 │PROD_006     │PROD_006   │PROD_ITEM_006 │2024-01-15  │2024-01-15   │2024-01-15           │2024-01-15 12:00:00      │89.90                 │89.90         │1            │89.90        │1            │APROVADA     │NACIONAL     │PART_006     │PART_ITEM_006        │2024-01-16 11:00:00   │                        │
│2024-01-15    │PURCH_011    │BUYER_011 │PROD_011     │PROD_011   │PROD_ITEM_011 │2024-01-15  │2024-01-15   │2024-01-15           │2024-01-15 10:00:00      │399.90                │399.90        │1            │399.90       │1            │APROVADA     │NACIONAL     │PART_011     │PART_ITEM_011        │2024-01-16 11:00:00   │                        │
│2024-01-15    │PURCH_012    │BUYER_012 │PROD_012     │PROD_012   │PROD_ITEM_012 │2024-01-15  │2024-01-15   │2024-01-15           │2024-01-15 14:30:00      │599.00                │599.00        │1            │599.00       │1            │APROVADA     │INTERNACIONAL│PART_012     │PART_ITEM_012        │2024-01-16 11:00:00   │                        │
│2024-01-16    │PURCH_002    │BUYER_002 │PROD_002     │PROD_002   │PROD_ITEM_002 │2024-01-11  │2024-01-13   │2024-01-15           │2024-01-15 14:20:00      │189.50                │189.50        │1            │0.0          │0            │CANCELADA    │NACIONAL     │PART_002     │PART_ITEM_002        │2024-01-17 11:00:00   │                        │
│2024-01-16    │PURCH_004    │BUYER_004 │PROD_004     │PROD_004   │PROD_ITEM_004 │2024-01-13  │2024-01-14   │2024-01-16           │2024-01-16 09:15:00      │149.90                │149.90        │1            │0.0          │0            │CANCELADA    │NACIONAL     │PART_004     │PART_ITEM_004        │2024-01-17 11:00:00   │                        │
│2024-01-16    │PURCH_006    │BUYER_006 │PROD_006     │PROD_006   │PROD_ITEM_006 │2024-01-15  │2024-01-15   │2024-01-15           │2024-01-15 12:00:00      │89.90                 │89.90         │1            │89.90        │1            │APROVADA     │NACIONAL     │PART_006     │PART_ITEM_006        │2024-01-17 11:00:00   │                        │
│2024-01-16    │PURCH_013    │BUYER_013 │PROD_013     │PROD_013   │PROD_ITEM_013 │2024-01-16  │2024-01-16   │2024-01-16           │2024-01-16 16:20:00      │249.90                │249.90        │1            │0.0          │0            │CANCELADA    │NACIONAL     │PART_013     │PART_ITEM_013        │2024-01-17 11:00:00   │                        │
│2024-01-16    │PURCH_014    │BUYER_014 │PROD_014     │PROD_014   │PROD_ITEM_014 │2024-01-16  │2024-01-16   │2024-01-16           │2024-01-16 18:45:00      │799.90                │799.90        │1            │799.90       │1            │APROVADA     │INTERNACIONAL│PART_014     │PART_ITEM_014        │2024-01-17 11:00:00   │                        │
└──────────────┴─────────────┴──────────┴─────────────┴───────────┴──────────────┴────────────┴─────────────┴─────────────────────┴─────────────────────────┴─────────────────────┴───────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────────────┴─────────────────────┴─────────────────────────┘

LEGENDA DOS DADOS:
- reference_date: Data de referência do snapshot
- purchase_id: Identificador único da compra
- buyer_id: Identificador do comprador
- producer_id: Identificador do produtor
- product_id: Identificador do produto
- prod_item_id: Identificador do item de produto
- order_date: Data do pedido
- release_date: Data de liberação (NULL = não liberado)
- purchase_transaction_date: Data da transação
- purchase_transaction_datetime: Data e hora da transação
- purchase_total_value: Valor total da compra
- purchase_value: Valor do item de produto
- item_quantity: Quantidade de itens
- gmv_value: Valor do GMV calculado
- is_gmv_valid: Flag de validade (1=liberada e não cancelada, 0=outros)
- purchase_status: Status da compra (INICIADA, APROVADA, CANCELADA, REEMBOLSADA)
- subsidiary: Subsidiária (NACIONAL, INTERNACIONAL)
- purchase_partition: Partição da tabela purchase
- prod_item_partition: Partição da tabela product_item
- etl_processed_at: Data/hora do processamento ETL

EXEMPLOS DE CENÁRIOS:
- PURCH_001: Aparece em 2 dias (14, 15/01)
  → MODIFICAÇÃO: subsidiary mudou de NACIONAL para INTERNACIONAL (15/01)
  → MOTIVO: Atualização de evento alterou classificação da subsidiária
  
- PURCH_002: Aparece em 2 dias (14, 15/01)
  → MODIFICAÇÃO: purchase_total_value mudou de 199.50 para 189.50 (15/01)
  → MOTIVO: Atualização de evento alterou valor da compra
  
- PURCH_004: Aparece em 2 dias (14, 16/01)
  → MODIFICAÇÃO: purchase_status mudou de REEMBOLSADA para CANCELADA (16/01)
  → MOTIVO: Atualização de evento alterou status da compra
  
- PURCH_006: Aparece em 1 dia (15/01)
  → MODIFICAÇÃO: purchase_status mudou de INICIADA para APROVADA (15/01)
  → MOTIVO: Evento de liberação da compra alterou status e GMV
  
- PURCH_011: Aparece em 1 dia (15/01)
  → MODIFICAÇÃO: Nova compra criada
  → MOTIVO: Compra não existia nos dias anteriores
  
- PURCH_012: Aparece em 1 dia (15/01)
  → MODIFICAÇÃO: Nova compra criada
  → MOTIVO: Compra não existia nos dias anteriores
  
- PURCH_013: Aparece em 1 dia (16/01)
  → MODIFICAÇÃO: Nova compra criada
  → MOTIVO: Compra não existia nos dias anteriores
  
- PURCH_014: Aparece em 1 dia (16/01)
  → MODIFICAÇÃO: Nova compra criada
  → MOTIVO: Compra não existia nos dias anteriores

