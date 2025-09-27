-- =====================================================
-- DDL para Tabela Purchase History Screenshot
-- Desafio Técnico Analytics Engineer - Hotmart
-- Autor: João Vitor
-- Data: 2025
-- =====================================================

-- Criação da tabela final do ETL de snapshot histórico de compras
CREATE TABLE IF NOT EXISTS purchase_history_screenshot (
    -- Identificadores e datas
    reference_date DATE NOT NULL COMMENT 'Data de referência para o cálculo do GMV',
    purchase_id STRING NOT NULL COMMENT 'Identificador único da compra',
    buyer_id STRING COMMENT 'Identificador do comprador',
    producer_id STRING COMMENT 'Identificador do produtor',
    product_id STRING COMMENT 'Identificador do produto',
    prod_item_id STRING COMMENT 'Identificador do item do produto',
    
    -- Datas da transação
    order_date DATE COMMENT 'Data do pedido',
    release_date DATE COMMENT 'Data de liberação do pagamento',
    purchase_transaction_date DATE COMMENT 'Data da transação de compra',
    purchase_transaction_datetime TIMESTAMP COMMENT 'Data e hora da transação de compra',
    
    -- Partições
    purchase_partition BIGINT COMMENT 'Partição da tabela purchase',
    prod_item_partition BIGINT COMMENT 'Partição da tabela product_item',
    
    -- Valores monetários
    purchase_total_value DOUBLE COMMENT 'Valor total da compra',
    purchase_value DOUBLE COMMENT 'Valor do item da compra',
    gmv_value DOUBLE COMMENT 'Valor do GMV (Gross Merchandising Value)',
    
    -- Quantidades e status
    item_quantity INT COMMENT 'Quantidade de itens',
    purchase_status STRING COMMENT 'Status da compra (INICIADA, APROVADA, CANCELADA, REEMBOLSADA)',
    subsidiary STRING COMMENT 'Subsidiária responsável (NACIONAL, INTERNACIONAL)',
    
    -- Flags e metadados
    is_gmv_valid INT COMMENT 'Flag indicando se o GMV é válido (1=liberada e não cancelada, 0=outros)',
    etl_processed_at TIMESTAMP COMMENT 'Timestamp de processamento do ETL'
)
COMMENT 'Tabela histórica com snapshot de compras para cálculo de GMV'
PARTITIONED BY (
    reference_date
)
STORED AS PARQUET
LOCATION '/data/purchase_history_screenshot/'
-- Índices para otimização de consultas
CREATE INDEX IF NOT EXISTS idx_purchase_subsidiary 
ON TABLE purchase_history_screenshot (subsidiary) 
AS 'COMPACT' 
WITH DEFERRED REBUILD;

CREATE INDEX IF NOT EXISTS idx_purchase_id 
ON TABLE purchase_history_screenshot (purchase_id) 
AS 'COMPACT' 
WITH DEFERRED REBUILD;

