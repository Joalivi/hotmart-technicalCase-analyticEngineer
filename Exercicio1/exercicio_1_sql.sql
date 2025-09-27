-- =============================================================================
-- EXERCÍCIO 1 - SQL - DESAFIO TÉCNICO HOTMART
-- =============================================================================
-- 
-- =============================================================================
-- PERGUNTA 1: 50 MAIORES PRODUTORES EM FATURAMENTO DE 2021
-- =============================================================================
-- 
-- LÓGICA:
-- 1. JOIN entre purchase e product_item para obter dados completos
-- 2. Filtrar por ano 2021 usando order_date
-- 3. Filtrar apenas compras pagas (release_date não nulo)
-- 4. Filtrar apenas compras aprovadas (purchase_status = 'APROVADA')
-- 5. Agrupar por producer_id
-- 6. Somar purchase_value (valor do item de compra)
-- 7. Ordenar por faturamento decrescente
-- 8. Limitar a 50 registros
-- =============================================================================

WITH faturamento_produtores_2021 AS (
    SELECT 
        p.producer_id,
        SUM(pi.purchase_value) AS faturamento_total
    FROM purchase p
    INNER JOIN product_item pi 
        ON p.prod_item_id = pi.prod_item_id
    WHERE 
        -- Filtrar por ano 2021
        YEAR(p.order_date) = 2021
        -- Apenas compras pagas 
        AND p.release_date IS NOT NULL
        -- Apenas compras aprovadas
        AND p.purchase_status = 'APROVADA'
    GROUP BY p.producer_id
)
SELECT 
    producer_id,
    faturamento_total
FROM faturamento_produtores_2021
ORDER BY faturamento_total DESC
LIMIT 50;

-- =============================================================================
-- PERGUNTA 2: 2 PRODUTOS QUE MAIS FATURARAM DE CADA PRODUTOR
-- =============================================================================
-- 
-- LÓGICA:
-- 1. JOIN entre purchase e product_item para obter dados completos
-- 2. Aplicar mesmos filtros da pergunta 1 (2021, pagas, aprovadas)
-- 3. Agrupar por producer_id e product_id
-- 4. Somar purchase_value para calcular faturamento por produto
-- 5. Usar ROW_NUMBER() para classificar produtos por produtor
-- 6. Filtrar apenas os 2 primeiros (rank <= 2)
-- 
-- OBSERVAÇÕES:
-- - Usar ROW_NUMBER() em vez de RANK() para evitar empates
-- - Window function particionada por producer_id
-- - Ordenação por faturamento decrescente
-- 
-- =============================================================================

WITH faturamento_produtos AS (
    SELECT 
        p.producer_id,
        pi.product_id,
        SUM(pi.purchase_value) AS faturamento_produto
    FROM purchase p
    INNER JOIN product_item pi 
        ON p.prod_item_id = pi.prod_item_id
    WHERE 
        -- Apenas compras pagas 
        AND p.release_date IS NOT NULL
        -- Apenas compras aprovadas
        AND p.purchase_status = 'APROVADA'
    GROUP BY p.producer_id, pi.product_id
),
ranking_produtos AS (
    SELECT 
        producer_id,
        product_id,
        faturamento_produto,
        ROW_NUMBER() OVER (
            PARTITION BY producer_id 
            ORDER BY faturamento_produto DESC
        ) AS rank_produto
    FROM faturamento_produtos
)
SELECT 
    producer_id,
    product_id,
    faturamento_produto,
    rank_produto
FROM ranking_produtos
WHERE rank_produto <= 2 
ORDER BY producer_id, rank_produto;