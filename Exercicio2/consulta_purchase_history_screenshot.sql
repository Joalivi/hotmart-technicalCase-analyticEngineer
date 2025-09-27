-- =====================================================
-- Consulta SQL - Purchase History Screenshot
-- Desafio Técnico Analytics Engineer - Hotmart
-- Autor: João Vitor
-- Data: 2025
-- =====================================================

-- =====================================================
-- CONSULTA 1: GMV Diário por Subsidiária
-- =====================================================

-- Consulta que retorna o GMV diário por subsidiária
-- Considera apenas compras válidas (liberadas e não canceladas)
SELECT 
    reference_date AS data_referencia,
    subsidiary AS subsidiaria,
    COUNT(*) AS total_compras,
    SUM(gmv_value) AS gmv_total,
    AVG(gmv_value) AS gmv_medio,
    MIN(gmv_value) AS gmv_minimo,
    MAX(gmv_value) AS gmv_maximo,
    SUM(item_quantity) AS total_itens_vendidos
FROM purchase_history_screenshot
WHERE is_gmv_valid = 1  -- Apenas compras liberadas e não canceladas
GROUP BY reference_date, subsidiary
ORDER BY reference_date DESC, gmv_total DESC;

-- =====================================================
-- CONSULTA 2: Fechamento Mensal para Contabilidade
-- =====================================================

-- Consulta para fechamento mensal considerando apenas a última ocorrência de cada purchase_id
-- Utiliza Window Function para encontrar a data mais recente por purchase_id e subsidiária
WITH ultima_ocorrencia_mes AS (
    SELECT 
        purchase_id,
        reference_date,
        subsidiary,
        gmv_value,
        is_gmv_valid,
        purchase_status,
        purchase_total_value,
        ROW_NUMBER() OVER (
            PARTITION BY purchase_id, subsidiary 
            ORDER BY reference_date DESC
        ) AS rn
    FROM purchase_history_screenshot
    WHERE YEAR(reference_date) = 2024  -- Ano de referência
      AND MONTH(reference_date) = 1    -- Mês de referência (Janeiro)
)
SELECT 
    reference_date AS data_referencia,
    subsidiary AS subsidiaria,
    COUNT(*) AS total_compras_unicas,
    SUM(CASE WHEN is_gmv_valid = 1 THEN 1 ELSE 0 END) AS compras_validas,
    SUM(CASE WHEN is_gmv_valid = 0 THEN 1 ELSE 0 END) AS compras_invalidas,
    SUM(CASE WHEN is_gmv_valid = 1 THEN gmv_value ELSE 0 END) AS gmv_total_mes,
    AVG(CASE WHEN is_gmv_valid = 1 THEN gmv_value ELSE NULL END) AS gmv_medio_mes,
    SUM(purchase_total_value) AS valor_total_compras
FROM ultima_ocorrencia_mes
WHERE rn = 1  -- Apenas a última ocorrência de cada purchase_id por subsidiária
GROUP BY reference_date, subsidiary
ORDER BY reference_date DESC, gmv_total_mes DESC;

-- =====================================================
-- RACIONAL DAS CONSULTAS
-- =====================================================

/*
CONSULTA 1 - GMV Diário por Subsidiária:
- Propósito: Análise operacional diária do GMV por subsidiária
- Lógica: Agrega todas as compras válidas por data e subsidiária
- Uso: Dashboards, relatórios operacionais, acompanhamento de performance
- Considera: Todas as ocorrências de purchase_id na data

CONSULTA 2 - Fechamento Mensal para Contabilidade:
- Propósito: Fechamento contábil mensal com dados mais atualizados por subsidiária
- Lógica: Considera apenas a última ocorrência de cada purchase_id por subsidiária no mês
- Uso: Relatórios contábeis, fechamento mensal, auditoria por subsidiária
- Considera: Apenas a versão mais recente de cada compra por subsidiária (snapshot final)

DIFERENÇA FUNDAMENTAL:
- Consulta 1: Análise temporal (todas as ocorrências por data)
- Consulta 2: Análise de estado final (última versão de cada compra)

A Consulta 2 é essencial para contabilidade porque:
1. Evita duplicação de compras que podem ter múltiplas atualizações
2. Considera o estado final mais atualizado de cada transação
3. Fornece dados consistentes para fechamento contábil
4. Respeita o conceito de "screenshot" - captura o estado em um momento específico
*/
