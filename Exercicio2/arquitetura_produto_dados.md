# Arquitetura do Produto de Dados - Purchase History

## Visão Geral

Este documento descreve a arquitetura do produto de dados **Purchase History** que centraliza e organiza todos os dados relacionados a compras, transformando tabelas de eventos em múltiplas camadas de dados para diferentes necessidades de negócio.

## Fluxo de Dados

```
Tabelas de Eventos → ETLs → Camadas de Dados → Consumo
```

### Estrutura do Produto
```
Purchase History
├── Camada Raw (Event Tables)
├── Camada Processed (Screenshot)
├── Camada Metrics (Agregações)
└── Camada Analytics (Dashboards/Relatórios)
```

## 1. Camada Raw - Tabelas de Eventos

### Tabelas de Eventos CDC
- **`purchase`**: Eventos de compra
- **`product_item`**: Eventos de itens de produto
- **`purchase_extra_info`**: Informações extras de compra

**Características:**
- Dados históricos e imutáveis
- Atualizações assíncronas
- Particionamento por data
- Múltiplas versões para o mesmo `purchase_id`
- Fonte única de verdade para dados de compra

## 2. Camada de Processamento (ETLs)

### ETL Principal - Purchase History Screenshot
**Arquivo:** `purchase_history_screenshot.py`

**Processo:**
1. **Extração**: Coleta dados das 3 tabelas de eventos
2. **Transformação**: 
   - Cria tabela base com `(reference_date, purchase_id)`
   - Busca última transação até cada `reference_date`
   - Enriquece dados com informações mais atuais
   - Aplica regras de qualidade de dados
3. **Carga**: Salva na tabela `purchase_history_screenshot`

**Características:**
- Processamento histórico e incremental
- Imutabilidade dos dados históricos
- Rastreabilidade por `reference_date`
- Validação de qualidade de dados

### ETLs Derivadas (Conceituais)
- **ETL Daily Metrics**: Gera métricas diárias por subsidiária *(planejado)*
- **ETL Accounting Metrics**: Gera métricas contábeis mensais *(planejado)*
- **ETL Product Metrics**: Gera métricas de performance de produtos *(planejado)*
- **ETL Customer Analytics**: Gera métricas de comportamento do cliente *(planejado)*
- **ETL Revenue Analytics**: Gera métricas de receita e faturamento *(planejado)*

**Status Atual:** Apenas o ETL principal `purchase_history_screenshot.py` está implementado.

## 3. Camada Processed - Tabelas de Dados Processados

### Tabela Principal: `purchase_history_screenshot`

**Características:**
- Particionamento por `reference_date`
- Dados históricos imutáveis
- Snapshot diário do estado das compras
- Suporte a consultas temporais
- Base para todas as análises de compra
- Contém todos os dados enriquecidos de compra

## 4. Camada Metrics - Tabelas de Métricas

### 4.1 Tabela de Métricas Diárias

**Nome:** `daily_metrics_subsidiary`

**Propósito:** Agregações diárias para dashboards e relatórios operacionais

**Métricas Calculadas:**
- GMV diário por subsidiária
- Taxa de sucesso de compras
- Número de compradores únicos
- Número de produtores únicos
- Número de produtos únicos
- Valores médios, mínimos e máximos

### 4.2 Tabela de Métricas Contábeis

**Nome:** `accounting_metrics_monthly`

**Propósito:** Fechamento contábil mensal com dados acumulados

**Métricas Calculadas:**
- GMV mensal por subsidiária
- Compras únicas (última ocorrência por `purchase_id`)
- Métricas acumuladas YTD
- Valores para fechamento contábil
- Taxa de sucesso mensal

### 4.3 Tabela de Métricas de Produto

**Nome:** `product_metrics_daily`

**Propósito:** Análise de performance de produtos

**Métricas Calculadas:**
- GMV por produto
- Número de itens vendidos
- Compradores únicos por produto
- Taxa de conversão
- Performance por produtor

### 4.4 Tabela de Métricas de Cliente

**Nome:** `customer_metrics_daily`

**Propósito:** Análise de comportamento e valor do cliente

**Métricas Calculadas:**
- Total de compras por cliente
- Valor total gasto
- Valor médio por compra
- Produtos únicos comprados
- Produtores únicos
- Dias desde primeira/última compra
- Customer Lifetime Value (CLV)

### 4.5 Tabela de Métricas de Receita

**Nome:** `revenue_metrics_daily`

**Propósito:** Análise de receita e faturamento

**Métricas Calculadas:**
- Receita total, líquida e bruta
- Valor de reembolsos
- Valor de cancelamentos
- Receita por cliente
- Receita por produto

## 5. Desenho da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           PURCHASE HISTORY - ARQUITETURA                       │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   purchase      │    │  product_item   │    │purchase_extra_  │
│   (CDC Events)  │    │  (CDC Events)   │    │     info        │
│                 │    │                 │    │  (CDC Events)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   ETL PRINCIPAL │
                    │purchase_history_│
                    │   screenshot.py │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │purchase_history_│
                    │   screenshot    │
                    │   (Base Table)  │
                    └─────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ETL DAILY     │    │   ETL ACCOUNTING│    │   ETL PRODUCT   │
│   METRICS       │    │   METRICS       │    │   METRICS       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│daily_metrics_   │    │accounting_      │    │product_metrics_ │
│subsidiary       │    │metrics_monthly  │    │daily            │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   DASHBOARDS    │
                    │   & REPORTS     │
                    │                 │
                    │ • GMV Diário    │
                    │ • Fechamento    │
                    │ • Top Produtos  │
                    │ • Clientes      │
                    │ • Receita       │
                    └─────────────────┘
```

## 6. Camada Analytics - Dashboards e Relatórios

### 6.1 Dashboards Operacionais
- **GMV Diário**: Monitoramento de performance por subsidiária
- **Top Produtos**: Ranking de produtos por GMV
- **Performance de Clientes**: Análise de comportamento do cliente
- **Receita**: Acompanhamento de faturamento

### 6.2 Relatórios Contábeis
- **Fechamento Mensal**: Relatórios para contabilidade
- **Auditoria**: Validação de dados para auditoria
- **Compliance**: Relatórios regulatórios

### 6.3 Análises de Negócio
- **Tendências**: Análise de tendências de compra
- **Segmentação**: Análise de segmentos de cliente
- **Previsão**: Modelos preditivos de vendas

## 7. Fluxo de Processamento

### 7.1 ETL Principal (Diário)
```
Event Tables → ETL Purchase History → purchase_history_screenshot
```

### 7.2 ETLs Derivadas (Diário)
```
purchase_history_screenshot → ETL Daily Metrics → daily_metrics_subsidiary
purchase_history_screenshot → ETL Product Metrics → product_metrics_daily
purchase_history_screenshot → ETL Customer Metrics → customer_metrics_daily
purchase_history_screenshot → ETL Revenue Metrics → revenue_metrics_daily
```


