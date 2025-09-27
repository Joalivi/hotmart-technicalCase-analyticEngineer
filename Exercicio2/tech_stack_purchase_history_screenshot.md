# Tech Stack - ETL Purchase History Screenshot

**Desafio Técnico Analytics Engineer - Hotmart**  
**Autor:** João Vitor  
**Data:** 2025

## Tecnologias Utilizadas

### **Apache Spark**
- **Linguagem:** PySpark (Python)
- **Versão:** 3.4+
- **Propósito:** Engine de processamento distribuído para ETL

### **Python**
- **Versão:** 3.8+
- **Bibliotecas:**
  - `pyspark.sql` - Manipulação de DataFrames
  - `pyspark.sql.functions` - Funções SQL
  - `pyspark.sql.window` - Window Functions
  - `datetime` - Manipulação de datas
  - `logging` - Sistema de logs

### **DataFrames Spark**
- **API:** PySpark DataFrame API
- **Operações:** Transformações e ações
- **Window Functions:** Para busca da última transação
- **Joins:** Para enriquecimento de dados

## Estrutura de Dados

### **Tabelas de Origem**
```sql
purchase              -- Transações de compra
product_item          -- Itens de produto  
purchase_extra_info   -- Informações extras da compra
```

### **Tabela de Destino**
```sql
purchase_history_screenshot
```

## Funcionalidades Implementadas

### **ETL Pipeline**
- Carregamento de tabelas via `spark.table()`
- Filtros por período de datas
- Joins para enriquecimento
- Window Functions para última transação
- Cast de tipos de dados
- Validação de qualidade

### **Processamento**
- **Modo Incremental:** D-1 (ontem)
- **Modo Histórico:** Período específico
- **Particionamento:** Por `reference_date`

### **Qualidade de Dados**
- Tratamento de valores nulos
- Validação de completude
- Verificação de consistência
- Métricas de GMV
- Validação de status (liberada e não cancelada para GMV válido)

## Arquitetura

```
Tabelas CDC → ETL PySpark → Tabela Final
     ↓              ↓            ↓
  purchase    Transformações   purchase_history
product_item     Validações   _screenshot
purchase_extra   Cast Types   (Parquet)
```

## Resumo

A solução utiliza **Apache Spark** com **PySpark** para processar dados CDC de 3 tabelas de origem, aplicando transformações, validações e enriquecimento para gerar a tabela final `purchase_history_screenshot` com snapshot histórico de compras para cálculo de GMV.
