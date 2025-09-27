"""
ETL para Purchase History Screenshot
Desafio Técnico Analytics Engineer - Hotmart

OBJETIVO:
Processar eventos CDC (Change Data Capture) das tabelas purchase, product_item e purchase_extra_info
para criar uma tabela histórica imutável com snapshot de compras que possibilita o cálculo de GMV diário por subsidiária.

FUNCIONAMENTO:
1. Extrai combinações únicas de (transaction_date, purchase_id) das 3 tabelas de eventos
2. Para cada combinação, busca a última transação válida até aquela data
3. Calcula o GMV considerando apenas compras pagas e não canceladas
4. Gera tabela histórica com rastreabilidade temporal completa

AUTOR: João Vitor
DATA: 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, when, sum as spark_sum, current_timestamp
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def handle_null_values(df, table_name):
    """
    Trata valores nulos nos DataFrames de acordo com regras específicas.
    
    TRATAMENTOS APLICADOS:
    - purchase_id: Remove registros com purchase_id nulo
    - transaction_date: Remove registros com transaction_date nulo
    - purchase_total_value: Substitui nulos por 0.0, negativos por 0.0
    - subsidiary: Substitui nulos por 'UNKNOWN'
    - purchase_value: Substitui nulos por 0.0, negativos por 0.0
    - item_quantity: Substitui nulos por 0, negativos por 0
    
    Args:
        df (DataFrame): DataFrame para tratamento
        table_name (str): Nome da tabela para logging
        
    Returns:
        DataFrame: DataFrame com valores nulos tratados
    """
    logger.info(f"🔧 Tratando valores nulos na tabela: {table_name}")
    
    original_count = df.count()
    treated_df = df
    
    # Tratamento específico por tabela
    if table_name == "purchase":
        # Remove registros com purchase_id nulo
        treated_df = treated_df.filter(col("purchase_id").isNotNull())
        
        # Remove registros com transaction_date nulo
        treated_df = treated_df.filter(col("transaction_date").isNotNull())
        
        # Trata purchase_total_value: nulos e negativos viram 0.0
        treated_df = treated_df.withColumn(
            "purchase_total_value",
            when(col("purchase_total_value").isNull() | (col("purchase_total_value") < 0), 0.0)
            .otherwise(col("purchase_total_value"))
        )
        
    elif table_name == "purchase_extra_info":
        # Remove registros com purchase_id nulo
        treated_df = treated_df.filter(col("purchase_id").isNotNull())
        
        # Remove registros com transaction_date nulo
        treated_df = treated_df.filter(col("transaction_date").isNotNull())
        
        # Substitui subsidiary nulo por 'UNKNOWN'
        treated_df = treated_df.withColumn(
            "subsidiary",
            when(col("subsidiary").isNull(), "UNKNOWN")
            .otherwise(col("subsidiary"))
        )
        
    elif table_name == "product_item":
        # Remove registros com transaction_date nulo
        treated_df = treated_df.filter(col("transaction_date").isNotNull())
        
        # Trata purchase_value: nulos e negativos viram 0.0
        treated_df = treated_df.withColumn(
            "purchase_value",
            when(col("purchase_value").isNull() | (col("purchase_value") < 0), 0.0)
            .otherwise(col("purchase_value"))
        )
        
        # Trata item_quantity: nulos e negativos viram 0
        treated_df = treated_df.withColumn(
            "item_quantity",
            when(col("item_quantity").isNull() | (col("item_quantity") < 0), 0)
            .otherwise(col("item_quantity"))
        )
    
    final_count = treated_df.count()
    removed_records = original_count - final_count
    
    if removed_records > 0:
        logger.warning(f"   ⚠️  Removidos {removed_records:,} registros com dados nulos")
    else:
        logger.info(f"   ✅ Nenhum registro removido por dados nulos")
    
    return treated_df


def cast_dataframe_columns(df):
    """
    Faz o cast dos tipos de dados das colunas do DataFrame final.
    
    TIPOS APLICADOS:
    - reference_date, order_date, release_date, purchase_transaction_date: date
    - purchase_id, buyer_id, producer_id, product_id, prod_item_id: string
    - purchase_total_value, purchase_value, gmv_value: double
    - item_quantity, is_gmv_valid: int
    - purchase_status, subsidiary: string
    - purchase_partition, prod_item_partition: bigint
    - purchase_transaction_datetime, etl_processed_at: timestamp
    
    Args:
        df (DataFrame): DataFrame para cast de tipos
        
    Returns:
        DataFrame: DataFrame com tipos de dados corretos
    """
    logger.info("🔧 Aplicando cast de tipos de dados no DataFrame final")
    
    try:
         casted_df = df \
        .withColumn("reference_date", col("reference_date").astype("date")) \
        .withColumn("order_date", col("order_date").astype("date")) \
        .withColumn("release_date", col("release_date").astype("date")) \
        .withColumn("purchase_transaction_date", col("purchase_transaction_date").astype("date")) \
        .withColumn("purchase_id", col("purchase_id").astype("string")) \
        .withColumn("buyer_id", col("buyer_id").astype("string")) \
        .withColumn("producer_id", col("producer_id").astype("string")) \
        .withColumn("product_id", col("product_id").astype("string")) \
        .withColumn("prod_item_id", col("prod_item_id").astype("string")) \
        .withColumn("purchase_total_value", col("purchase_total_value").astype("double")) \
        .withColumn("purchase_value", col("purchase_value").astype("double")) \
        .withColumn("gmv_value", col("gmv_value").astype("double")) \
        .withColumn("item_quantity", col("item_quantity").astype("int")) \
        .withColumn("is_gmv_valid", col("is_gmv_valid").astype("int")) \
        .withColumn("purchase_status", col("purchase_status").astype("string")) \
        .withColumn("subsidiary", col("subsidiary").astype("string")) \
        .withColumn("purchase_partition", col("purchase_partition").astype("bigint")) \
        .withColumn("prod_item_partition", col("prod_item_partition").astype("bigint")) \
        .withColumn("purchase_transaction_datetime", col("purchase_transaction_datetime").astype("timestamp")) \
        .withColumn("etl_processed_at", col("etl_processed_at").astype("timestamp"))
    except Exception as e:
        logger.error(f"Erro ao fazer cast de tipos: {str(e)}")
        raise
    
    logger.info("   ✅ Cast de tipos aplicado: date, string, double, int, timestamp")
    
    logger.info("🎉 Cast de tipos de dados concluído!")
    return casted_df


def validate_and_cast_final_dataframe(df):
    """
    Valida qualidade e aplica cast de tipos no DataFrame final do ETL.
    
    VALIDAÇÕES REALIZADAS:
    - Verifica se o DataFrame não está vazio
    - Calcula métricas de qualidade (completeness, accuracy, consistency)
    - Aplica cast de tipos de dados
    - Valida valores de GMV
    
    Args:
        df (DataFrame): DataFrame final para validação
        
    Returns:
        DataFrame: DataFrame validado e com tipos corretos
    """
    logger.info("🔍 Validando qualidade e aplicando cast no DataFrame final do ETL")
    
    # VALIDAÇÃO 1: DataFrame não vazio
    total_records = df.count()
    if total_records == 0:
        logger.error("❌ ERRO: DataFrame final está vazio!")
        raise ValueError("DataFrame final está vazio")
    else:
        logger.info(f"✅ DataFrame contém {total_records:,} registros")
    
    # VALIDAÇÃO 2: Métricas de qualidade
    logger.info("📊 === MÉTRICAS DE QUALIDADE ===")
    
    # COMPLETENESS: Verifica valores nulos por coluna
    completeness_issues = []
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        completeness_pct = ((total_records - null_count) / total_records * 100) if total_records > 0 else 0
        
        if completeness_pct < 95:  # Alerta para completude baixa
            logger.warning(f"   ⚠️  {column}: {completeness_pct:.1f}% completo ({null_count} nulos)")
            completeness_issues.append(column)
        else:
            logger.info(f"   ✅ {column}: {completeness_pct:.1f}% completo")
    
    # ACCURACY: Validações específicas do GMV
    negative_gmv = df.filter(col("gmv_value") < 0).count()
    if negative_gmv > 0:
        logger.warning(f"   ⚠️  {negative_gmv} registros com GMV negativo")
    
    # CONSISTENCY: Verifica consistência temporal
    if "purchase_transaction_date" in df.columns and "purchase_transaction_datetime" in df.columns:
        inconsistent_dates = df.filter(
            col("purchase_transaction_date") != col("purchase_transaction_datetime").cast("date")
        ).count()
        if inconsistent_dates > 0:
            logger.warning(f"   ⚠️  {inconsistent_dates} registros com datas inconsistentes")
    
    # VALIDAÇÃO 3: Aplica cast de tipos de dados
    df_casted = cast_dataframe_columns(df)
    
    # VALIDAÇÃO 4: Valores de GMV válidos
    try:
        valid_gmv_count = df_casted.filter(col("is_gmv_valid") == 1).count()
        total_gmv_value = df_casted.agg(spark_sum("gmv_value")).collect()[0][0] or 0
        
        logger.info(f"📊 === VALIDAÇÃO GMV ===")
        logger.info(f"   Registros com GMV válido: {valid_gmv_count:,}")
        logger.info(f"   Valor total do GMV: R$ {total_gmv_value:,.2f}")
        
        # Evita divisão por zero
        if total_records > 0:
            gmv_percentage = (valid_gmv_count/total_records*100)
            logger.info(f"   Percentual de GMV válido: {gmv_percentage:.1f}%")
        else:
            logger.warning("   ⚠️  Não foi possível calcular percentual de GMV (total_records = 0)")
    except Exception as e:
        logger.error(f"Erro ao calcular métricas de GMV: {str(e)}")
        raise
    
    # Resumo final
    if completeness_issues:
        logger.warning(f"⚠️  {len(completeness_issues)} colunas com completude baixa: {completeness_issues}")
    else:
        logger.info("🎉 Todas as colunas têm completude adequada!")
    
    logger.info("🎉 Validação e cast do DataFrame final concluídos!")
    return df_casted




def create_base_reference_table(start_date, end_date=None):
    """
    Constrói a tabela base com todas as combinações únicas de (transaction_date, purchase_id).
    
    PROCESSO:
    1. Carrega as 3 tabelas de eventos: purchase, product_item, purchase_extra_info
    2. Filtra registros pelo período especificado (start_date até end_date)
    3. Extrai pares (transaction_date, purchase_id) de cada tabela
    4. Faz UNION de todas as combinações e remove duplicatas
    5. Ordena por data e purchase_id para processamento sequencial
    
    IMPORTANTE: Para product_item, faz JOIN com purchase para obter purchase_id,
    pois a tabela product_item não possui purchase_id diretamente.
    
    Args:
        start_date (str): Data inicial do período (formato: 'YYYY-MM-DD')
        end_date (str, optional): Data final do período. Se None, usa ontem
        
    Returns:
        DataFrame: Tabela base com colunas [reference_date, purchase_id]
                  Cada linha representa uma combinação única de transaction_date e purchase_id
    """
    logger.info(f"Construindo tabela base para período: {start_date} até {end_date}")
    
    # Define data final padrão se não fornecida
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # PASSO 1: Carrega as tabelas de eventos
    purchase_df = spark.table("purchase")
    product_item_df = spark.table("product_item")
    purchase_extra_info_df = spark.table("purchase_extra_info")
    
    # PASSO 1.1: Trata valores nulos nas tabelas de origem
    purchase_df = handle_null_values(purchase_df, "purchase")
    product_item_df = handle_null_values(product_item_df, "product_item")
    purchase_extra_info_df = handle_null_values(purchase_extra_info_df, "purchase_extra_info")
    
    # PASSO 2: Filtra registros pelo período especificado
    purchase_filtered = purchase_df.filter(
        (col("transaction_date") >= start_date) & 
        (col("transaction_date") <= end_date)
    )
    
    product_item_filtered = product_item_df.filter(
        (col("transaction_date") >= start_date) & 
        (col("transaction_date") <= end_date)
    )
    
    purchase_extra_info_filtered = purchase_extra_info_df.filter(
        (col("transaction_date") >= start_date) & 
        (col("transaction_date") <= end_date)
    )
    
    # PASSO 3: Extrai combinações (data, purchase_id) de cada tabela
    
    # Tabela purchase: tem purchase_id diretamente
    dates_and_purchase_ids_from_purchase = purchase_filtered.select(
        col("transaction_date").alias("reference_date"), 
        col("purchase_id")
    ).distinct()
    
    # Tabela purchase_extra_info: tem purchase_id diretamente
    dates_and_purchase_ids_from_extra_info = purchase_extra_info_filtered.select(
        col("transaction_date").alias("reference_date"), 
        col("purchase_id")
    ).distinct()
    
    # Tabela product_item: precisa JOIN com purchase para obter purchase_id
    # JOIN por prod_item_id + prod_item_partition (chave composta)
    dates_and_purchase_ids_from_product_item = purchase_df \
        .join(product_item_filtered, 
              (purchase_df.prod_item_id == product_item_filtered.prod_item_id) & 
              (purchase_df.prod_item_partition == product_item_filtered.prod_item_partition)) \
        .select(
            product_item_filtered.transaction_date.alias("reference_date"),
            purchase_df.purchase_id
        ).distinct()
    
    # PASSO 4: Combina todas as fontes e remove duplicatas
    base_table_df = dates_and_purchase_ids_from_purchase \
        .union(dates_and_purchase_ids_from_extra_info) \
        .union(dates_and_purchase_ids_from_product_item) \
        .distinct() \
        .orderBy("reference_date", "purchase_id")
    
    
    # Evita .count() desnecessário para melhor performance
    logger.info("Tabela base criada com combinações únicas de reference_date e purchase_id")
    
    return base_table_df


def get_latest_transaction_data_with_base_table(table_name, base_table_df, partition_columns):
    """
    Busca a última transação válida de uma tabela para cada linha da tabela base.
    
    FUNCIONAMENTO:
    1. Carrega a tabela de eventos especificada
    2. Faz JOIN com a tabela base para obter reference_date por linha
    3. Filtra transações que ocorreram até a reference_date de cada linha
    4. Usa Window Function para encontrar a transação mais recente por grupo
    5. Retorna apenas a última transação válida para cada combinação
    
    LÓGICA TEMPORAL:
    - Cada linha da tabela base tem sua própria reference_date
    - Busca transações com transaction_date <= reference_date
    - Dentro do grupo, seleciona a transação com transaction_datetime mais recente
    
    Args:
        table_name (str): Nome da tabela de eventos (ex: 'purchase', 'product_item')
        base_table_df (DataFrame): Tabela base com colunas [reference_date, purchase_id]
        partition_columns (list): Colunas para agrupar (ex: ['purchase_id'])
        
    Returns:
        DataFrame: Última transação válida para cada linha da tabela base
    """
    logger.info(f"Buscando última transação de {table_name} para cada linha da tabela base")
    
    # PASSO 1: Carrega a tabela de eventos
    table_df = spark.table(table_name)
    
    # PASSO 2: JOIN com tabela base para obter reference_date por linha
    base_with_table = base_table_df.join(
        table_df,
        base_table_df.purchase_id == table_df.purchase_id,
        "left"
    )
    
    # PASSO 3: Filtra transações que ocorreram até a reference_date
    # Cada linha usa sua própria reference_date como limite temporal
    filtered_df = base_with_table.filter(
        col("transaction_date") <= col("reference_date")
    )
    
    # PASSO 4: Window Function para encontrar a transação mais recente
    # Agrupa por reference_date + colunas de partição
    # Ordena por transaction_datetime descendente (mais recente primeiro)
    window_columns = ["reference_date"] + partition_columns
    window_spec = Window.partitionBy(*window_columns).orderBy(col("transaction_datetime").desc())
    
    # PASSO 5: Seleciona apenas a primeira linha de cada grupo (mais recente)
    latest_table_df = filtered_df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")
    
    logger.info(f"Última transação de {table_name} processada com sucesso")
    
    return latest_table_df


def enrich_base_table_with_latest_data(base_table_df):
    """
    Enriquece a tabela base com dados mais atuais das 3 tabelas de eventos.
    
    PROCESSO DE ENRIQUECIMENTO:
    1. Busca última transação de purchase para cada linha
    2. Busca última transação de product_item para cada linha  
    3. Busca última transação de purchase_extra_info para cada linha
    4. Faz JOINs sequenciais para combinar todos os dados
    5. Calcula flags de validação e valor do GMV
    6. Adiciona metadados de processamento
    
    LÓGICA DO GMV:
    - GMV válido: compra com release_date preenchida E status diferente de CANCELADA/REEMBOLSADA
    - Valor do GMV: purchase_total_value se válido, senão 0.0
    
    Args:
        base_table_df (DataFrame): Tabela base com [reference_date, purchase_id]
        
    Returns:
        DataFrame: Tabela enriquecida com dados completos para cálculo do GMV
    """
    logger.info("Iniciando enriquecimento da tabela base com dados das 3 tabelas de eventos")
    
    # PASSO 1: Busca última transação de cada tabela para cada linha
    latest_purchase_df = get_latest_transaction_data_with_base_table("purchase", base_table_df, ["purchase_id"])
    latest_product_item_df = get_latest_transaction_data_with_base_table("product_item", base_table_df, ["prod_item_id", "prod_item_partition"])
    latest_purchase_extra_info_df = get_latest_transaction_data_with_base_table("purchase_extra_info", base_table_df, ["purchase_id", "purchase_partition"])
    
    # PASSO 2: Inicia enriquecimento com dados de purchase (base principal)
    enriched_df = latest_purchase_df
    
    # PASSO 3: Enriquece com dados de product_item
    enriched_df = enriched_df.join(
        latest_product_item_df.select(
            "reference_date", "purchase_id", "product_id", "item_quantity", "purchase_value"
        ),
        ["reference_date", "purchase_id"],
        "left"
    )
    
    # PASSO 4: Enriquece com dados de purchase_extra_info (subsidiária)
    enriched_df = enriched_df.join(
        latest_purchase_extra_info_df.select(
            "reference_date", "purchase_id", "subsidiary"
        ),
        ["reference_date", "purchase_id"],
        "left"
    )
    
    # PASSO 5: Calcula flags e valores do GMV
    enriched_df = enriched_df.withColumn(
        "is_gmv_valid",
        when(
            (col("release_date").isNotNull()) & 
            (~col("purchase_status").isin(["CANCELADA", "REEMBOLSADA"])), 
            1
        ).otherwise(0)
    ).withColumn(
        "gmv_value",
        when(
            (col("release_date").isNotNull()) & 
            (~col("purchase_status").isin(["CANCELADA", "REEMBOLSADA"])), 
            col("purchase_total_value")
        ).otherwise(0.0)
    ).withColumn(
        "etl_processed_at",
        current_timestamp()
    )
    
    # PASSO 6: Seleciona e renomeia colunas finais
    enriched_df = enriched_df.select(
        "reference_date",
        "purchase_id",
        "buyer_id",
        "prod_item_id",
        "order_date",
        "release_date", 
        "producer_id",
        "purchase_partition",
        "prod_item_partition",
        "purchase_total_value",
        "purchase_status",
        col("transaction_datetime").alias("purchase_transaction_datetime"),
        col("transaction_date").alias("purchase_transaction_date"),
        "product_id",
        "item_quantity",
        "purchase_value",
        "subsidiary",
        "is_gmv_valid",
        "gmv_value",
        "etl_processed_at"
    )
    
    logger.info("Tabela enriquecida criada com sucesso")
    
    return enriched_df


def main(start_date=None, end_date=None, is_historical=False):
    """
    Função principal para execução do ETL de snapshot histórico de compras.
    
    MODOS DE PROCESSAMENTO:
    1. INCREMENTAL (is_historical=False): Processa apenas dados de ontem (D-1)
    2. HISTÓRICO (is_historical=True): Processa período específico fornecido
    
    FLUXO DE EXECUÇÃO:
    1. Define datas de processamento baseado no modo
    2. Cria tabela base com combinações (data, purchase_id)
    3. Enriquece com dados mais atuais das 3 tabelas de eventos
    4. Calcula GMV válido e valor por linha
    5. Valida qualidade do DataFrame final
    6. Exibe estatísticas e amostra dos resultados
    
    Args:
        start_date (str, optional): Data inicial (formato: 'YYYY-MM-DD')
        end_date (str, optional): Data final (formato: 'YYYY-MM-DD')
        is_historical (bool): True=processamento histórico, False=incremental
        
    Returns:
        DataFrame: Tabela enriquecida com snapshot histórico de compras
    """
    try:
        # PASSO 1: Define datas de processamento baseado no modo
        if is_historical:
            # MODO HISTÓRICO: Processa período específico fornecido
            if start_date is None or end_date is None:
                raise ValueError("Para processamento histórico, start_date e end_date são obrigatórios")
            process_start_date = start_date
            process_end_date = end_date
            logger.info(f"🔄 MODO HISTÓRICO: Processando período {process_start_date} até {process_end_date}")
        else:
            # MODO INCREMENTAL: Processa apenas dados de ontem (D-1)
            process_start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            process_end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            logger.info(f"🔄 MODO INCREMENTAL: Processando dados de {process_start_date}")
        
        # PASSO 2: Cria tabela base com combinações (data, purchase_id)
        base_table_df = create_base_reference_table(process_start_date, process_end_date)
        
        # PASSO 3: Enriquece tabela base com dados mais atuais
        enriched_df = enrich_base_table_with_latest_data(base_table_df)
        
        # PASSO 4: Exibe estatísticas do processamento
        logger.info("📊 === ESTATÍSTICAS DO ETL ===")
        total_records = enriched_df.count()
        valid_gmv_records = enriched_df.filter(col('is_gmv_valid') == 1).count()
        logger.info(f"📈 Total de registros processados: {total_records:,}")
        logger.info(f"✅ Registros com GMV válido: {valid_gmv_records:,}")
        logger.info(f"📊 Percentual de GMV válido: {(valid_gmv_records/total_records*100):.1f}%")
        
        # PASSO 5: Valida qualidade e aplica cast de tipos no DataFrame final
        enriched_df = validate_and_cast_final_dataframe(enriched_df)
        
        # PASSO 6: Exibe amostra dos dados processados
        logger.info("🔍 === AMOSTRA DOS DADOS ===")
        enriched_df.select(
            "reference_date", "purchase_id", "subsidiary", 
            "gmv_value", "is_gmv_valid", "purchase_status"
        ).show(5, truncate=False)
        
        return enriched_df
        
    except Exception as e:
        logger.error(f"Erro durante execução do ETL: {str(e)}")
        raise


if __name__ == "__main__":
    main()
