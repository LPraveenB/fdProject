import os
from pyspark.sql import functions as F
import time
import argparse
import utils
from table_mapping_config import TableMappingConfiguration
from pyspark.sql import Window
from pyspark.sql.types import *
import numpy as np

"""
spark-submit <SPARK_OPTIONS> denormalizer.py

  --location_groups LOCATION_GROUPS
                        comma seperated list of location group IDs to be processed
  --mapping_path MAPPING_PATH
                        GCS Location of the table mapping JSON
  --src_path SRC_PATH   GCS Path where the transactional/daily table CSVs/Parquets are located
  --master_src_path MASTER_SRC_PATH
                        GCS Path where the master tables(SKU, CARRIER and LOCATION) CSVs/Parquets are located
  --dest_path DEST_PATH
                        GCS Path where the daily incremental denormalized parquet to be stored
  --historical_mode HISTORICAL_MODE
                        Y or N indicating whether to run in historical mode or Incremental mode, Y for Historical and N for Incremental
"""

print('Starting Denormalizer')


def get_date_columns(col_list):
    """
    Returns the list of date columns from the list of columns provided.
    :param col_list: list of column names
    :return: the list of date columns
    """
    date_cols = []
    for each_col in col_list:
        if each_col == 'DATE' or each_col.startswith('DATE_') or '_DATE' in each_col:
            date_cols.append(each_col)
    return date_cols

def read_df_internal(df, mapping_config, object_name, historical_mode):
    """
    Transform the source dataframe given as per the mapping configurations defined.
    :param df: the source dataframe
    :param mapping_config: mapping configuration
    :param object_name: key name representing the table configuration in the mapping file.
    :param historical_mode: Boolean flag indicating the run mode as whether historical or incremental
    :return: the transformed dataframe
    """
    """
    Used for Dev Testing
    if 'LOCATION' in df.columns and  'SKU'  in df.columns:
        df = df.filter((df.LOCATION == 7843) & (df.SKU == 919437))
    """
    src_dict = mapping_config.get_source_dict(object_name)
    dest_dict = mapping_config.get_dest_dict(object_name)
    orig_key_list = list(src_dict.keys())
    col_list = []
    key_list = []
    for orig_key in orig_key_list:
        if src_dict[orig_key] in df.columns:
            col_list.append(src_dict[orig_key])
            key_list.append(orig_key)
    df = df.select(col_list)
    for each_key in key_list:
        src_value = src_dict[each_key]
        value = dest_dict[each_key]
        if src_value != value and value.strip() != '':
            df = df.withColumnRenamed(src_value, value)

    date_cols = get_date_columns(df.columns)
    if historical_mode:

        for each_date_col in date_cols:
            if object_name == 'yearly_audit_schedule':
                df = df.withColumn(each_date_col, F.to_date(F.col(each_date_col), 'dd-MMM-yy'))
            else:
                df = df.withColumn(each_date_col, F.when(F.col(each_date_col).contains('-'),
                                                                      F.to_date(F.col(each_date_col), 'yyyy-MM-dd')).otherwise(
                    F.to_date(F.col(each_date_col), 'M/d/yyyy')))
    else:
        for each_date_col in date_cols:
            df = df.withColumn(each_date_col, F.to_date(F.col(each_date_col), 'yyyy-MM-dd'))
    return df

def read_df(spark, src_path, mapping_config, location_group_id, object_name, historical_mode):
    """
    Transform the source dataframe given as per the mapping configurations defined.
    :param spark: spark session
    :param src_path: base path whether the source table data is stored.
    :param mapping_config: table mapping configuration
    :param location_group_id: location group id
    :param object_name: key name representing the table configuration in the mapping file.
    :param historical_mode: Boolean flag indicating the run mode as whether historical or incremental
    :return: the transformed dataframe
    """
    src_full_path = utils.get_full_path(src_path, mapping_config.get_source_table(object_name), location_group_id)
    df = spark.read.parquet(src_full_path)
    return  read_df_internal(df, mapping_config, object_name, historical_mode)

def read_df_master(spark, src_path, mapping_config, object_name, historical_mode):
    """
    Transform the source dataframe given as per the mapping configurations defined.
    :param spark: spark session
    :param src_path: base path whether the source table data is stored.
    :param mapping_config: table mapping configuration
    :param location_group_id: location group id
    :param object_name: key name representing the table configuration in the mapping file.
    :param historical_mode: Boolean flag indicating the run mode as whether historical or incremental
    :return: the transformed dataframe
    """
    src_full_path = src_path + '/' + mapping_config.get_source_table(object_name) + '.parquet'
    df = spark.read.parquet(src_full_path)
    return read_df_internal(df, mapping_config, object_name, historical_mode)

def select_columns(df, mapping_config, object_name):
    """
    Select the dataframe columns based on destination mapping defined.
    :param df: the source dataframe
    :param mapping_config: table mapping configuration
    :param object_name: key name representing the table configuration in the mapping file.
    :return: the dataframe with columns as defined in the destination mapping
    """
    dest_dict = mapping_config.get_dest_dict(object_name)
    orig_col_list = list(dest_dict.values())
    col_list = []
    for orig_col in orig_col_list:
        if orig_col in df.columns:
            col_list.append(orig_col)

    df = df.select(col_list)
    return df

def get_store_inv(spark, src_path, mapping_config, location_group_id, historical_mode):
    """
    Read the Data from Store Inventory Snapshot table  for the given location id  and transform as per mappings defined
    :param spark: the spark session object
    :param src_path: base path where the store inventory snapshot table data is stored.
    :param mapping_config: mapping configuration
    :param location_group_id: location group id
    :param historical_mode: whether to load the store inventory snapshot needs to be loaded in historical or incremental mode.
    :return: the store inventory snapshot dataframe
    """
    store_dest_dict = mapping_config.get_dest_dict('inventory_snapshot')
    sku_col_name = store_dest_dict['sku']
    loc_col_name = store_dest_dict['location']
    date_col_name = store_dest_dict['date']
    df = read_df(spark, src_path, mapping_config, location_group_id, 'inventory_snapshot', historical_mode)
    df = df.withColumn(store_dest_dict['day_end_total_balance'],  F.col(store_dest_dict['day_end_intras_balance']) + F.col(store_dest_dict['day_end_soh_balance']))
    df = df.withColumn(store_dest_dict['day_start_total_balance'],
                  F.col(store_dest_dict['day_start_instrans_balance']) + F.col(store_dest_dict['day_begin_soh_balance']))
    if historical_mode:
        """
        Below logic fills missing in between dates
        """
        unique_store_inv_df = df.groupBy(sku_col_name, loc_col_name).agg(F.min(F.col(date_col_name)).alias('MIN_DATE'), F.max(F.col(date_col_name)).alias('MAX_DATE'))
        unique_store_inv_df = unique_store_inv_df.select(sku_col_name, loc_col_name, 'MIN_DATE', 'MAX_DATE')
        all_dates_df = unique_store_inv_df.withColumn("Ranges",
                                              F.sequence(F.col('MIN_DATE'), F.col('MAX_DATE'),
                                                         F.expr("interval 1 day"))).withColumn(date_col_name, F.explode(
            "Ranges")).drop("Ranges")
        store_inv_df = all_dates_df.join(df, [sku_col_name, loc_col_name, date_col_name], 'left')

        """
        Derive the day end total balance, day end intrans balance, day begin soh balance and day end soh 
        balance based on day start total balance and day start intrans balance.
        """
        store_inv_df = store_inv_df.na.fill(value=0, subset=[store_dest_dict["day_start_total_balance"], store_dest_dict["day_start_instrans_balance"]])
        window_spec1 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name).desc())
        store_inv_df = store_inv_df.select("*", F.lag(store_dest_dict["day_start_total_balance"]).over(window_spec1).alias(store_dest_dict["day_end_total_balance"]),
                                   F.lag(store_dest_dict["day_start_instrans_balance"]).over(window_spec1).alias(store_dest_dict["day_end_intras_balance"]))
        store_inv_df = store_inv_df.na.fill(value=0, subset=[store_dest_dict["day_end_total_balance"], store_dest_dict["day_end_intras_balance"]])
        store_inv_df = store_inv_df.withColumn(store_dest_dict["day_begin_soh_balance"], F.col(store_dest_dict["day_start_total_balance"]) - F.col(store_dest_dict["day_start_instrans_balance"]))
        store_inv_df = store_inv_df.withColumn(store_dest_dict["day_end_soh_balance"], F.col(store_dest_dict["day_end_total_balance"]) - F.col(store_dest_dict["day_end_intras_balance"]))
        return select_columns(store_inv_df, mapping_config, 'inventory_snapshot')
    else:
        return df


def get_base_inv_adj_df(spark, src_path, mapping_config, location_group_id, historical_mode):
    """
    Read the Data from Inventory adjustment table  for the given location id  and transform as per mappings defined
    :param spark: the spark session object
    :param src_path: base path where the store inventory snapshot table data is stored.
    :param mapping_config: mapping configuration
    :param location_group_id: location group id
    :param historical_mode: whether to load the store inventory snapshot needs to be loaded in historical or incremental mode.
    :return: the inventory adjustment dataframe
    """
    inv_dest_dict = mapping_config.get_dest_dict('inventory_adjustment')
    src_full_path = utils.get_full_path(src_path, mapping_config.get_source_table('inventory_adjustment'), location_group_id)
    if utils.blob_exists(src_full_path):
        inv_adj_df = read_df(spark, src_path, mapping_config, location_group_id, 'inventory_adjustment', historical_mode)
        inv_adj_df = inv_adj_df.na.fill(value=1, subset=[inv_dest_dict['adj_reason']])
        inv_adj_df = select_columns(inv_adj_df, mapping_config, 'inventory_adjustment')
    else:
        """
        Returns an empty dataframe with required columns if the inventory adjustment table csv is not present
        """
        invAdjSchema = StructType([
            StructField(inv_dest_dict['sku'], IntegerType(), True),
            StructField(inv_dest_dict['location'], IntegerType(), True),
            StructField(inv_dest_dict['date'], DateType(), True),
            StructField(inv_dest_dict['adj_units'], DoubleType(), True),
            StructField(inv_dest_dict['adj_reason'], IntegerType(), True)
        ])

        inv_adj_df = spark.createDataFrame(data=[], schema=invAdjSchema)

    return inv_adj_df


def get_reason_inv_adj_df(inv_adj_df, reason_codes, qty_col, inv_dest_dict):
    """
    Returns the sub dataframe only for the reason codes given and aggregate quantity at a daily granularity
    :param inv_adj_df: source inventory dataframe
    :param reason_codes: reason codes
    :param qty_col: alias name for the aggregated quantity column
    :param inv_dest_dict: mapping dictionary
    :return: the sub dataframe
    """
    sku_col_name = inv_dest_dict['sku']
    loc_col_name = inv_dest_dict['location']
    date_col_name = inv_dest_dict['date']

    if len(reason_codes) > 0:
        inv_adj_df = inv_adj_df.filter(inv_adj_df.REASON.isin(reason_codes))
    inv_adj_df = inv_adj_df.groupby(sku_col_name, loc_col_name, date_col_name).agg(F.sum(inv_dest_dict["adj_units"]).alias(qty_col)).select(sku_col_name, loc_col_name, date_col_name, qty_col)

    return inv_adj_df


def get_inv_adj_df(spark, src_path, mapping_config, location_group_id, historical_mode):
    """
    Read the Data from Inventory adjustment table  for the given location id, transform as per mappings defined and derive damage, storeuse, expired and scanerr qty based on adjustment units and reason codes.
    :param spark: the spark session object
    :param src_path: base path where the store inventory snapshot table data is stored.
    :param mapping_config: mapping configuration
    :param location_group_id: location group id
    :param historical_mode: whether to load the store inventory snapshot needs to be loaded in historical or incremental mode.
    :return: the inventory adjustment dataframe
    """
    inv_dest_dict = mapping_config.get_dest_dict('inventory_adjustment')
    sku_col_name = inv_dest_dict['sku']
    loc_col_name = inv_dest_dict['location']
    date_col_name = inv_dest_dict['date']
    adjqty_col_name = inv_dest_dict['adj_units']
    join_key = [sku_col_name, loc_col_name, date_col_name]
    base_inv_adj = get_base_inv_adj_df(spark, src_path, mapping_config, location_group_id, historical_mode)

    adj_damage_df = get_reason_inv_adj_df(base_inv_adj, [4, 11], inv_dest_dict['damage_units'], inv_dest_dict)
    adj_storeuse_df = get_reason_inv_adj_df(base_inv_adj, [10], inv_dest_dict['storeuse_units'], inv_dest_dict)
    adj_expired_df = get_reason_inv_adj_df(base_inv_adj, [14], inv_dest_dict['expired_units'], inv_dest_dict)
    adj_scanerr_df = get_reason_inv_adj_df(base_inv_adj, [9], inv_dest_dict['scanerr_units'], inv_dest_dict)

    inv_adj_df = base_inv_adj.groupby(sku_col_name, loc_col_name, date_col_name).agg(F.sum(adjqty_col_name).alias(adjqty_col_name), F.count(date_col_name).alias(inv_dest_dict['intraday_count']),
                                                                   F.count(F.when(F.col(adjqty_col_name) > 0, True)).alias(
                                                                       inv_dest_dict['pos_intraday_count']),
                                                                   F.count(F.when(F.col(adjqty_col_name) < 0, True)).alias(
                                                                       inv_dest_dict['neg_intraday_count']), F.sum(
            F.when(F.col(adjqty_col_name) > 0, F.col(adjqty_col_name)).otherwise(F.lit(0))).alias(inv_dest_dict['pos_adj_units']), F.sum(
            F.when(F.col(adjqty_col_name) < 0, F.col(adjqty_col_name)).otherwise(F.lit(0))).alias(inv_dest_dict['neg_adj_units'])).select(sku_col_name,
                                                                                                                        loc_col_name, date_col_name,
                                                                                                                        adjqty_col_name,
                                                                                                                        inv_dest_dict['intraday_count'],
                                                                                                                        inv_dest_dict['pos_intraday_count'],
                                                                                                                        inv_dest_dict['neg_intraday_count'],
                                                                                                                        inv_dest_dict['pos_adj_units'],
                                                                                                                        inv_dest_dict['neg_adj_units'])
    inv_adj_df = inv_adj_df.join(adj_damage_df, on=join_key, how='left')
    inv_adj_df = inv_adj_df.join(adj_storeuse_df, on=join_key, how='left')
    inv_adj_df = inv_adj_df.join(adj_expired_df, on=join_key, how='left')
    inv_adj_df = inv_adj_df.join(adj_scanerr_df, on=join_key, how='left')

    return inv_adj_df

def get_sales_df(spark, src_path, mapping_config, location_group_id, historical_mode):
    """
    Read the Data from Sales table  for the given location id, transform as per mappings defined and aggregate at a daily granularity
    :param spark: the spark session object
    :param src_path: base path where the table data is stored.
    :param mapping_config: mapping configuration
    :param location_group_id: location group id
    :param historical_mode: whether to load the data in historical or incremental mode.
    :return: the sales dataframe
    """
    sales_dest_dict = mapping_config.get_dest_dict('sales')
    src_full_path = utils.get_full_path(src_path, mapping_config.get_source_table('sales'), location_group_id)
    print('SALES PATH', src_full_path, utils.blob_exists(src_full_path))
    if utils.blob_exists(src_full_path):
        sales_df = read_df(spark, src_path, mapping_config, location_group_id, 'sales', historical_mode)

    else:
        """
        Returns an empty dataframe with required columns if the sales table csv is not present
        """
        salesSchema = StructType([
            StructField(sales_dest_dict['sku'], IntegerType(), True),
            StructField(sales_dest_dict['location'], IntegerType(), True),
            StructField(sales_dest_dict['date'], DateType(), True),
            StructField(sales_dest_dict['sales_units'], DoubleType(), True),
            StructField(sales_dest_dict['sales_price_total'], DoubleType(), True)
        ])

        sales_df = spark.createDataFrame(data=[], schema=salesSchema)

    sales_df = sales_df.withColumn(sales_dest_dict['bulk_sales_units'], F.when(F.col(sales_dest_dict["sales_units"]) > 3, F.col(sales_dest_dict["sales_units"])).otherwise(F.lit(0)))
    sales_df = sales_df.groupby(sales_dest_dict['sku'], sales_dest_dict['location'],
                                sales_dest_dict['date']).agg(
        F.count(sales_dest_dict['date']).alias(sales_dest_dict['sales_count']), F.sum(sales_dest_dict['bulk_sales_units']).alias(sales_dest_dict['bulk_sales_units']),
        F.sum(sales_dest_dict["sales_units"]).alias(sales_dest_dict["sales_units"]),
        F.sum(sales_dest_dict["sales_price_total"]).alias(sales_dest_dict["sales_price_total"]))

    return sales_df


def get_returns_df(spark, src_path, mapping_config, location_group_id, historical_mode):
    """
    Read the Data from Returns table  for the given location id, transform as per mappings defined and aggregate at a daily granularity
    :param spark: the spark session object
    :param src_path: base path where the table data is stored.
    :param mapping_config: mapping configuration
    :param location_group_id: location group id
    :param historical_mode: whether to load the data in historical or incremental mode.
    :return: the returns dataframe
    """
    returns_dest_dict = mapping_config.get_dest_dict('returns')
    src_full_path = utils.get_full_path(src_path, mapping_config.get_source_table('returns'), location_group_id)
    if utils.blob_exists(src_full_path):
        returns_df = read_df(spark, src_path, mapping_config, location_group_id, 'returns', historical_mode)

    else:
        """
        Returns an empty dataframe with required columns if the returns table csv is not present
        """
        returnsSchema = StructType([
            StructField(returns_dest_dict['sku'], IntegerType(), True),
            StructField(returns_dest_dict['location'], IntegerType(), True),
            StructField(returns_dest_dict['date'], DateType(), True),
            StructField(returns_dest_dict['return_units'], DoubleType(), True)
        ])

        returns_df = spark.createDataFrame(data=[], schema=returnsSchema)

    returns_df = returns_df.groupby(returns_dest_dict['sku'], returns_dest_dict['location'],
                                returns_dest_dict['date']).agg(
        F.sum(returns_dest_dict["return_units"]).alias(returns_dest_dict["return_units"]))

    return returns_df

def get_invoice_df(spark, src_path, mapping_config, location_group_id, historical_mode):
    """
    Read the Data from invoice table for the given location id, transform as per mappings defined and aggregate at a daily granularity
    :param spark: the spark session object
    :param src_path: base path where the table data is stored.
    :param mapping_config: mapping configuration
    :param location_group_id: location group id
    :param historical_mode: whether to load the data in historical or incremental mode.
    :return: the invoice dataframe
    """
    invoice_dest_dict = mapping_config.get_dest_dict('store_invoice')
    src_full_path = utils.get_full_path(src_path, mapping_config.get_source_table('store_invoice'), location_group_id)
    if utils.blob_exists(src_full_path):
        invoice_df = read_df(spark, src_path, mapping_config, location_group_id, 'store_invoice', historical_mode)

    else:
        """
        Returns an empty dataframe with required columns if the invoice table csv is not present
        """
        invoiceSchema = StructType([
            StructField(invoice_dest_dict['sku'], IntegerType(), True),
            StructField(invoice_dest_dict['location'], IntegerType(), True),
            StructField(invoice_dest_dict['date'], DateType(), True),
            StructField(invoice_dest_dict['invoice_units'], DoubleType(), True)
        ])

        invoice_df = spark.createDataFrame(data=[], schema=invoiceSchema)

    invoice_df = invoice_df.groupby(invoice_dest_dict['sku'], invoice_dest_dict['location'],
                                invoice_dest_dict['date']).agg(
        F.sum(invoice_dest_dict["invoice_units"]).alias(invoice_dest_dict["invoice_units"]))

    return invoice_df

def get_transfer_df(spark, src_path, mapping_config, location_group_id, historical_mode):
    """
    Read the Data from transfer table  for the given location id, transform as per mappings defined and aggregate at a daily granularity
    :param spark: the spark session object
    :param src_path: base path where the table data is stored.
    :param mapping_config: mapping configuration
    :param location_group_id: location group id
    :param historical_mode: whether to load the data in historical or incremental mode.
    :return: the transfer dataframe
    """
    transfer_dest_dict = mapping_config.get_dest_dict('store_transfers')
    src_full_path = utils.get_full_path(src_path, mapping_config.get_source_table('store_transfers'), location_group_id)

    if utils.blob_exists(src_full_path):
        transfer_df = read_df(spark, src_path, mapping_config, location_group_id, 'store_transfers', historical_mode)

    else:
        """
        Returns an empty dataframe with required columns if the transfer table csv is not present
        """
        transferSchema = StructType([
            StructField(transfer_dest_dict['sku'], IntegerType(), True),
            StructField(transfer_dest_dict['location'], IntegerType(), True),
            StructField(transfer_dest_dict['date'], DateType(), True),
            StructField(transfer_dest_dict['transfer_units'], DoubleType(), True),
            StructField(transfer_dest_dict['transfer_code'], IntegerType(), True)
        ])

        transfer_df = spark.createDataFrame(data=[], schema=transferSchema)

    transfer_df = transfer_df.groupby(transfer_dest_dict['sku'], transfer_dest_dict['location'],
                                transfer_dest_dict['date'], transfer_dest_dict['transfer_code']).agg(
        F.sum(transfer_dest_dict["transfer_units"]).alias(transfer_dest_dict["transfer_units"]))

    return transfer_df

"""
Commented for  Demo
def get_yearly_audit_df(spark, src_path, mapping_config, location_group_id, historical_mode):
    yearly_audit_dest_dict = mapping_config.get_dest_dict('yearly_audit')
    src_full_path = utils.get_full_path(src_path, mapping_config.get_source_table('yearly_audit'), location_group_id)
    if utils.blob_exists(src_full_path):
        yearly_audit_df = read_df(spark, src_path, mapping_config, location_group_id, 'yearly_audit', historical_mode)

    else:
        year_audit_schema = StructType([
            StructField(yearly_audit_dest_dict['sku'], IntegerType(), True),
            StructField(yearly_audit_dest_dict['location'], IntegerType(), True),
            StructField(yearly_audit_dest_dict['audit_date'], DateType(), True),
            StructField(yearly_audit_dest_dict['audit_units'], DoubleType(), True)
        ])

        yearly_audit_df = spark.createDataFrame(data=[], schema=year_audit_schema)

    return yearly_audit_df
"""


def get_yearly_audit_schedule_df(spark, src_path, mapping_config, location_group_id, historical_mode):
    """
    Read the Data from yearly audit schedule table  for the given location id, transform as per mappings defined and aggregate at a daily granularity
    :param spark: the spark session object
    :param src_path: base path where the table data is stored.
    :param mapping_config: mapping configuration
    :param location_group_id: location group id
    :param historical_mode: whether to load the data in historical or incremental mode.
    :return: the yearly audit schedule dataframe
    """
    yearly_audit_sch_dest_dict = mapping_config.get_dest_dict('yearly_audit_schedule')
    src_full_path = utils.get_full_path(src_path, mapping_config.get_source_table('yearly_audit_schedule'), location_group_id)
    if utils.blob_exists(src_full_path):
        yearly_audit_sch_df = read_df(spark, src_path, mapping_config, location_group_id, 'yearly_audit_schedule', historical_mode)
        yearly_audit_sch_df = yearly_audit_sch_df.withColumn(yearly_audit_sch_dest_dict['audit_flag'], F.lit(1))

    else:
        """
        Returns an empty dataframe with required columns if the yearly audit schedule table csv is not present
        """
        year_audit__schedule_schema = StructType([
            StructField(yearly_audit_sch_dest_dict['location'], IntegerType(), True),
            StructField(yearly_audit_sch_dest_dict['audit_date'], DateType(), True),
            StructField(yearly_audit_sch_dest_dict['date'], DateType(), True),
            StructField(yearly_audit_sch_dest_dict['audit_flag'], IntegerType(), True)
        ])

        yearly_audit_sch_df = spark.createDataFrame(data=[], schema=year_audit__schedule_schema)

    return yearly_audit_sch_df

def carrier_asof_join(l, r):
    """
    Pandas  based function to join/merge dataframes based on nearby dates upto 7 days.
    :param l: the left dataframe
    :param r: the right dataframe
    :return: the merged dataframe.
    """
    import pandas as pd
    l["DATE"] = pd.to_datetime(l["DATE"])
    r["DATE"] = pd.to_datetime(r["DATE"])
    return pd.merge_asof(l.sort_values(by="DATE"), r.sort_values(by="DATE"), on="DATE", by="LOCATION", allow_exact_matches=True, direction='forward', tolerance=pd.Timedelta(7,"d"))

def get_sku(spark, src_path, mapping_config):
    """
    Read the sku data from the given source path and transform as per mapping configurations defined.
    :param spark: the spark session object
    :param src_path: the source path where the sku data is stored.
    :param mapping_config: table mapping configuration
    :return: the sku master dataframe
    """
    return read_df_master(spark, src_path, mapping_config, 'sku', False)

"""
Commented  for Demo
def get_carrier_master(spark, src_path, read_config, historical_mode):
    return read_df_master(spark, src_path, read_config, 'carrier_master', historical_mode)

def get_transfers_in_carriers_df(dc_transfers_in_df, carrier_master, read_config):
    dc_transer_units_col_name = read_config.get_dest_dict('derived_fields')['dc_transferred_units']
    in_transit_units_col_name = read_config.get_dest_dict('store_transfers')['in_transit_units']
    sku_col_name = read_config.get_dest_dict('inventory_snapshot')['sku']
    loc_col_name = read_config.get_dest_dict('inventory_snapshot')['location']
    date_col_name = read_config.get_dest_dict('inventory_snapshot')['date']
    actual_arr_date = read_config.get_dest_dict('carrier_master')['actual_arrival']
    dc_transfers_in_df = dc_transfers_in_df.withColumnRenamed(in_transit_units_col_name, dc_transer_units_col_name)
    carrier_window_spec = Window.partitionBy(read_config.get_dest_dict('carrier_master')['shipment_id']).orderBy(read_config.get_dest_dict('carrier_master')['stop_num'])
    carrier_master = carrier_master.withColumn(loc_col_name, F.when(F.col(read_config.get_dest_dict('carrier_master')['location_gid']).contains("FD.FDS_"),
                                                                  F.substring(F.col(read_config.get_dest_dict('carrier_master')['location_gid']), 8, 5).cast(
                                                                      "int")).otherwise(0))
    carrier_master = carrier_master.withColumn(actual_arr_date, F.coalesce(F.col(actual_arr_date),
                                                                            F.lag(actual_arr_date, 1).over(
                                                                                carrier_window_spec),
                                                                            F.lag(actual_arr_date, -1).over(
                                                                                carrier_window_spec),
                                                                            F.col(read_config.get_dest_dict('carrier_master')['estimated_arrival']),
                                                                            F.col(read_config.get_dest_dict('carrier_master')['planned_arrival']),
                                                                            F.col(read_config.get_dest_dict('carrier_master')['end_time'])))
    carrier_master = carrier_master.withColumn(actual_arr_date, F.when(F.col(actual_arr_date) < F.col(read_config.get_dest_dict('carrier_master')['start_time']),
                                                                        F.col(read_config.get_dest_dict('carrier_master')['end_time'])).otherwise(
        F.col(actual_arr_date)))

    carrier_master = carrier_master.select(loc_col_name, actual_arr_date)
    carrier_master = carrier_master.withColumn(date_col_name, F.col(actual_arr_date))
    carrier_master = carrier_master.distinct()

    dc_transfers_in_df_carriers_group = dc_transfers_in_df.groupby(loc_col_name).cogroup(
        carrier_master.groupby(loc_col_name)).applyInPandas(
        carrier_asof_join, schema=sku_col_name + " int, " + loc_col_name +  " int, " + date_col_name + " date, "  + dc_transer_units_col_name + " double, " + actual_arr_date + " date"
    )

    dc_transfers_in_df_carriers_group = dc_transfers_in_df_carriers_group.select(sku_col_name, loc_col_name, date_col_name,
                                                                                 actual_arr_date)
    dc_transfers_in_df_carriers_group = dc_transfers_in_df_carriers_group.withColumn(actual_arr_date, F.when(
        F.col(actual_arr_date).isNull(), F.col(date_col_name)).otherwise(F.col(actual_arr_date)))
    dc_transfers_in_df_carriers = dc_transfers_in_df.join(dc_transfers_in_df_carriers_group,
                                                          [sku_col_name, loc_col_name, date_col_name], "left")

    dc_transfers_in_df_carriers = dc_transfers_in_df_carriers.drop(date_col_name).withColumnRenamed(actual_arr_date, date_col_name)
    dc_transfers_in_df_carriers = dc_transfers_in_df_carriers.groupby(sku_col_name, loc_col_name, date_col_name).agg(
        F.sum(dc_transer_units_col_name).alias(dc_transer_units_col_name))
    return dc_transfers_in_df_carriers
"""

def generate_result_internal(spark, location_group_id, mapping_path, src_path, master_src_path, dest_path, historical_mode, mapping_config):
    print('Processing', location_group_id)
    if historical_mode:
        #Configs required to run manually in GCP dataproc
        os.environ['PYSPARK_PYTHON'] = '/opt/conda/default/bin/python'
        os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

    total_start_time = time.time()
    join_dest_dict = mapping_config.get_dest_dict('inventory_snapshot')
    join_key = [join_dest_dict['sku'], join_dest_dict['location'], join_dest_dict['date']]
    store_inv_df = get_store_inv(spark, src_path, mapping_config, location_group_id, historical_mode)

    inv_adj_df = get_inv_adj_df(spark, src_path, mapping_config, location_group_id, historical_mode)

    sales_df = get_sales_df(spark, src_path, mapping_config, location_group_id, historical_mode)

    returns_df = get_returns_df(spark, src_path, mapping_config, location_group_id, historical_mode)

    transfer_df = get_transfer_df(spark, src_path, mapping_config, location_group_id, historical_mode)
    transfer_dest_dict = mapping_config.get_dest_dict('store_transfers')
    dc_transfer_in_df = transfer_df.filter(F.col(transfer_dest_dict['transfer_code']) == 30)
    dc_transfer_in_df = dc_transfer_in_df.withColumnRenamed(transfer_dest_dict['transfer_units'], transfer_dest_dict['in_transit_units'])
    dc_transfer_in_df = dc_transfer_in_df.drop(transfer_dest_dict['transfer_code'])

    dsd_transfer_in_df = transfer_df.filter(F.col(transfer_dest_dict['transfer_code']) == 20)
    dsd_transfer_in_df = dsd_transfer_in_df.drop(transfer_dest_dict['transfer_code'])
    dsd_transfer_in_df = dsd_transfer_in_df.withColumnRenamed(transfer_dest_dict['transfer_units'], transfer_dest_dict['dsd_units'])


    invoice_df = get_invoice_df(spark, src_path, mapping_config, location_group_id, historical_mode)
    '''
        Commented for Demo
    audit_df =  get_yearly_audit_df(spark, src_path, read_config, location_group_id)
    '''

    audit_schedule_df = get_yearly_audit_schedule_df(spark, src_path, mapping_config, location_group_id, historical_mode)
    '''
    Commented for Demo
    audit_df_joined = audit_df.join(audit_schedule_df, [read_config.get_dest_dict('yearly_audit')['location'], read_config.get_dest_dict('yearly_audit')['audit_date']], 'left')
    audit_df_joined = audit_df_joined.drop(read_config.get_dest_dict('yearly_audit_schedule')['audit_flag'])
    audit_df_joined = audit_df_joined.drop(read_config.get_dest_dict('yearly_audit')['audit_date'])
    '''
    sku_df = get_sku(spark, master_src_path, mapping_config)
    final_df = store_inv_df.join(inv_adj_df, join_key, 'left')
    final_df = final_df.join(sales_df, join_key, 'left')
    final_df = final_df.join(returns_df, join_key, 'left')
    final_df = final_df.join(dc_transfer_in_df, join_key, 'left')
    final_df = final_df.join(dsd_transfer_in_df, join_key, 'left')
    final_df = final_df.join(invoice_df, join_key, 'left')

    final_df = final_df.join(audit_schedule_df, [join_dest_dict['location'], join_dest_dict['date']], 'left')
    '''
    Commented for Demo
    final_df = final_df.join(audit_df_joined, join_key, 'left')
    '''
    final_df = final_df.join(sku_df, [join_dest_dict['sku']], 'left')
    # Commented for Demo
    #final_df = final_df.join(get_transfers_in_carriers_df(dc_transfer_in_df,  get_carrier_master(spark, src_path, read_config), read_config), join_key, "left")
    cols_fill_zero = []
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['adj_units'])
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['damage_units'])
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['storeuse_units'])
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['expired_units'])
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['scanerr_units'])
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['intraday_count'])
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['pos_intraday_count'])
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['neg_intraday_count'])
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['pos_adj_units'])
    cols_fill_zero.append(mapping_config.get_dest_dict('inventory_adjustment')['neg_adj_units'])

    cols_fill_zero.append(mapping_config.get_dest_dict('sales')['sales_units'])
    cols_fill_zero.append(mapping_config.get_dest_dict('sales')['sales_price_total'])
    cols_fill_zero.append(mapping_config.get_dest_dict('sales')['sales_count'])
    cols_fill_zero.append(mapping_config.get_dest_dict('sales')['bulk_sales_units'])

    cols_fill_zero.append(mapping_config.get_dest_dict('returns')['return_units'])
    cols_fill_zero.append(mapping_config.get_dest_dict('store_transfers')['in_transit_units'])
    cols_fill_zero.append(mapping_config.get_dest_dict('store_transfers')['dsd_units'])

    cols_fill_zero.append(mapping_config.get_dest_dict('store_invoice')['invoice_units'])

    cols_fill_zero.append(mapping_config.get_dest_dict('yearly_audit_schedule')['audit_flag'])

    cols_fill_zero.append(mapping_config.get_dest_dict('sku')['width'])
    cols_fill_zero.append(mapping_config.get_dest_dict('sku')['height'])
    cols_fill_zero.append(mapping_config.get_dest_dict('sku')['depth'])
    # Commented for Demo
    #cols_fill_zero.append(read_config.get_dest_dict('derived_fields')['dc_transferred_units'])

    final_df = final_df.na.fill(value=0, subset=cols_fill_zero)

    final_df = post_process(final_df, mapping_config)
    final_df = final_df.drop(F.col(mapping_config.get_dest_dict('yearly_audit_schedule')['audit_date']))#For Demo
    print('Writing')
    out_file = dest_path + '/LOCATION_GROUP=' + str(location_group_id) + '/'
    final_df.write.option("header", True).option("compression", "gzip").mode("overwrite").parquet(out_file)

    print('Total Time Taken', time.time() - total_start_time, 'Seconds')
    print('Successfully Written the DataFrame to ', dest_path)

def post_process(df, mapping_config):
    derived_dest_dict = mapping_config.get_dest_dict('derived_fields')

    df = df.withColumn(derived_dest_dict['sales_units_without_return'], F.col(mapping_config.get_dest_dict('sales')['sales_units']) - F.col(mapping_config.get_dest_dict('returns')['return_units']))

    df = df.withColumn(mapping_config.get_dest_dict('store_transfers')['dsd_units'], F.when(F.col(mapping_config.get_dest_dict('store_transfers')['dsd_units']) > 0, F.col(mapping_config.get_dest_dict('store_transfers')['dsd_units']) - F.col(
        mapping_config.get_dest_dict('store_transfers')['in_transit_units'])).otherwise(F.col(mapping_config.get_dest_dict('store_transfers')['dsd_units'])))

    df = df.withColumn(derived_dest_dict['dsd_transferred_units'],
                       F.when((F.col(mapping_config.get_dest_dict('store_transfers')['dsd_units']) > 0) | (F.col(mapping_config.get_dest_dict('store_invoice')['invoice_units']) > 0),
                              (F.col(mapping_config.get_dest_dict('inventory_snapshot')['day_end_total_balance']) + F.col(mapping_config.get_dest_dict('sales')['sales_units']) - F.col(mapping_config.get_dest_dict('inventory_adjustment')['adj_units'])) - (
                                              F.col(mapping_config.get_dest_dict('inventory_snapshot')['day_start_total_balance']))).otherwise(F.lit(0)))


    df = df.withColumn(derived_dest_dict['sales_unit_retail'],
                       F.round(F.col(mapping_config.get_dest_dict('sales')['sales_price_total']) / F.col(mapping_config.get_dest_dict('sales')['sales_units']), 2))

    """
    After Training is  introduced, below audit_adj_units and onhand_recorded computation to be mmoved after audit units assignment
    """
    df = df.withColumn(derived_dest_dict['audit_adj_units'], F.col(mapping_config.get_dest_dict('inventory_adjustment')['adj_units']))

    df = df.withColumn(derived_dest_dict['onhand_recorded'], F.when(
        (F.col(mapping_config.get_dest_dict('inventory_snapshot')['day_end_soh_balance']) + F.col(
            mapping_config.get_dest_dict('sales')['sales_units']) - F.col(
            derived_dest_dict['audit_adj_units'])) > F.col(
            mapping_config.get_dest_dict('inventory_snapshot')['day_begin_soh_balance']), (
                (F.col(mapping_config.get_dest_dict('inventory_snapshot')['day_end_soh_balance']) + F.col(
                    mapping_config.get_dest_dict('sales')['sales_units']) - F.col(
                    derived_dest_dict['audit_adj_units'])) - F.col(
            mapping_config.get_dest_dict('inventory_snapshot')['day_begin_soh_balance']))).otherwise(F.lit(0)))

    df = df.withColumn(derived_dest_dict['onhand_recorded'],
                                     F.when(F.col(derived_dest_dict['dsd_transferred_units']) < 0,
                                            F.col(derived_dest_dict['onhand_recorded']) + F.col(
                                                derived_dest_dict['dsd_transferred_units'])).otherwise(
                                         F.col(derived_dest_dict['onhand_recorded'])))

    df = df.na.fill(value=0.0, subset=[derived_dest_dict['sales_unit_retail']])
    return df

def generate_results(spark, location_groups, mapping_path, src_path, master_src_path, dest_path, historical_mode):
    mapping_config = TableMappingConfiguration(mapping_path)
    for each_loc_group in location_groups:
        generate_result_internal(spark, each_loc_group, mapping_path, src_path, master_src_path, dest_path, historical_mode, mapping_config)
def main(args=None):
    print("args", args)
    parser = argparse.ArgumentParser(description='Denormalizer Process')

    parser.add_argument(
        '--location_groups',
        dest='location_groups',
        type=str,
        required=True,
        help='comma seperated list of location group IDs to be processed')

    parser.add_argument(
        '--mapping_path',
        dest='mapping_path',
        type=str,
        required=True,
        help='GCS Location of the table mapping JSON')

    parser.add_argument(
        '--src_path',
        dest='src_path',
        type=str,
        required=True,
        help='GCS Path where the transactional/daily table CSVs/Parquets are located')

    parser.add_argument(
        '--master_src_path',
        dest='master_src_path',
        type=str,
        required=True,
        help='GCS Path where the master tables(SKU, CARRIER and LOCATION) CSVs/Parquets are located')

    parser.add_argument(
        '--dest_path',
        dest='dest_path',
        type=str,
        required=True,
        help='GCS Path where the daily incremental denormalized parquet to be stored')

    parser.add_argument(
        '--historical_mode',
        dest='historical_mode',
        type=str,
        required=True,
        help='Y or N  indicating whether to run in historical mode or Incremental mode, Y for Historical and N for Incremental')


    args = parser.parse_args(args)

    print('args.location_groups', args.location_groups)
    print('args.mapping_path', args.mapping_path)
    print('args.src_path', args.src_path)
    print('args.master_src_path', args.master_src_path)
    print('args.dest_path', args.dest_path)
    print('args.historical_mode', args.historical_mode)

    historical_mode = False
    if args.historical_mode == 'Y':
        historical_mode = True

    location_groups = args.location_groups.split(',')

    spark = utils.get_spark_session()
    generate_results(spark, location_groups, args.mapping_path, args.src_path, args.master_src_path, args.dest_path, historical_mode)


if __name__ == '__main__':
    main()