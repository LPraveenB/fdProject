log_level = "INFO"
log_component_name = "Data Validator"
files = ["FD_STORE_INV",
         "FD_RETURNS",
         "FD_SAMPLE",
         "FD_SALES",
         "FD_INV_ADJ",
         "FD_STORE_TRANSFERS_IN",
         "FD_DSD_INVOICE",
         "FD_SKU_MASTER",
         "FD_PHYSICAL_INV_ADJUSTMENTS",
         "FD_PHYSICAL_INVENTORY_COUNT_SCHEDULE"]
permitted_extensions = ["csv", "gz"]
report_columns = ["column", "data_type", "null_check", "date_format"]
