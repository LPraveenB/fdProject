import json
from urllib.parse import urlparse
from google.cloud import storage

class TableMappingConfiguration:
    """
    Reads the table mapping configurations from JSON and exposes the details via Helper methods.
    """
    config_dict = {}
    computed_dict = {}
    table_key_mapping = {}
    def __init__(self, mapping_path):
        """
        Loads the Table Mapping from the JSON path in to dict for easier access.
        :param mapping_path: GCS Path where the table mapping is stored.
        """
        gcs_path = urlparse(mapping_path, allow_fragments=False)
        bucket_name, filename = gcs_path.netloc, gcs_path.path.lstrip("/")
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(filename)
        self.config_dict = json.loads(blob.download_as_string())

        for each_key in self.config_dict.keys():
            table_dict = self.config_dict[each_key]
            temp_dict = {}
            temp_dict['source_table_name'] = table_dict['source_table_name']
            col_dict = table_dict['columns']
            src_dict = {}
            dest_dict = {}
            type_dict = {}
            nullable_list = []
            for sub_key in col_dict:
                value_dict = col_dict[sub_key]
                src_dict[sub_key] = value_dict['src']
                dest_dict[sub_key] = value_dict['dest']
                if value_dict['src'] is not None:
                    type_dict[value_dict['src']] = value_dict['data_type']
                    if 'nullable' in value_dict and  value_dict['nullable'] == 'Y':
                        nullable_list.append(value_dict['src'])
            temp_dict['src_dict'] = src_dict
            temp_dict['dest_dict'] = dest_dict
            temp_dict['type_dict'] = type_dict
            temp_dict['nullable_list'] = nullable_list
            self.computed_dict[each_key] = temp_dict
            self.table_key_mapping[table_dict['source_table_name']] = each_key

    def get_source_table(self, table_key):
        """
        Returns the name of source table File name without extension for the given table key for e.g., FD_STORE_INV
        :param table_key: unique key representing the table mapping configuration in dict/JSON.
        :return: the source table File name without extension
        """
        return self.computed_dict[table_key]['source_table_name']

    def get_source_dict(self, table_key):
        """
        Returns the dictionary containing the column technical identifier/key along with source table column names.
        :param table_key: unique key representing the table mapping configuration in dict/JSON.
        :return: the dictionary containing the column technical identifier/key along with source table column names
        """
        return self.computed_dict[table_key]['src_dict']

    def get_dest_dict(self, table_key):
        """
        Returns the dictionary containing the column technical identifier/key along with destination table column names.
        :param table_key: unique key representing the table mapping configuration in dict/JSON.
        :return: the dictionary containing the column technical identifier/key along with destination table column names
        """
        return self.computed_dict[table_key]['dest_dict']

    def get_type_dict(self, table_key):
        """
        Returns the dictionary containing the column technical identifier/key along with required column types.
        :param table_key: unique key representing the table mapping configuration in dict/JSON.
        :return: the dictionary containing the column technical identifier/key along with required column types
        """
        return self.computed_dict[table_key]['type_dict']

    def get_nullable_columns(self, table_key):
        """
        Returns the list of source columns that can be null as per mapping defined for a given table key.
        :param table_key: unique key representing the table mapping configuration in dict/JSON.
        :return: the list of source columns that can be null as per mapping defined for a given table key.
        """
        return self.computed_dict[table_key]['nullable_list']

    def get_table_key_mapping(self):
        """
        :return: the reverse mapping of source table names (File names without extension) to table  key name. e.g., FD_STORE_INV -> inventory_snapshot
        """
        return self.table_key_mapping

    def get_computed_dict(self):
        """
        :return: the entire computed dictionary.
        """
        return self.computed_dict