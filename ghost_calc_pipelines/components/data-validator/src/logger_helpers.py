import file_utils
import gcs_utils
import main_helpers
import spark_utils


def assign_logger_to_files(logger: object):
    """
    The assign_logger_to_files function assigns the logger object to all of the helper and utility files.
    This is done so that each file can log messages, which makes it easier for debugging purposes.

    :param logger: object: Assign the logger to all of the files in this project
    """
    try:
        main_helpers.logger = logger

        gcs_utils.logger = logger

        file_utils.logger = logger

        spark_utils.logger = logger
    except Exception as e:
        print(f"Logger not assigned to file : {e}")
        raise
