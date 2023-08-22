from datetime import datetime

import config
import file_utils
import gcs_utils
import logger_helpers
import logger_utils
import main_helpers

if __name__ == "__main__":
    try:
        # Logging
        log_level = config.log_level
        log_component_name = config.log_component_name
        logger = logger_utils.setup_logger(log_level, log_component_name)
        logger_helpers.assign_logger_to_files(logger)

        # Parsing command line arguments
        command_line_arguments_dict = main_helpers.get_command_line_arguments()

        # input variables
        input_path = command_line_arguments_dict["input_path"]
        error_path = command_line_arguments_dict["error_path"].rstrip("/")+"/"
        report_path = command_line_arguments_dict["report_path"].rstrip("/")+"/"
        json_path = command_line_arguments_dict["json_path"]
        date_missing_files = command_line_arguments_dict["date_missing_files"]

        logger.info(f"Input Path : {input_path} ; Type : {type(input_path)}")
        logger.info(f"Error Path : {error_path} ; Type : {type(error_path)}")
        logger.info(f"Report Path : {report_path} ; Type : {type(report_path)}")
        logger.info(f"JSON Path : {json_path} ; Type : {type(json_path)}")
        logger.info(f"Date Missing Files : {date_missing_files} ; Type : {type(date_missing_files)}")

        json_data = gcs_utils.read_json_from_gcs(json_path)

        files = config.files
        permitted_extensions = config.permitted_extensions
        report_columns = config.report_columns

        start_time = datetime.now()
        main_helpers.process_files(input_path, error_path, report_path, files, json_data,
                                   permitted_extensions, report_columns, date_missing_files)
        end_time = datetime.now()
        print(f"The time taken for completing all the validations : {end_time-start_time}")

    except Exception as e:
        raise
