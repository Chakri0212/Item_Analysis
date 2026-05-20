import pandas as pd
import numpy as np
import json
import os
import sys
import zipfile
#from Codes.s01_data_pull import data_pull
from Codes.s02_data_cleaning import data_cleaning_driver
#from Automation_code.s03_item_analysis import item_analysis_script
import warnings 
warnings.filterwarnings("ignore")


## Specify which zip files to process (without .zip extension)
files_to_process = ['en_test_mental_health_d_eryka.nosal_20260309_1924','en_test_mental_health_d_eryka.nosal_20260331_1539']


def process_input_raw_data(selected_files=None):
    """
    Processes specified zip files from Input_raw_data folder:
    1. Unzips each zip file
    2. Reads input_variables.json from the supplemental folder
    3. Sets data_path to the data folder inside the unzipped content
    4. Creates result_path in output_results based on folder name and report_name

    Parameters:
        selected_files: list of zip file names (without .zip extension) to process.
                        If None, uses the files_to_process list defined above.
    """
    if selected_files is None:
        selected_files = files_to_process

    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_raw_data_dir = os.path.join(base_dir, 'Input_raw_data')
    output_results_dir = os.path.join(base_dir, 'output_results')

    # Build zip file list from selected names
    zip_files = [f"{name}.zip" for name in selected_files]

    # Validate that the zip files exist
    missing = [f for f in zip_files if not os.path.exists(os.path.join(input_raw_data_dir, f))]
    if missing:
        print(f"Warning: The following zip files were not found: {missing}")
        zip_files = [f for f in zip_files if f not in missing]

    if not zip_files:
        print("No valid zip files to process.")
        return

    for zip_filename in zip_files:
        zip_path = os.path.join(input_raw_data_dir, zip_filename)
        folder_name = os.path.splitext(zip_filename)[0]
        extract_dir = os.path.join(input_raw_data_dir, folder_name)

        # Unzip the file
        print(f"Unzipping: {zip_filename}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # Look for supplemental/input_variables.json
        supplemental_dir = os.path.join(extract_dir, 'supplemental')
        input_var_path = os.path.join(supplemental_dir, 'input_variables.json')

        if not os.path.exists(input_var_path):
            print(f"Warning: input_variables.json not found in {supplemental_dir}. Skipping.")
            continue

        # Read input variables
        with open(input_var_path, 'r') as f:
            params = json.load(f)

        # Set data_path to the data folder inside unzipped content
        data_path = os.path.join(extract_dir, 'data') + os.sep
        if not os.path.exists(data_path):
            print(f"Warning: data folder not found in {extract_dir}. Skipping.")
            continue

        # Create result_path in output_results based on folder_name and report_name
        report_name = params.get('report_name', folder_name)
        result_folder_name = f"{folder_name}_{report_name}"
        result_path = os.path.join(output_results_dir, result_folder_name) + os.sep
        os.makedirs(result_path, exist_ok=True)

        # Add paths to params
        params['data_path'] = data_path
        params['results_path'] = result_path

        print(f"Processing: {folder_name}")
        print(f"  Data path: {data_path}")
        print(f"  Results path: {result_path}")
        print(f"  Report name: {report_name}")

        # Run the analysis
        ngn_iaa_run(params)

        print(f"Analysis completed for: {folder_name}\n")
        print("--------------------------------------------------\n")


def ngn_iaa_run(params):
    #data_pull.run_code(params)
    data_cleaning_driver.run_code(params)
    #item_analysis_script.run_code(params)


if __name__ == '__main__':
    process_input_raw_data()
    