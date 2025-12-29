import pandas as pd
import numpy as np
import json
import os
import sys
#from Codes.s01_data_pull import data_pull
from Codes.s02_data_cleaning import data_cleaning_driver
#from Automation_code.s03_item_analysis import item_analysis_script
import warnings 
warnings.filterwarnings("ignore")
#from knight.salvador

def ngn_iaa_run(params):
    #data_pull.run_code(params)
    data_cleaning_driver.run_code(params)
    #item_analysis_script.run_code(params)

input_variables=['input_variables_physical_health_122025.json']
file_path = 'G:\\My Drive\\My_laptop_backup\\DRCR_Q2_2024\\Automation_project\\Git_hub_code\\item_analysis_automation-main\\NGN_IAA\\'
for var in input_variables:
    with open(file_path+var) as f:
        params = json.load(f)
    
    print(f"Running analysis with parameters: {params}")
    ngn_iaa_run(params)

    print("Analysis completed.\n")
    print("--------------------------------------------------\n")
    