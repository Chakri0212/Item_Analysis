#!/usr/bin/env python
# coding: utf-8

import requests
import hashlib
import json
import os
import zipfile
import io
import time
import sys
from datetime import date
from . import passcode
#import passcode



API_URL = 'https://knowledge.eng.kaplan.com/bjorne/itemLevelData/service.php'
start_time = 0
end_time = 0

def setup_dirs(params):
    PATH = 'projects/' + params['report_name']
    if not os.path.exists(PATH):
        print(f"\tCreating directory: {PATH}")
        os.makedirs(PATH)
        os.makedirs(PATH+'/results')
        os.makedirs(PATH+'/01_data_pull')
        os.makedirs(PATH+'/02_data_cleaning')
        os.makedirs(PATH+'/03_item_analysis')
    else:
        print(f"\tDirectory already exists: {PATH}")

def create_secret():
    today = date.today().strftime("%Y-%m-%d")
    secret = passcode.secret
    api_token = hashlib.md5((today+secret).encode())
    auth_token = api_token.hexdigest()
    return auth_token

def create_job(auth_token, params):
    job = {"auth_token":auth_token,
            "command":"submit_job",    
            "input_variables": params
        }
    print(json.dumps(job,indent=2))
    return job

def run_job(job):
    response = requests.post(API_URL, data=json.dumps(job))
    response.raise_for_status()
    response_json = response.json()
    #print(f"job:{job}")
    print("--")
    print(f"response: {response}")
    print("--")
    print(f"response_json: {response_json}")
    return response_json


def get_status(auth_token, result):
    print(result)
    if result['success'] == 'New job created.':
        print("\tJob created")
        # Get job status
        params01 = {"auth_token":auth_token, "command":"get_job_status","job_id":result['job_id']}
        start_time = time.time()
        continue_loop = True
        while continue_loop:
            time.sleep(60)
            response01 = requests.get(API_URL, data=json.dumps(params01))
            result01 = response01.json()
            if 'current_step' in result01:
                if result01['current_step'] == 'completed':
                    continue_loop = False
            else:
                print(f"Warning: 'current_step' not found in response: {result01}")
                # Optionally, break or handle error as needed
                break
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(result01)
        return result01, elapsed_time
    else:
        print(f"Error: Job was not created")    
        
        
def save_data(final_result, params):
    # Download & extract data
    if "download_url" in final_result:
        download_url = final_result["download_url"]
        file = requests.get(download_url)
        z = zipfile.ZipFile(io.BytesIO(file.content))
        z.extractall(f"projects/{params['report_name']}/01_data_pull")
        return download_url
    else:
        print(f"Error: 'download_url' not found in final_result: {final_result}")
        return None
        
        
def run_code(params):
    print("-- 0: Setup --")
    setup_dirs(params)
    print('\tDONE')    
    print("-- 1. Data Pull --")   
    auth_token = create_secret()
    job1 = create_job(auth_token, params)
    result1 = run_job(job1)
    final_result1, elapsed_time1 = get_status(auth_token, result1)
    save_data(final_result1, params)
    print(f"\tData extraction completed successfully and saved data at {params['report_name']}/01_data_pull") 

    