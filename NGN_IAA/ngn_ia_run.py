#!/usr/bin/env python
# coding: utf-8

# # <center>NGN IA MAIN

# ### OVERVIEW
# This script takes in .json files with input variables of a Nursing project and automates running the 
# (1) data pull, 
# (2) data cleaning, and 
# (3) item analysis 
# 
# of that project.
# 
# 
# ### EXAMPLE 
# ngn_iaa_run(project_params)
# 
# ### PARAMETERS
# An example of the project_params variable:
# 
# ```
# project_params = {'report_type': 'full_item_level_data',
#  'report_name': 'EN_TEST_pediatric_b_ngn',
#  'start_date': '2022-11-01',
#  'end_date': '2022-12-01',
#  'date_range_for': 'activity completed',
#  'enrollment_ids': [],
#  'programs': ['NCLEX'],
#  'products': [],
#  'activity_types': [],
#  'activity_templates': ['Atom|pediatric-b-ngn'],
#  'include_alternate_timing': None,
#  'include_tutor_mode': None,
#  'include_extra_qbank_scores': None,
#  'include_item_yield': None,
#  'user_email': 'eryka.nosal@kaplan.com'}
# ```
# 
# 
# ### RETURNS
# The following directories will be created:
# 
# ```
# projects/{project_params['report_name']/01_data_pull
# projects/{project_params['report_name']/02_data_cleaning
# projects/{project_params['report_name']/03_item_analysis
# ```
# 
# The following files will be created in the folder '01_data_pull':
# 
# ```
# data/activity_info.tsv
# data/content_info_Atom.tsv
# data/response_dat.tsv
# supplemental/headers.txt
# supplemental/input_variables.json
# supplemental/query_activity_info.sql
# supplemental/query_content_info_Atom.sql
# supplemental/query_response_info_Atom_sample.sql
# 
# ```
# 
# The following files will be created in the folder '02_data_cleaning':
# ```
# {project_params['report_name']}_activity_Level_Info.csv
# {project_params['report_name']}_Cleaning_info.txt
# {project_params['report_name']}_cleaningInfo.csv
# {project_params['report_name']}_Content_Item_Info.csv
# {project_params['report_name']}_rejects_info.csv
# {project_params['report_name']}_responseData.csv
# {project_params['report_name']}_User_Level_Info.csv
# {project_params['report_name']}_User_level_Item_Scores.csv
# {project_params['report_name']}_User_level_milliseconds_per_Item.csv
# {project_params['report_name']}_User_level_responses.csv
# 
# ```
# 
# 
# The following files will be created in the folder '03_item_analysis':
# TBD
# 
# 
# ### VERSION HISTORY
# * v0.10 (April 2025): Created. Currently only runs (1) and (2)

# In[1]:


import pandas as pd
import numpy as np
import json
import os
import sys


from Codes.s01_data_pull import data_pull
from Codes.s02_data_cleaning import data_cleaning_driver
#from Automation_code.s03_item_analysis import item_analysis_script
import warnings 
warnings.filterwarnings("ignore")


# # Function Definition

# In[2]:


def ngn_iaa_run(params):
    #data_pull.run_code(params)
    data_cleaning_driver.run_code(params)
    #item_analysis_script.run_code(params)


# # Run

# In[3]:


with open('input_variables_challenge_test4.json') as t01:
    p1 = json.load(t01)
    
with open('test_input_variables_02.json') as t02:
    p2 = json.load(t02)


# In[4]:


p1


# In[5]:


ngn_iaa_run(p1)


# In[6]:


ngn_iaa_run(p2)

