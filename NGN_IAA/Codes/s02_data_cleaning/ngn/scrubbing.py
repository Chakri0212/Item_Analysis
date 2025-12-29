import pandas as pd
import numpy as np

import getpass as gt
import psycopg2
from sqlalchemy import create_engine, sql

import shutil
import os
from openpyxl import load_workbook

import re
import datetime
import warnings
import sys

import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

from . import salvador
from .salvador import color

#DRCR Code
#================================================================================================

#below function will remove dups and sorts the options selected in a response
#also this will encode alphabet responses to numbers
#No, need of sorting for items of type "order-interaction" items, need to be careful for such types.
#better to remove sorting overall as we are in good faith that we capture as is
def resp_cleaning(resp, re_order = True):
    #output of this function will be string
    resp_list = str(resp).split('|')
    cln_resp_list = []
    for resp_part in resp_list:
        #an error may rise here if the response is a string
        #add np.sort after ','.join function to include sorting feature
        #to remove space around each choice input
        resp_part = ','.join([x.strip() for x in resp_part.split(',')])
        if re_order:
            cln_resp_part = ','.join(np.sort(list(set([str(ord(x.lower())-96) if x.isalpha() else x for x in resp_part.split(',')]))))
        else:
            cln_resp_part = ','.join(list(dict.fromkeys([str(ord(x.lower())-96) if x.isalpha() else x for x in resp_part.split(',')])))
            
        cln_resp_list.append(cln_resp_part)
        
    cln_resp_list = [x.strip() for x in cln_resp_list]
    final_resp = '|'.join(cln_resp_list)
    
    return final_resp.strip()

def describe_dupe_cor_ans(df, no_correctAnswer = False, use_contentItemName = False):
    if (use_contentItemName == True):
        if (no_correctAnswer == True):
            cadf = df[(df['score']==1) & (df['response']!=0) & (df['response'].notnull())][['contentItemName', 'response']].drop_duplicates().sort_values(by=['contentItemName'], ignore_index = True)
            cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
        else:
            cadf = df[df['correctAnswer'].notnull()][['contentItemName', 'correctAnswer']].drop_duplicates()
       
        if (cadf['contentItemName'].nunique() < df['contentItemName'].nunique()):
            cadf = df[df['score']==1][['contentItemName', 'response']].drop_duplicates()
            cadf.rename(columns={'response':'correctAnswer'}, inplace = True)
        
        #we get the second instance of duplicate item names with duplicated() fn
        dupe_itemIds = cadf[cadf['contentItemName'].duplicated()]['contentItemName'].unique()
        if (len(dupe_itemIds)==0):
            sys.exit(salvador.color.BOLD + 'No duplicate correct answer'  + salvador.color.END)
            
        df_to_summarize = df[(df['contentItemName'].isin(dupe_itemIds)) & (df['score']==1)]
        outputdf = df_to_summarize.fillna('None').groupby(by=['contentItemName', 'response'], as_index = False).agg(score_count = ('jasperUserId', 'nunique'), min_date = ('dateCreated', 'min'), max_date = ('dateCreated', 'max'))
        
        return outputdf
    else:
        if (no_correctAnswer == True):
            cadf = df[(df['score']==1) & (df['response']!=0) & (df['response'].notnull())][['contentItemId', 'response']].drop_duplicates().sort_values(by=['contentItemId'], ignore_index = True)
            cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
        else:
            cadf = df[df['correctAnswer'].notnull()][['contentItemId', 'correctAnswer']].drop_duplicates()
        
        if (cadf['contentItemId'].nunique() < df['contentItemId'].nunique()):
            cadf = df[df['score']==1][['contentItemId', 'response']].drop_duplicates()
            cadf.rename(columns = {'response':'correctAnswer'})
            
        dupe_itemIds = cadf[cadf['contentItemId'].duplicated()]['contentItemId'].unique()
        
        if (len(dupe_itemIds)==0):
            sys.exit(salvador.color.BOLD + 'No duplicate correct answers'  + salvador.color.END)
        
        df_to_summarize = df[(df['contentItemId'].isin(dupe_itemIds)) & (df['score']==1)]
        
        if(len(df_to_summarize['dateCreated'])!=0):
            outputdf = df_to_summarize.fillna('None').groupby(by=['contentItemId', 'response'], as_index = False).agg(total = ('jasperUserId', 'nunique'), min_date = ('dateCreated', 'min'), max_date = ('dateCreated', 'max'))
        else:
            outputdf = df_to_summarize.fillna('None').groupby(by=['contentItemId', 'response'], as_index = False).agg(total = ('jasperUserId', 'nunique'), min_date = ('dateCreated', 'min'), max_date = ('dateCreated', 'max'))
        return outputdf
        
def cor_ans(df):
    #cor_ans_df = df[(df['score']==1) & (df['response']!=0) & (df['response'].notnull())][['contentItemId', 'contentItemName', 'displaySeq', 'response', 'dateCreated']]
    #cor_ans_df = cor_ans_df.groupby(by=['contentItemId', 'contentItemName', 'displaySeq', 'response'])['dateCreated'].agg(['max']).reset_index().sort_values(by=['displaySeq'], ignore_index = True)
    #cor_ans_df['rank'] = cor_ans_df.groupby(by=['contentItemId', 'contentItemName'])['max'].rank(ascending = False)
    #cor_ans_df = cor_ans_df[cor_ans_df['rank']==1].drop(columns=['rank', 'max'])
    #cor_ans_df.rename(columns={'response':'correctAnswer'}, inplace = True)
    
    cor_ans_df = df[(df['score']==1) & (df['response']!=0) & (df['response'].notnull())][['contentItemId', 'contentItemName', 'displaySeq', 'response', 'dateCreated']].drop_duplicates(ignore_index=True)
    cor_ans_df = cor_ans_df.groupby(by=['contentItemId', 'contentItemName', 'displaySeq', 'response'], as_index = False).agg(max_dateCreated = ('dateCreated', 'max')).sort_values(by=['displaySeq'], ignore_index = True)
    cor_ans_df['rank'] = cor_ans_df.groupby(by=['contentItemId', 'contentItemName'])['max_dateCreated'].rank(ascending = False, method = 'first')
    cor_ans_df = cor_ans_df[cor_ans_df['rank']==1].drop(columns=['rank'])

    #grouping by content item name so that we can keep track of which version of itemname is present in corr_ans df so that same data can be present in content_item_info file
    cor_ans_df['rank'] = cor_ans_df.groupby(by=['contentItemName'])['max_dateCreated'].rank(ascending = False, method = 'first')
    cor_ans_df = cor_ans_df[cor_ans_df['rank']==1].drop(columns=['rank'])

    cor_ans_df.rename(columns={'response':'correctAnswer'}, inplace = True)
    return cor_ans_df

def get_item_cor_ans(df, no_correctAnswer = False, use_contentItemName = False):
    if(use_contentItemName == True):
        if (no_correctAnswer == True):
            cadf = df[(df['score']==1) & (df['response']!=0) & (df['response'].notnull())][['contentItemName', 'response']].drop_duplicates().sort_values(by=['contentItemName'], ignore_index = True)
            cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
            
            if(cadf['contentItemName'].nunique() < cadf.shape[0]):
                dup_ca_count = describe_dupe_cor_ans(df, no_correctAnswer, use_contentItemName = use_contentItemName)
                # print(dup_ca_count)
                print(salvador.color.BOLD + 'Duplicate correct answers for some items'  + salvador.color.END)
                dup_cadf = cadf[cadf['contentItemName'].duplicated(keep = False)].reset_index(drop = True)
                cor_ans_df = cor_ans(df)
                return cor_ans_df, dup_cadf, dup_ca_count
            else:
                print(salvador.color.BOLD + 'No duplicate correct answer'  + salvador.color.END)
                dup_cadf = cadf.groupby(by=['contentItemName']).filter(lambda x: len(x)>1).reset_index(drop=True)
                cor_ans_df = cor_ans(df)
                return cor_ans_df, dup_cadf, None
        else:
            cadf = df[df['correctAnswer'].notnull()][['contentItemName', 'correctAnswer']].drop_duplicates().sort_values(by=['contentItemName'], ignore_index = True)
            
            if (cadf['contentItemName'].nunique() < df[df['correctAnswer'].notnull()]['contentItemName'].nunique()):
                cadf = df[df['score']==1][['contentItemName', 'response']].drop_duplicates(ignore_index = True)
                cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
            
            if (cadf['contentItemName'].nunique() < cadf.shape[0]):
                dup_ca_count = describe_dupe_cor_ans(df, no_correctAnswer, use_contentItemName = use_contentItemName)
                print(dup_ca_count)
                print(salvador.color.BOLD + 'Duplicate correct answers for some items'  + salvador.color.END)
                dup_cadf = cadf[cadf['contentItemName'].duplicated(keep = False)].reset_index(drop = True)
                return cadf, dup_cadf, dup_ca_count
            
            else:
                print(salvador.color.BOLD + 'No duplicate correct answer'  + salvador.color.END)
                dup_cadf = cadf[cadf['contentItemName'].duplicated(keep = False)].reset_index(drop=True)
                return cadf, dup_cadf, None
    else:
        #use_contentItemName = False
        if (no_correctAnswer):
            cadf = df[(df['score']==1) & (df['response']!=0) & (df['response'].notnull())][['contentItemId', 'response']].drop_duplicates().sort_values(by=['contentItemId'], ignore_index = True)
            cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
            
            if(cadf['contentItemId'].nunique() < cadf.shape[0]):
                dup_ca_count = describe_dupe_cor_ans(df, no_correctAnswer,use_contentItemName = use_contentItemName)
                print(dup_ca_count)
                print(salvador.color.BOLD + 'Duplicate correct answers for some items'  + salvador.color.END)
                dup_cadf = cadf[cadf['contentItemId'].duplicated(keep = False)].reset_index(drop = True)
                cor_ans_df = cor_ans(df)
                return cor_ans_df, dup_cadf, dup_ca_count
            
            else:
                print(salvador.color.BOLD + 'No duplicate correct answer'  + salvador.color.END)
                dup_cadf = cadf[cadf['contentItemId'].duplicated(keep = False)].reset_index(drop = True)
                cor_ans_df = cor_ans(df)
                return cor_ans_df, dup_cadf, None
        else:
            cadf = df[df['correctAnswer'].notnull()][['contentItemId', 'correctAnswer']].drop_duplicates().sort_values(by=['contentItemId'], ignore_index = True)
            
            if (cadf['contentItemId'].nunique() < df[df['correctAnswer'].notnull()]['contentItemId'].nunique()):
                cadf = df[df['score']==1][['contentItemId', 'response']].drop_duplicates(ignore_index = True)
                cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
            
            if (cadf['contentItemId'].nunique() < cadf.shape[0]):
                dup_ca_count = describe_dupe_cor_ans(df, no_correctAnswer,use_contentItemName = use_contentItemName)
                print(dup_ca_count)
                print(salvador.color.BOLD + 'Duplicate correct answers for some items'  + salvador.color.END)
                dup_cadf = cadf[cadf['contentItemId'].duplicated(keep = False)].reset_index(drop = True)
                return cadf, dup_cadf, dup_ca_count
            else:
                print(salvador.color.BOLD + 'No duplicate correct answer'  + salvador.color.END)
                dup_cadf = cadf[cadf['contentItemId'].duplicated(keep = False)].reset_index(drop = True)
                return cadf, dup_cadf, None


def recode_as_omitted(df, omit_condition = pd.Series(dtype = 'float')):
    if (omit_condition.empty):
        warnings.warn('No omit_condition provided.')
    if(len(set(df.columns).intersection(set(['orig_response', 'orig_score'])))==0):
        #There are no columns called orig_response, orig_score, create them before changing any data
        df['orig_response'] = df['response']
        df['orig_score'] = df['score']
    if(sum(omit_condition.isnull())>0):
        omit_condition[omit_condition.isnull()] = False
        warnings.warn('Null omit condition defaulted to False')
    if(len(omit_condition)>0):
        #change responses to 0
        df.loc[omit_condition, 'response'] = 0
        #change score to 0
        df.loc[omit_condition, 'score'] = 0
        #change attempted to False
        df.loc[omit_condition, 'attempted'] = False
        #change responseStatus to 'omitted' if it is null
        df.loc[omit_condition,  'responseStatus'] = 'omitted'
        # & (df['attempted']==False) & (df['responseStatus'].isnull()
    else:
        warnings.warn('Nothing recoded due to entirely False omit_condition')
    return df
    
def timing_exclusion(df, mSec_min_threshold = None, mSec_max_threshold = None, sec_min_threshold = None, sec_max_threshold = None):
    df_time = df[df['response']!=0]
    
    if((mSec_min_threshold == None) and (sec_min_threshold == None) and (mSec_max_threshold == None) and (sec_max_threshold == None)):
        ## this whole first part is only if arguments are not specified.
        ## When called by another function, one of the thresholds should always be specified.
        if(sum(['mSecUsed' in df_time.columns])>0):
            print("Here's what the distribution of timing looks like in milliseconds for questions answered:")
            print(df_time['mSecUsed'].describe())
            print('There are ',df_time[df_time['mSecUsed']<=0].shape[0],' actual responses where time spent was 0 milliseconds or less.')
            
            minTime = int(input('What is the minimum time allowed in milliseconds? '))
            print('This will change ', df[df['mSecUsed'] <= minTime].shape[0], " responses to 'omit' status.")
            
            cont = input('Continue? Y/N :').lower()
            
            if(cont=='y'):
                df = recode_as_omitted(df, omit_condition = df['mSecUsed'] <= minTime)
            else:
                print('No reponse changed.')
        
        elif(sum(['secUsed' in df_time])>0):
            print("Here's what the distribution of timing looks like in seconds for questions answered:")
            print(df_time['secUsed'].describe())
            print('There are ',df_time[df_time['secUsed']<=0].shape[0],' actual responses where time spent was 0 seconds or less.')
            
            minTime = int(input('What is the minimum time allowed in seconds? '))
            print('This will change ', df[df['secUsed'] <= minTime].shape[0], " responses to 'omit' status.")
            
            cont = input('Continue? Y/N :').lower()
            
            if(cont=='y'):
                df = recode_as_omitted(df, omit_condition = df['secUsed'] <= minTime)
            else:
                print('No reponse changed.')
        
        else:
            print('no time field found')
    
    elif((mSec_min_threshold != None) and (sum(['mSecUsed' in df_time.columns])>0)):
        df = recode_as_omitted(df, omit_condition = df['mSecUsed'] <= mSec_min_threshold)
    elif((sec_min_threshold != None) and (sum(['secUsed' in df_time.columns])>0)):
        df = recode_as_omitted(df, omit_condition = df['secUsed'] <= sec_min_threshold)
    elif(mSec_min_threshold != None):
        warnings.warn('Data frame does not contain mSecUsed column. No questions recoded for minimum timing.')
    elif(sec_min_threshold != None):
        warnings.warn('Data frame does not contain secUsed column. No questions recoded for minimum timing.')
    
    if((mSec_max_threshold != None) and (sum(['mSecUsed' in df_time.columns])>0)):
        df = recode_as_omitted(df, omit_condition = df['mSecUsed'] > mSec_max_threshold)
    elif((sec_max_threshold != None) and (sum(['secUsed' in df_time.columns])>0)):
        df = recode_as_omitted(df, omit_condition = df['secUsed'] > sec_max_threshold)
    elif(mSec_max_threshold != None):
        warnings.warn('Data frame does not contain mSecUsed column. No questions recoded for maximum timing.')
    elif(sec_max_threshold != None):
        warnings.warn('Data frame does not contain secUsed column. No questions recoded for maximum timing.')
    
    return df


def remove_repeat_questions(df, rejects_df, remove = False, add_col = False):
    ## order by the times the person saw the question - dates on sequences.
    df['quesRank'] = df.groupby(by=['contentItemId', 'jasperUserId'])['dateCreated'].rank(method = 'first')
    
    if(remove==False):
        df = recode_as_omitted(df, omit_condition = (df['quesRank']>1))
        if(add_col == True):
            if(sum(df.columns=='repeatOmitted')==0):
                df['repeatOmitted'] = df['quesRank']>1
                
        outputdf = df.drop(columns = ['quesRank'])
    
    elif(remove == True):
        #adding data to rejects df
        temp_rej = df[df['quesRank'] > 1][['jasperUserId', 'contentItemId']].drop_duplicates()
        temp_rej.rename(columns={'contentItemId':'kbsEnrollmentId'}, inplace = True)
        temp_rej['Reason'] = '2nd or later instances of items for users removed'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
        removed = df[df['quesRank'] > 1]
        df = df[df['quesRank']==1]
        outputdf = df.drop(columns = ['quesRank'])
    
    return outputdf, rejects_df

def combine_CIinfo(path, respdf, cidf = pd.DataFrame(), ci_cols_to_include = [], interaction_type_list = 1):
    
    #read in content info file
    if(cidf.empty == True):
        cidf = pd.read_csv(path + 'contentItemInfo.tsv', sep = '\t')
    
    #turn all initial letters in column headers to lowercse
    cidf.columns = [col[0].lower()+col[1:] for col in cidf.columns]
    
    cidf = cidf.drop_duplicates()
    
    cidf = cidf[cidf['interactionTypeId'].isin(interaction_type_list)]
    
    columnsToAdd = ['contentItemName', 'contentItemId']
    columnsToAdd = columnsToAdd + ci_cols_to_include
    
    columnsdf = cidf[columnsToAdd]
    
    outputdf = pd.merge(respdf, columnsdf)
    
    return outputdf

def removed_record_count(df, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current, num_responses_current, things_to_say = 'Sequence removed', include_item_count = False):
    
    num_seq_new = df['sequenceId'].nunique()
    num_users_new = df['jasperUserId'].nunique()
    num_items_new = df['contentItemName'].nunique()
    num_responses_new = df.shape[0]
    
    if ((num_seq_current-num_seq_new)!=0 and (num_responses_current-num_responses_new)!=0):
        print(things_to_say, num_seq_current-num_seq_new)
        print('Users removed ', num_users_current-num_users_new)
        print('Unique items removed ', num_items_current-num_items_new)
        print('Responses removed ', num_responses_current-num_responses_new)
        print('')
        
        cleaning_info.loc[len(cleaning_info)] = [sec, sub_sec, things_to_say, num_seq_current-num_seq_new,
                                                 num_users_current-num_users_new, num_items_current-num_items_new,
                                                    num_responses_current-num_responses_new]
        
    else:
        print(things_to_say + 'NONE\n')
        cleaning_info.loc[len(cleaning_info)] = [sec, sub_sec, things_to_say, 'None', 'None', 'None', 'None']
    
    num_seq_current = num_seq_new
    num_users_current = num_users_new
    num_items_current = num_items_new
    num_responses_current = num_responses_new
    
    return num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info


def clean_item_data(data_path = '_',
                    results_path = '_',
                    analysis_name = '_',
                    resp = pd.DataFrame(),
                    remove_users_deleted_sequences = True,
                    remove_dup_CIs = True,
                    remove_no_kbsEID = True,
                    remove_deleted_sequences = True,
                    remove_impo_response_scored = True,
                    remove_impo_timing_seq = True,
                    remove_seq_w_tmq = False,
                    remove_staged_responses = False,
                    remove_FT_items = False,
                    data_pool = dict(),
                    CI_remove_before_after = 'before',
                    repeat_treatment = 'omit',
                    mSec_min_threshold = None,
                    mSec_max_threshold = None,
                    sec_min_threshold = None,
                    sec_max_threshold = None,
                    remove_frt_users = True,
                    remove_olc_users = True,
                    remove_repeat_enrolls = True,
                    remove_tutor = True,
                    remove_ada_seq = True,
                    remove_untimed_seq = True,
                    remove_incomplete_seq = True,
                    seq_item_minutes_threshold = None,
                    seq_section_minutes_threshold = None,
                    seq_total_minutes_threshold = None,
                    qbank = False,
                    min_items_per_seq = None,
                    section_calc = True,
                    #seq_item_resp_threshold = None,
                    remove_unscored = False,
                    precombined_files = False,
                    remove_repeat_test_administrations = False,
                    remove_seq_wo_dispseq = True,
                    remove_over_time_sequences = True):
    
    CI_old_version_dates = data_pool.get('CI_old_version_dates', pd.DataFrame())
    CI_old_version_list = data_pool.get('CI_old_version_list', pd.DataFrame())
    CI_old_keys = data_pool.get('CI_old_keys', pd.DataFrame())
    frt_enrols = data_pool.get('frt_enrols', pd.DataFrame())
    olc_enrols = data_pool.get('olc_enrols', pd.DataFrame())
    repeaters = data_pool.get('repeaters', pd.DataFrame())
    section_map = data_pool.get('section_map', pd.DataFrame())
    test_map = data_pool.get('test_map', pd.DataFrame())
    seqHist_to_exclude = data_pool.get('seqHist_to_exclude', pd.DataFrame())
    cidf = data_pool.get('cidf', pd.DataFrame())
    field_test_items = data_pool.get('field_test_items', pd.DataFrame())
    ci_cols_to_include = data_pool.get('ci_cols_to_include', pd.DataFrame())
    
    if (test_map.empty and section_map.empty):
        sys.exit('No section_map or test_map')
    
    pre_ins = sys.stdout
    
    sys.stdout = open(results_path + analysis_name +'_Cleaning_info.txt', 'w')
    
    print('Starting clean item data function at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    if(resp.empty):
        sys.exit('No response df')
    
    cleaning_info = pd.DataFrame(columns = ['Section', 'Sub_section', 'Condition',
                           'value', 'Users removed', 'Unique items removed', 'Responses removed'])
    
    rejects_df = pd.DataFrame(columns=['jasperUserId', 'kbsEnrollmentId', 'templateId', 'Reason'])
    
    num_seq_current = resp['sequenceId'].nunique()
    num_users_current = resp['jasperUserId'].nunique()
    num_items_current = resp['contentItemName'].nunique()
    num_responses_current = resp.shape[0]
    
    
    print('Total sequences at start: ', num_seq_current)
    print('Total users at start: ', num_users_current)
    print('Unique items at start: ', num_items_current)
    print('Total responses at start: ', num_responses_current)
    print('')
    
    #this needed if we want to analyse only sections from given df
    if (section_map.empty == False):  
        #adding data to rejects df
        temp_rej = resp[~resp['sectionName'].isin(section_map['jasperSectionName'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'section filtered from input'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        #filtering only sections that are in the section map
        respExcl = resp[resp['sectionName'].isin(section_map['jasperSectionName'])]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                    num_responses_current
                                                                                    ,things_to_say = 'Sequences without responses specified in the section map, removed: ')
        
    else:
        respExcl = resp
    
    
    
    print('Working on Disqualifiers :')
    print('='*30)
    print('User Removal')
    print('-'*20)
    sec = 'Disqualifiers'
    sub_sec = 'User Removal'
    respExcl['deleted_name'] = respExcl['sequenceName'].apply(lambda x: True if (re.search(r'\d', x) and re.search(r'_d$', x)) else False)

    users_w_deletes = respExcl[(respExcl['deleted_name']==True) | (respExcl['sequenceStatus']=='reset')][['jasperUserId',
                                                                                             'kbsEnrollmentId',
                                                                                             'sequenceStatus',
                                                                                             'deleted_name',
                                                                                             'sequenceName',
                                                                                             'templateId']].drop_duplicates().copy()
    
    #Users with any deleted requested sequence(s)
    if(remove_users_deleted_sequences == True):
        #adding data to rejects df
        temp_rej = respExcl[(respExcl['jasperUserId'].isin(users_w_deletes['jasperUserId'].unique()))][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Users with deleted requested sequence(s)'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        #below code removes entire users from response_data
        respExcl = respExcl[~(respExcl['jasperUserId'].isin(users_w_deletes['jasperUserId'].unique()))]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                                                             things_to_say = 'Sequences with deleted names are removed: ')
        
    
    
    #Users with sequences containing duplicate content items
    #Excluding sequences that have a single content item more than once (after filtering out tutorials/breaks/staged)
    if(remove_dup_CIs == True):
        seq_to_exclude_calc4 = respExcl[respExcl['contentItemId']!=-1].copy()
        seq_to_exclude_calc4['count'] = seq_to_exclude_calc4.groupby(by=['sequenceId', 'contentItemName'])['contentItemName'].transform('count')
        seq_to_exclude_calc4 = seq_to_exclude_calc4[seq_to_exclude_calc4['count']>1]['sequenceId'].unique()
    
        if(len(seq_to_exclude_calc4) > 0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc4)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences containing duplicate content items'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc4)]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                , things_to_say = 'Sequences with dupe content items within the same exam, removed: ')
    
    #Users with no KBS EID
    #only enrollments are removed
    if (remove_no_kbsEID == True):
        #adding data to rejects df
        temp_rej = respExcl[respExcl['kbsEnrollmentId']=='0'][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Enrollments with null Eid'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
        respExcl = respExcl[respExcl['kbsEnrollmentId']!='0']
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = 'Sequences with no KBS EID removed: ')
    
    #Users with non null KBS enrollment IDs and multiple Jasper/Other system user IDs
    #no code for this
    
    print('Sequence Removal')
    print('-'*20)
    sub_sec = 'Sequence Removal'
    #Sequence(s) sharing a template with a deleted sequence for that user
    if(remove_deleted_sequences == True):
        seq_deleted = respExcl[(respExcl['deleted_name']==True) | (respExcl['sequenceStatus']=='reset')]['sequenceId'].unique()
        
        if(len(seq_deleted)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_deleted)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with a deleted seq name'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_deleted)]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = 'Sequences with deleted names are removed: ')
    
    #Sequences with impossibly scored responses: response=0 and score=1
    if (remove_impo_response_scored == True):
        
        #making response = 0 for empty responses and got score
        respExcl.loc[(respExcl['response'].isnull()) & (respExcl['score']==1), 'response'] = 0
        # Excluding sequences that have weird response records - scored as correct without a response
        seq_to_exclude_calc1 = respExcl[(respExcl['score']==1) & (respExcl['response'] == 0)]['sequenceId'].unique()
        
        if(len(seq_to_exclude_calc1)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc1)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with impossibly scored responses: response=0 and score=1'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc1)]
                            
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                , things_to_say = 'Sequences with bad records (response = 0 with score = 1), removed: ')
    
    #Sequences with impossible timing: mSecUsed < 0
    if(remove_impo_timing_seq == True):
        seq_to_exclude_calc1_5 = respExcl[respExcl['mSecUsed']<0]['sequenceId'].unique()
        if(len(seq_to_exclude_calc1_5)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc1_5)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with impossible timing: mSecUsed < 0'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc1_5)]
                            
        num_seq_current, num_users_current, num_items_current, num_responses_current, leaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                          num_responses_current
                                                                                , things_to_say = 'Sequences with bad timing (mSecUsed < 0), removed: ')
    
    
    initial_columns = respExcl.columns
    #Sequences with too many questions in a section (not applicable for QBank)
    #preparing the response data for applying above condition
    if (section_map.empty == False):
        if (qbank == True):
            warnings.warn('Why do you have a section map for qbank ?')
            section_map_df = pd.DataFrame({'sectionName' : section_map['jasperSectionName'],
                                          'test_minutes_allowed' : section_map['minutesAllowed'],
                                          'test_response_threshold' : section_map['responseThreshold']})
        else:
            section_map_df = section_map.rename(columns = {'jasperSectionName' : 'sectionName',
                                                          'minutesAllowed' : 'test_minutes_allowed',
                                                          'responseThreshold' : 'test_response_threshold'})
        
        #making cols test_minutes allowed, test_response_threshold on sections from section_map
        respExcl = pd.merge(respExcl, section_map_df, how = 'inner')
        if(respExcl.empty):
            warnings.warn("response df is empty after merging with section_map df")
            
        #respExcl['actualNumQues'] = respExcl.groupby(by = ['sequenceId', 'sectionName'])['contentItemName'].transform('count')
    
    if (test_map.empty == False and qbank == False):
        ##prep to find sequences with bad records in them or too much time in a section, or multiple items seen in one test,
        # or too many items in a section(repeated positions) for total exclusion
        respExcl = pd.merge(respExcl, pd.DataFrame({'sequenceName' : test_map['jasperSequenceName'],
                                                    'test_minutes_allowed' : test_map['minutesAllowed'],
                                                    'test_num_ques' : test_map['numQues'],
                                                    'test_response_threshold' : test_map['responseThreshold']}))
        if(respExcl.empty):
            warnings.warn("response df is empty after merging with test_map df on 'sequenceName'")
                            
        #respExcl['actualNumQues'] = respExcl.groupby(by = ['sequenceId', 'sequenceName'])['contentItemName'].transform('count')
                            
    elif (test_map.empty == False and qbank == True):
        temp_record_check = respExcl.shape[0]
        respExcl = pd.merge(respExcl, pd.DataFrame({'sequenceName' : test_map['jasperSequenceName'],
                                            'test_minutes_allowed' : test_map['minutesAllowed'],
                                            'test_num_ques' : test_map['numQues'],
                                            'test_response_threshold' : test_map['responseThreshold']}))
        #respExcl['actualNumQues'] = respExcl.groupby(by = ['sequenceId', 'sequenceName'])['contentItemName'].transform('count')

        if(respExcl.empty):
            warnings.warn("response df is empty after merging with test_map df")
            
        
        
        if (temp_record_check != respExcl.shape[0]):
            sys.exit('Too many things in test_map, probably')
                            
    print('Here are the new columns after joining all the test and section maps')
    print(set(respExcl)^set(initial_columns))
    print('')
    
    respExcl['actualNumQues'] = respExcl.groupby(by = ['sequenceId', 'sequenceName'])['contentItemName'].transform('count')
        
    #Sequences with too many questions in a section (not applicable for QBank)
    if(remove_seq_w_tmq == True):
        if(qbank==False):
            seq_to_exclude_calc3 = respExcl[respExcl['actualNumQues'] > respExcl['test_num_ques']]['sequenceId'].unique()
            
            if(len(seq_to_exclude_calc3) > 0):
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc3)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with too many questions in a section'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc3)]
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current
                                                                                    , things_to_say = 'Sequences with too many questions in a section, removed: ')
        else:
            print('You are asking if there are too many questions for a qbank which is bad :(')
    

    #remove_over_time_sequences
    if(remove_over_time_sequences):
        if(qbank == False):
            #exclude sequences that took longer than specified time to complete
            over_time_seq = respExcl.copy()
            over_time_seq['test_sum_time'] = over_time_seq.groupby(by=['sequenceId'])['mSecUsed'].transform('sum')/60000
            over_time_seq = over_time_seq[over_time_seq['test_sum_time'] > over_time_seq['test_minutes_allowed']]['sequenceId'].unique()
        
            if(len(over_time_seq)>0):
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(over_time_seq)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences taking longer than '+ ', '.join(str(x) for x in respExcl['test_minutes_allowed'].unique()) + ' minutes to complete, removed'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
                respExcl = respExcl[~respExcl['sequenceId'].isin(over_time_seq)]
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences taking longer than '+ salvador.color.BOLD  + ', '.join(str(x) for x in respExcl['test_minutes_allowed'].unique()) +  salvador.color.END + ' minutes to complete, removed: ')

    #################################

    print('Item Removal')
    print('-'*20)
    sub_sec = 'Item Removal'
    #num_item_responses = respExcl.shape[0]
    print('Total item responses: ', num_responses_current,'\n')
    
    #Staged response records (for CATs only)
    respExcl['attempted'] = np.where(respExcl['response'].isnull(), False, respExcl['response']!=0)
    
    if(remove_staged_responses == True):
        #adding data to rejects df
        temp_rej = respExcl[respExcl['contentItemId']==-1][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Staged response records'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
        respExcl = respExcl[respExcl['contentItemId']!=-1] # these are staged records and do not represent a question that was viewed
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current
                                                                                    , things_to_say = 'Staged response records removed : ')
    

    sub_sec = 'Item Removal'
    #Field test / experimental items:
    if(remove_FT_items == True):
        if (field_test_items.empty == False):
            respExcl['FT'] = respExcl['contentItemName'].isin(field_test_items['contentItemName'])
            
            #adding data to rejects df
            temp_rej = respExcl[respExcl['FT']!=False][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Field test / experimental items'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
            respExcl = respExcl[respExcl['FT']==False]
            #removed items that represents field test items from responses
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current
                                                                                    , things_to_say = 'Field Test items/Responses removed : ')
        
        else:
            print('No Field test items provided\n')
    
    print('Working on Cleaning Rules :')
    print('='*30)
    
    
    print('Response / Score Re-coding')
    print('-'*20)
    sec = 'Cleaning Rules'
    sub_sec = 'Response/Score Re-coding'
    
    #2nd and later instances of a repeated item coded as omitted
    remove_value = False
    if (repeat_treatment not in ['omit', 'remove', 'ignore']):
        print("Unknown repeat_treatment value. Allowed values include 'omit','remove', and 'ignore'. Repeat treatment is skipped for now. Repeated questions are recorded as omit by default.")
    elif(repeat_treatment == 'omit'):
        remove_value = False
    elif(repeat_treatment == 'remove'):
        remove_value = True
    elif(repeat_treatment == 'ignore'):
        remove_value = None
    print('Remove repeat item responses, instead of recoding as omitted = ', remove_value)
    
    num_items_omitted = respExcl[respExcl['attempted'] == False].shape[0]
    num_seq_w_omitted = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
    num_users_w_omitted = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
    num_items_omitted_new = 0
    num_seq_w_omitted_new = 0
    num_users_w_omitted_new = 0
    print('Original number of responses omitted', num_items_omitted)
    print('Original number of seq w items omitted', num_seq_w_omitted)
    print('Original number of users w items omitted', num_users_w_omitted, '\n')
    
    
    #Items with inconsistent keying/suggesting multiple versions of answer key coded as omitted
    #treating old version of items based on dates
    if (CI_old_version_dates.empty == False):
        # If items were under an earlier version the response should be recoded as omitted
        if(sum(CI_old_version_dates.columns == 'contentItemName')>0):
            for row, val in enumerate(CI_old_version_dates['contentItemName']):
                
                if(CI_remove_before_after=='before'):
                    date_cond = respExcl['dateCreated'] < CI_old_version_dates['cutoff_date'].loc[row]
                elif(CI_remove_before_after=='after'):
                    date_cond = respExcl['dateCreated'] > CI_old_version_dates['cutoff_date'].loc[row]
                            
                respExcl = recode_as_omitted(respExcl,
                                            omit_condition = ((respExcl['contentItemName']==CI_old_version_dates['contentItemName'].loc[row]) & (date_cond)))
        
        elif(sum(CI_old_version_dates.columns == 'contentItemId')>0):
            for row, val in enumerate(CI_old_version_dates['contentItemId']):
                if(CI_remove_before_after=='before'):
                    date_cond = respExcl['dateCreated'] < CI_old_version_dates['cutoff_date'].loc[row]
                elif(CI_remove_before_after=='after'):
                    date_cond = respExcl['dateCreated'] > CI_old_version_dates['cutoff_date'].loc[row]

                respExcl = recode_as_omitted(respExcl,
                                            omit_condition = ((respExcl['contentItemId']==CI_old_version_dates['contentItemId'].loc[row]) & (date_cond)))
        
        num_items_omitted_new = respExcl[respExcl['attempted'] == False].shape[0]
        num_seq_w_omitted_new = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
        num_users_w_omitted_new = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
        print('Item responses under previous version marked as omitted: ', num_items_omitted_new - num_items_omitted)
        print('Affected sequences : ', num_seq_w_omitted_new - num_seq_w_omitted)
        print('Affected users : ', num_users_w_omitted_new - num_users_w_omitted, '\n')
        
        num_items_omitted = num_items_omitted_new
        num_seq_w_omitted = num_seq_w_omitted_new
        num_users_w_omitted = num_users_w_omitted_new
        
    
    #treating old version of items based on item_ids
    if (CI_old_version_list.empty == False):
        # If items were under an earlier version the response should be recoded as omitted
        for row, val in enumerate(CI_old_version_list['contentItemId']):
            respExcl = recode_as_omitted(respExcl,
                                       omit_condition = respExcl['contentItemId']==CI_old_version_list['contentItemId'].loc[row])
        
        num_items_omitted_new = respExcl[respExcl['attempted'] == False].shape[0]
        num_seq_w_omitted_new = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
        num_users_w_omitted_new = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
        print('Item responses under previous version (from id list) marked as omitted: ', num_items_omitted_new - num_items_omitted)
        print('Affected sequences : ', num_seq_w_omitted_new - num_seq_w_omitted)
        print('Affected users : ', num_users_w_omitted_new - num_users_w_omitted, '\n')
        
        num_items_omitted = num_items_omitted_new
        num_seq_w_omitted = num_seq_w_omitted_new
        num_users_w_omitted = num_users_w_omitted_new
    
    #treating items answered with old version keys
    if (CI_old_keys.empty == False):
        # If items are scored from an earlier answer key, the response should be recoded as omitted
        #changing matching field to contentItemName as every time name remaining constant
        for row, val in enumerate(CI_old_keys['contentItemId']):
            #elig = respExcl[(respExcl['contentItemId']==CI_old_keys['contentItemId'].loc[row]) & 
            #                                              (respExcl['response'].fillna('None')==CI_old_keys['correctAnswer'].fillna('None').loc[row])]
            #elig_cnt = elig.shape[0]
            #omit_already = elig[elig['attempted']==False].shape[0]
            #print('Total matching', elig_cnt)
            #print('Already in omitted state for these rows', omit_already)
    
            #respExcl = recode_as_omitted(respExcl,
            #                            omit_condition = ((respExcl['contentItemId']==CI_old_keys['contentItemId'].loc[row]) & 
            #                                              (respExcl['response'].fillna('None')==CI_old_keys['correctAnswer'].fillna('None').loc[row])))
            
            respExcl = recode_as_omitted(respExcl,
                                        omit_condition = ( (respExcl['contentItemId']==CI_old_keys['contentItemId'].loc[row]) ))

        num_items_omitted_new = respExcl[respExcl['attempted']==False].shape[0]
        num_seq_w_omitted_new = respExcl[respExcl['attempted']==False]['sequenceId'].nunique()
        num_users_w_omitted_new = respExcl[respExcl['attempted']==False]['jasperUserId'].nunique()
        
        print('Already might have removed in earlier rules or already in omit state')
        print('Item responses with previous version of answer key marked as omitted: ', num_items_omitted_new - num_items_omitted)                                     
        print('Affected sequences: ', num_seq_w_omitted_new - num_seq_w_omitted)
        print('Affected users: ', num_users_w_omitted_new - num_users_w_omitted, '\n')
    
        num_items_omitted = num_items_omitted_new
        num_seq_w_omitted = num_seq_w_omitted_new
        num_users_w_omitted = num_users_w_omitted_new
    
    num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                       num_responses_current,
                                                                                 things_to_say = 'Number of sequences removed during item exclusions: ')
    
    
    
    
    #2nd and later instances of a repeated item coded as omitted
    if (remove_value!=None):
        respExcl, rejects_df = remove_repeat_questions(respExcl, rejects_df, remove = remove_value, add_col = True)
        
        num_items_omitted_new = respExcl[respExcl['attempted'] == False].shape[0]
        num_seq_w_omitted_new = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
        num_users_w_omitted_new = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
        
        print('Repeated items marked as omitted: ', num_items_omitted_new - num_items_omitted)
        print('Current number of responses omitted: ', num_items_omitted_new)
        print('Current number of seq w items omitted: ', num_seq_w_omitted_new)
        print('Current number of users w items omitted: ', num_users_w_omitted_new)
                                             
        print('Affected sequences: ', num_seq_w_omitted_new - num_seq_w_omitted)
        print('Affected users: ', num_users_w_omitted_new - num_users_w_omitted, '\n')

        num_items_omitted = num_items_omitted_new
        num_seq_w_omitted = num_seq_w_omitted_new
        num_users_w_omitted = num_users_w_omitted_new
                                             
        marked_omit_items = respExcl[respExcl['response'] != respExcl['orig_response']]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = 'Sequences with 2nd and later instances of a repeated item coded as omitted : ')
    
    
    #Last seen response coded as omitted, subsequent responses as not-reached
    #need to find code for this
    
    #Items with time "under" or "over" given threshold of seconds coded as omitted
    respExcl = timing_exclusion(respExcl, mSec_min_threshold = mSec_min_threshold, sec_min_threshold = sec_min_threshold,
                               mSec_max_threshold = mSec_max_threshold, sec_max_threshold = sec_max_threshold)
    #Responses given in less than the threshold allowed will be recoded as omitted
    num_items_omitted_new = respExcl[respExcl['attempted'] == False].shape[0]
    num_seq_w_omitted_new = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
    num_users_w_omitted_new = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
        
    print('Item response time under threshold of ', mSec_min_threshold, ' mSec or over ', mSec_max_threshold, ' mSec, marked as omitted: ', num_items_omitted_new - num_items_omitted)
    print('Current number of responses omitted: ', num_items_omitted_new)
    print('Current number of seq w items omitted: ', num_seq_w_omitted_new)
    print('Current number of users w items omitted: ', num_users_w_omitted_new)
                                             
    print('Affected sequences: ', num_seq_w_omitted_new - num_seq_w_omitted)
    print('Affected users: ', num_users_w_omitted_new - num_users_w_omitted,'\n')
    num_items_omitted = num_items_omitted_new
    num_seq_w_omitted = num_seq_w_omitted_new
    num_users_w_omitted = num_users_w_omitted_new
    
    num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                       num_responses_current,
                                                                                     things_to_say = 'Sequences with Items with time "under" or "over" given threshold of seconds coded as omitted : ')
    
    print('User Removal')
    print('-'*20)
    sub_sec = 'User Removal'
    #Users enrolled in specific products (Online companion, Free trials)
    if(remove_frt_users == True):
        if 'kbsenrollmentid' in frt_enrols.columns:
            temp_rej = respExcl[respExcl['kbsEnrollmentId'].isin(frt_enrols['kbsenrollmentid'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Users enrolled in Free Trials removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            respExcl = respExcl[~respExcl['kbsEnrollmentId'].isin(frt_enrols['kbsenrollmentid'])]
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current,
                                                                                         things_to_say = 'Sequences with enrollments in "Free Trial" product are removed: ')
        else:
            print("Warning: 'kbsenrollmentid' column missing in frt_enrols. Skipping Free Trial user removal.")
    
    if(remove_olc_users == True):
        if 'kbsenrollmentid' in olc_enrols.columns:
            temp_rej = respExcl[respExcl['kbsEnrollmentId'].isin(olc_enrols['kbsenrollmentid'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Users enrolled in Online companion removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            respExcl = respExcl[~respExcl['kbsEnrollmentId'].isin(olc_enrols['kbsenrollmentid'])]
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current,
                                                                                         things_to_say = 'Sequences with enrollments in "Online Companion" product are removed: ')
        else:
            print("Warning: 'kbsenrollmentid' column missing in olc_enrols. Skipping Online Companion user removal.")
    
    
    #Higher score guarantee or other repeat enrolls
    #We remove only erolls here not users
    if(remove_repeat_enrolls == True):
        if 'kbsenrollmentid' in repeaters.columns:
            temp_rej = respExcl[respExcl['kbsEnrollmentId'].isin(repeaters['kbsenrollmentid'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with repeated enrollments are removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            respExcl = respExcl[~respExcl['kbsEnrollmentId'].isin(repeaters['kbsenrollmentid'])]
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current,
                                                                                         things_to_say = 'Sequences with repeated enrollments are removed: ')
        else:
            print("Warning: 'kbsenrollmentid' column missing in repeaters. Skipping repeated enrolls removal.")
    
    #Total items taken fewer than set threshold removed (mostly for Qbank):
    #Fewer than ___ items attempted
    #Fewer than ___ %of items attempted across sequence
    #Fewer than ___ % of items attempted across sections(s)
    #have to make code for this
    
    
    print('Sequence Removal')
    print('-'*20)
    sub_sec = 'Sequence Removal'
    #Tutor mode sequences
    if(remove_tutor == True):
        #adding data to rejects df
        temp_rej = respExcl[(respExcl['tutorMode'] == True)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Tutor mode sequences'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[(respExcl['tutorMode'] != True) | (respExcl['tutorMode'].isnull())]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Tutor mode sequences removed: ')
    
    
    #Sequences administered under ADA accommodations
    #No-need to worry on this as we don't get much in the data
    #but need to make code for this
    if(remove_ada_seq == True):
        print('make code for this to remove ADA enrollemnts\n')
    
    #Untimed sequences
    #have to make code for this
    if(remove_untimed_seq == True):
        respExcl['untimedSequence'] = respExcl['sequenceName'].str.lower().str.endswith('_untimed')
        
        #adding data to rejects df
        temp_rej = respExcl[respExcl['untimedSequence']==True][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Untimed sequences'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[respExcl['untimedSequence']==False]
        respExcl.drop(columns=['untimedSequence'], inplace = True)
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Untimed sequences removed: ')
    
    
    #Timed sequences
    #condition is not clear for this
    
    #Incomplete sequences
    if(remove_incomplete_seq == True):
        #adding data to rejects df
        temp_rej = respExcl[(respExcl['sequenceStatus'] != 4) | (respExcl['sequenceStatus'] != 'completed')][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Incomplete sequences'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[(respExcl['sequenceStatus'] == 4) | (respExcl['sequenceStatus'] == 'completed')]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Non Complete sequences removed: ')
    
    #Sequences with an item taking ___ minutes, or more
    if(seq_item_minutes_threshold):
        seq_item_min_thres = respExcl[respExcl['mSecUsed']>seq_item_minutes_threshold]['sequenceId'].unique()
        
        if(len(seq_item_min_thres)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_item_min_thres)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with Items taking longer than '+ str(seq_item_minutes_threshold) + ' minutes to complete, removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_item_min_thres)]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences with Items taking longer than '+ str(seq_item_minutes_threshold) + ' minutes to complete, removed: ')
        
    
    #Sequences with a section taking ___ minutes, or more
    #currently assuming that each section in a sequence will be given same criterion for time
    if(seq_section_minutes_threshold):
        seq_sec_min_thres = respExcl.copy()
        seq_sec_min_thres['section_time'] = seq_sec_min_thres.groupby(by=['sequenceId', 'sectionName'])['mSecUsed'].transform('sum')/60000
        seq_sec_min_thres = seq_sec_min_thres[seq_sec_min_thres['test_sum_time']> seq_section_minutes_threshold]['sequenceId'].unique()
        
        if(len(seq_sec_min_thres)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_min_thres)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with sections taking longer than '+ str(seq_section_minutes_threshold) + ' minutes to complete, removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_min_thres)]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences with sections taking longer than '+ str(seq_section_minutes_threshold) + ' minutes to complete, removed: ')
        
    
    #Sequences taking longer than ___ minutes, defined as
    if(seq_total_minutes_threshold):
        #exclude sequences that took longer than specified time to complete
        seq_min_thres = respExcl.copy()
        seq_min_thres['test_sum_time'] = seq_min_thres.groupby(by=['sequenceId'])['mSecUsed'].transform('sum')/60000
        seq_min_thres = seq_min_thres[seq_min_thres['test_sum_time']> seq_total_minutes_threshold]['sequenceId'].unique()
        
        if(len(seq_min_thres)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_min_thres)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences taking longer than '+ str(seq_total_minutes_threshold) + ' minutes to complete, removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_min_thres)]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences taking longer than '+ salvador.color.BOLD  + str(seq_total_minutes_threshold) +  salvador.color.END + ' minutes to complete, removed: ')
        
    
    #Sequences with fewer items than set threshold removed:
    #Fewer than ___ items attempted
    #Fewer than ___ % of items attempted in a sequence
    #Fewer than ___ % of items attempted in a section(s)
    
    if(section_map.empty == False):
        
        #get calculations across entire pool of questions
        respExcl[['overall_raw_correct','overall_num_attempted']] = respExcl.groupby(['jasperUserId'])[['score', 'attempted']].transform('sum')
        respExcl['temp_uniq_items'] =  respExcl.groupby(by=['jasperUserId'])['contentItemName'].transform('nunique')
        respExcl['overall_pTotal'] = respExcl['overall_raw_correct']/respExcl['temp_uniq_items'] #dividing by total number of unique questions in this section
        respExcl['overall_pPlus'] = respExcl['overall_raw_correct']/respExcl['overall_num_attempted']
        respExcl.drop(columns = ['temp_uniq_items'], inplace = True)
            
        #get all sequence level calculations
        respExcl[['template_raw_correct','template_num_attempted']] = respExcl.groupby(['sequenceId'])[['score', 'attempted']].transform('sum')
        respExcl['template_pTotal'] = respExcl['template_raw_correct']/respExcl['actualNumQues'] #total questions on a single exam across all sections
        respExcl['template_pPlus'] = respExcl['template_raw_correct']/respExcl['template_num_attempted']

        #get all the calculations at the section level
        respExcl['section_num_omitted'] = respExcl.groupby(by=['sequenceId', 'sectionName'])['attempted'].transform(lambda x: sum(x!=True))
        respExcl['section_num_attempted'] = respExcl.groupby(by=['sequenceId', 'sectionName'])['attempted'].transform('sum')
        respExcl['section_perc_attempted'] = respExcl['section_num_attempted']/respExcl['sectionNumQues']
        respExcl['section_raw_correct'] = respExcl.groupby(by=['sequenceId', 'sectionName'])['score'].transform('sum')
        respExcl['section_num_scored'] = respExcl.groupby(by=['sequenceId', 'sectionName'])['scored'].transform('sum')
        respExcl['section_pTotal'] = respExcl['section_raw_correct']/respExcl['sectionNumQues']
        respExcl['section_pPlus'] = respExcl['section_raw_correct']/respExcl['section_num_attempted']
            
        if(qbank == True):
            #Fewer than ___ % of items attempted in a sequence
            if(min_items_per_seq == None):
                print('No minimum item threshold provided for this qbank.\n')
            else:
                seq_below_resp_threshold = respExcl[respExcl['template_num_attempted'] < min_items_per_seq]['sequenceId'].unique()
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than '+ str(min_items_per_seq*100) + '% of items attempted in a sequence, removed'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
                
                #calculate overall sequence order after all cleaning is complete
                #only needed here coz of qbank
                seq_order_df = respExcl[['jasperUserId', 'sequenceId', 'dateCreated']].drop_duplicates()
                seq_order_df['actual_sequence_order'] =  seq_order_df.groupby(by=['jasperUserId'])['dateCreated'].rank(method = 'first')
                
                respExcl = pd.merge(respExcl, seq_order_df)
                num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                                   num_responses_current,
                                                                                 things_to_say = 'Sequences with items fewer than '+ str(min_items_per_seq*100) + '% of items attempted in a sequence, removed')
            
        else: # if qbank==False
            if(section_map.empty == False):
                total_qs = sum(section_map['sectionNumQues'])
                #create output_df_list variable here
            else:
                if(test_map.empty == False):
                    total_qs = sum(test_map['numQues'])
                else:
                    warnings.warning('No test_map or section_map')
            
            #Fewer than ___ % of items attempted in a section(s)
            ##response threshold filter - first use section map if any, otherwise use response threshold
            if(len(section_map['min_items_per_seq']>0)):
                temp_df = pd.merge(respExcl, pd.DataFrame({'sectionName': section_map['jasperSectionName'],
                                                                           'min_items_per_seq' : section_map['min_items_per_seq']}))
                seq_below_resp_threshold = temp_df[(temp_df['section_num_attempted'] < temp_df['min_items_per_seq']) | (temp_df['template_num_attempted'] < sum(section_map['min_items_per_seq']))]['sequenceId'].unique()
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than __% of items attempted in a section'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
                
                num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                                   num_responses_current,
                                                                                 things_to_say = 'Sequences removed because one or more sections were under the threshold of attempted questions: ')
            
            #Fewer than ___ % of items attempted across sections(s)
            elif(section_calc == True):
                seq_below_resp_threshold = respExcl[respExcl['section_perc_attempted'] < respExcl['section_response_threshold']]['sequenceId'].unique()
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than __% of items attempted across sections'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
                rspExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
                
                num_seq_current, num_users_current, num_items_current = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                             num_responses_current,
                                                                                 things_to_say = 'Sequences removed because one or more sections were under the threshold of attempted questions: ')
                
            elif(section_calc == False):
                seq_below_resp_threshold = respExcl[respExcl['section_perc_attempted']<respExcl['test_response_threshold']]['sequenceId'].unique() #this should come from test_map
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than __% of items attempted in a sequence, removed'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
                
                num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                                   num_responses_current,
                                                                                 things_to_say = 'Sequences removed because one or more sections were under the threshold of attempted questions: ')
                
            if(section_map.empty == False):
                #create a output_df_list df with data for each section from section_map
                print('section_map is False & section_separated = True\n')
            else:
                #store total data from respexcl in output_df_list at index 1
                print('all out of section_map\n')
            
    
    elif(test_map.empty == False):
        if(qbank == True):
            #sequences with less than a pre-determined number of valid sequence responses in each of the sections (considered separately) will be excluded
            
            #get all sequence level calculations
            respExcl[['template_raw_correct','template_num_attempted']] = respExcl.groupby(['sequenceId'])[['score', 'attempted']].transform('sum')
            print('Sequence level sums complete')
            
            respExcl['template_pTotal'] = respExcl['template_raw_correct']/respExcl['actualNumQues'] #total questions on a single exam across all sections
            respExcl['template_pPlus'] = respExcl['template_raw_correct']/respExcl['template_num_attempted']
            
            if(min_items_per_seq==None):
                print('No minimum item threshold provided for this qbank.','\n')
            else:
                #Fewer than ___ % of items attempted in a sequence
                seq_below_resp_threshold = respExcl[respExcl['template_num_attempted'] < min_items_per_seq]['sequenceId'].unique()
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than __% of items attempted in a sequence, removed'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
                
                seq_order_df = respExcl[['jasperUserId', 'sequenceId', 'dateCreated']].drop_duplicates()
                seq_order_df['actual_sequence_order'] =  seq_order_df.groupby(by=['jasperUserId'])['dateCreated'].rank(method = 'first')
                
                respExcl = pd.merge(respExcl, seq_order_df)
                num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                                   num_responses_current,
                                                                                 things_to_say = 'Sequences removed under the threshold of attempted items: ')
            
            number_of_unique_CIs = respExcl['contentItemName'].nunique()
            respExcl[['overall_raw_correct', 'overall_num_attempted']] = respExcl.groupby(by=['jasperUserId'])[['score', 'attempted']].transform('sum')
            
            respExcl['overall_pTotal'] = respExcl['overall_raw_correct']/number_of_unique_CIs  #divide by total number of unique questions in this section
            respExcl['overall_pPlus'] = respExcl['overall_raw_correct']/respExcl['overall_num_attempted']
            
            seq_order_df = respExcl[['jasperUserId', 'sequenceId', 'dateCreated']].drop_duplicates()
            seq_order_df['actual_sequence_order'] =  seq_order_df.groupby(by=['jasperUserId'])['dateCreated'].rank(method = 'first')
                
            respExcl = pd.merge(respExcl, seq_order_df)
            
        
        elif(qbank == False):

            #sequences with less than a pre-determined number of valid responses will be excluded
            respExcl['template_num_omitted'] = respExcl.groupby(by=['sequenceId'])['attempted'].transform(lambda x: sum(x!=True))
            respExcl['template_num_attempted'] = respExcl.groupby(by=['sequenceId'])['attempted'].transform('sum')
            respExcl['template_perc_attempted'] = respExcl['template_num_attempted']/respExcl['test_num_ques']
            respExcl['template_raw_correct'] = respExcl.groupby(by=['sequenceId'])['score'].transform('sum')
            respExcl['template_pTotal'] = respExcl['template_raw_correct']/respExcl['test_num_ques']
            respExcl['template_pPlus'] = respExcl['template_raw_correct']/respExcl['template_num_attempted']

            #Fewer than ___ % of items attempted across sections(s)
            seq_below_resp_threshold = respExcl[respExcl['template_perc_attempted'] < respExcl['test_response_threshold']]['sequenceId'].unique()
            
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with items fewer than ' + ', '.join(str(x) for x in respExcl['test_response_threshold'].unique()) + '% of items attempted in a sequence, removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
            
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                         num_responses_current,
                                                                                 things_to_say = 'Sequences with items fewer than ' + ', '.join(str(x) for x in respExcl['test_response_threshold'].unique()) + '% of items attempted in a sequence, removed :')
            
        
        else:
            warnings.warn('Parameter qbank was not true or false? somehow?')
            
    else:
        warnings.warn('No section_map or test_map!!!')
    
    
    #Sequences with invalid/missing recorded score elements:
    #Percent correct
    #Scaled Score(s)
    #Theta/Penalty Theta
    
    sec = 'Extras'
    sub_sec = 'Item Removal'
    #Extras
    # remove unscored items if requested, otherwise do nothing. defaults to doing nothing.
    if(remove_unscored == True):
        #adding data to rejects df
        temp_rej = respExcl[respExcl['scored']!=1][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'unscored items'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
        respExcl = respExcl[respExcl['scored']==1]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Unscored item responses removed : ')
    
    
    sub_sec = 'Sequence Removal'
    #remove sequences if given in list
    if (seqHist_to_exclude.empty == False):
        
        #adding data to rejects df
        temp_rej = respExcl[respExcl['sequenceId'].isin(seqHist_to_exclude['sequenceId'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Sequences removed from a given list'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[~respExcl['sequenceId'].isin(seqHist_to_exclude['sequenceId'])]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences input from list, removed : ')
    
    #adding content_item info to response df
    if(precombined_files == False):
        #add content item info
        if not (cidf.empty):
            if not (ci_cols_to_include.empty):
                respExcl = combine_CIinfo(data_path, respExcl, cidf = cidf, ci_cols_to_include = ci_cols_to_include, interaction_type_list = interaction_type_list)
            else:
                respExcl = combine_CIinfo(data_path, respExcl, cidf = cidf, interaction_type_list = interaction_type_list)
        else:
            respExcl = combine_CIinfo(data_path, respExcl, interaction_type_list = interaction_type_list)
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Number of sequences removed during content item join (should be 0): ')
    
    sub_sec = 'Sequence Removal'
    #Remove sequences for the users who practiced more than once
    if(remove_repeat_test_administrations == True):
        seq_to_exclude_calc5 = respExcl.copy()
        seq_to_exclude_calc5['num_ques'] = seq_to_exclude_calc5.groupby(by=['jasperUserId', 'sequenceName', 'sequenceId', 'dateCreated'])['contentItemName'].transform('count')
        seq_to_exclude_calc5 = seq_to_exclude_calc5[['jasperUserId', 'sequenceName', 'sequenceId', 'dateCreated']].drop_duplicates()
        seq_to_exclude_calc5['sequence_order'] = seq_to_exclude_calc5.groupby(by=['jasperUserId', 'sequenceName'])['dateCreated'].rank(method = 'first')
        seq_to_exclude_calc5 = seq_to_exclude_calc5[seq_to_exclude_calc5['sequence_order']>1]['sequenceId'].unique()
        
        if(len(seq_to_exclude_calc5) > 0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc5)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Remove sequences for the users who practiced more than once'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc5)]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences that were not the first administration for the user, removed: ')
    
    sub_sec = 'Sequence Removal'
    if(remove_seq_wo_dispseq == True):
        seq_to_exclude_calc6 = respExcl.copy()
        seq_to_exclude_calc6 = seq_to_exclude_calc6[seq_to_exclude_calc6['displaySeq'].isnull()]['sequenceId'].unique()
        
        if(len(seq_to_exclude_calc6) > 0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc6)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Remove sequences for the users who have display sequence null for the items'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc6)]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences with items that have display sequence is null, removed :')
    
    print('Remaining number of responses in final output: ', respExcl.shape[0])
    print('Remaining number of sequences in final output: ', num_seq_current)
    print('Remaining number of users in final output: ', num_users_current)
    print('Remaining number of unique items in final output: ', num_items_current)
    print('Cleaning function time completed: ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cleaning_info.set_index(['Section', 'Sub_section', 'Condition'], inplace = True)
    
    sys.stdout = pre_ins
    #cleaned_data=pre_ins
    f = open(results_path + analysis_name+'_Cleaning_info.txt', 'r')
    file_contents = f.read()
    print (file_contents)
    #cleaning_info_data_content=file_contents
    f.close()
    
    #renaming colum names
    respExcl.columns = [col.replace('jasperUser', 'student').replace('sequence', 'activity') for col in respExcl.columns ]
    rejects_df.columns = [col.replace('jasperUser', 'student').replace('sequence', 'activity') for col in rejects_df.columns ]
    
    return respExcl, cleaning_info, rejects_df


def make_user_level_matrices(df,
                  vars_for_matrices,
                  destination_file_path,
                  destination_file_name_prefix,
                  analysis_name,
                  omit_code = '.',
                  not_seen_code = '-99',
                  use_display_order = False,
                  zero_sec_as_not_reached = False,
                 qbank = True,
                 item_order_list = []):
    
    if(zero_sec_as_not_reached == True):
        df = df[df['mSecUsed']>0].copy()
    
    for col, mat_nam in vars_for_matrices.items():
        df = df[df['repeatOmitted']==False].copy()
        #marking omit values to omit_code here
        new_col = col+'_omits'
        df[new_col] = df[col]
        df.loc[df['attempted']==False, new_col] = omit_code
        df.loc[df['responseStatus']=='not-reached', new_col] = not_seen_code
        big_matrix = pd.pivot(data = df, index = 'studentId', columns='contentItemName', values = new_col)
            
        #marking not seen items with not_seen_code
        big_matrix.fillna(value = not_seen_code, inplace = True)
    
        if(use_display_order == True):
            if(qbank == False):
                CI_ordered = df[['activityName', 'contentItemName', 'displaySeq']].drop_duplicates().sort_values(by=['activityName', 'displaySeq'])['contentItemName']
                big_matrix = big_matrix[CI_ordered]
            else:
                CI_ordered = df[['contentItemName']].drop_duplicates().sort_values(by=['contentItemName'], ignore_index=True)['contentItemName']
                big_matrix = big_matrix[CI_ordered]
        
        if(len(item_order_list)>0):
            big_matrix = big_matrix[item_order_list]
        
        big_matrix.to_csv(destination_file_path+analysis_name+destination_file_name_prefix+mat_nam+'.csv')
        print('Finished creating matrix for ' + mat_nam)
    return big_matrix


#display sequence is removed due to for questbank creating multiple rows
#below also holds for items that are not having constant display sequence across various templates
def make_item_level_info(df, content_df, results_path, analysis_name, corr_ans, qbank = False):
    
    df = df[df['repeatOmitted']==False].copy()
    #making a seen field for an item to make count_seen
    df['itemSeen'] = df['responseStatus']!='not-reached'
    
    if(qbank == True):
        cidf_summary = df.sort_values(by=['displaySeq']).groupby(by=['contentItemName', 'activityName', 'templateId'], as_index=False, dropna = False).agg(displaySeq = ('displaySeq', lambda x: ', '.join(x.drop_duplicates().astype(str))),
                                                                                                                                  count_att = ('attempted', 'sum'),
                                                                                                                                                   count_seen = ('itemSeen', 'sum'),
                                                                                                                                                   num_correct = ('score', 'sum'),
                                                                                                                                                   first_date = ('dateCreated', 'min'),
                                                                                                                                                   last_date = ('dateCreated', 'max')).sort_values(by=['activityName'], ignore_index = True)
        

    
        cidf_summary = pd.merge(cidf_summary, content_df)
    
        cidf_summary = cidf_summary[['contentItemId', 'contentItemName',
                             'activityName',
                             'templateId',
                             'displaySeq',
                             'count_att',
                             'count_seen',
                             'num_correct',
                             'first_date',
                             'last_date',
                             'interactiontypename',
                             'countchoices',
                             'correctAnswer']].drop_duplicates(ignore_index = True)
    
    else:
        cidf_summary = df.sort_values(by=['displaySeq']).groupby(by=['contentItemName', 'activityName', 'templateId'], as_index=False, dropna = False).agg(displaySeq = ('displaySeq', lambda x: ', '.join(x.drop_duplicates().astype(str))),
                                                                                                                                  count_att = ('attempted', 'sum'),
                                                                                                                                                   count_seen = ('itemSeen', 'sum'),
                                                                                                                                                   num_correct = ('score', 'sum'),
                                                                                                                                                   first_date = ('dateCreated', 'min'),
                                                                                                                                                   last_date = ('dateCreated', 'max')).sort_values(by=['activityName'], ignore_index = True)
        

        cidf_summary = pd.merge(cidf_summary, content_df, how = 'left')
        
        cidf_summary = cidf_summary[['contentItemId', 'contentItemName',
                             'activityName',
                             'templateId',
                            'displaySeq',
                             'count_att',
                             'count_seen',
                             'num_correct',
                             'first_date',
                             'last_date',
                             'interactiontypename',
                             'countchoices',
                             'correctAnswer']].drop_duplicates().sort_values(by=['displaySeq'], ignore_index = True)
        
    cidf_summary = pd.merge(cidf_summary.drop(columns = ['correctAnswer', 'displaySeq', 'contentItemId']), corr_ans, on = ['contentItemName'])
    cidf_summary.to_csv(results_path+analysis_name+'_Content_Item_Info.csv', index = False)
    
    return cidf_summary

def make_activity_level_info(df, results_path, analysis_name):
    
    df = df[df['repeatOmitted']==False].copy()
    activity_info_metadata = df[['studentId', 'activityId', 'dateCreated', 'dateCompleted', 'activityName',
                                'template_num_attempted']].drop_duplicates(ignore_index = False)
    
    #correcting template_pTotal & template_pPlus as per max_points
    activity_level_metrics = df.groupby(by=['activityId'], as_index = False).apply(lambda x: pd.Series(dict(template_raw_correct = x['score'].sum(),
                                                                                    max_points = x['max_points'].sum(),
                                                                                    num_att_by_maxpoints = (x[x['responseStatus']=='responded']['max_points']).sum()
                                                                                    )))

    activity_level_metrics['template_pTotal'] = activity_level_metrics['template_raw_correct']/activity_level_metrics['max_points']
    activity_level_metrics['template_pPlus'] = activity_level_metrics['template_raw_correct']/activity_level_metrics['num_att_by_maxpoints']

    #merging with corrected pTotal & pPlus
    activity_level_info = pd.merge(activity_info_metadata, activity_level_metrics[['activityId', 'template_raw_correct', 'template_pTotal', 'template_pPlus']], on =['activityId'])

    activity_level_info.to_csv(results_path+analysis_name+'_activity_Level_Info.csv', index = False)
    
    return activity_level_info
    
    
#adding max_points used to compute ptotal & pplus for NGN items
def make_user_level_info(df, results_path, analysis_name, test_map, qbank = False):
    df = df[df['repeatOmitted']==False].copy()

    panel_calc = df.groupby(by=['studentId', 'activityName'], as_index = False, dropna = False).apply(lambda x: pd.Series(dict(num_seen = (x[x['responseStatus']!='not-reached']['max_points']).sum(),
                                                                                                                                num_att = (x[x['responseStatus']=='responded']['max_points']).sum(),
                                                                                                                                num_correct = (x['score']).sum())))

    panel_calc['activityName'] = 'incl_'+panel_calc['activityName'].astype('str')

    panel_calc[['total_seen', 'total_att', 'total_correct']] = panel_calc.groupby(by=['studentId'])[['num_seen', 'num_att', 'num_correct']].transform('sum')
    panel_calc['num_panel_tests_taken'] = panel_calc.groupby(by=['studentId'])['activityName'].transform('nunique')
    panel_calc['test_incl'] = 1
    panel_calc['total_panel'] = sum(test_map['numQues']) if not qbank else df[['studentId', 'activityName', 'actualNumQues']].drop_duplicates(ignore_index = True)['actualNumQues']
    panel_calc['panel_ptotal'] = panel_calc['total_correct']/panel_calc['total_seen']
    panel_calc['panel_pplus'] = panel_calc['total_correct']/panel_calc['total_att']
    
    
    test_resp_squished = df.groupby(by=['studentId'], as_index = False, dropna = False).agg(num_seq_taken = ('activityName', 'nunique'),
                                                                                        test_seen = ('activityName', 'size'),
                                                                                        test_att = ('attempted', 'sum'),
                                                                                        test_correct = ('score', 'sum'),
                                                                                        first_test_date = ('dateCreated', 'min'),
                                                                                        last_test_date = ('dateCreated', 'max'))
    
    test_resp_squished['test_total'] = sum(test_map['numQues']) if not qbank else df[['studentId', 'actualNumQues']].drop_duplicates(ignore_index = True).groupby(by=['studentId'], as_index = False)[['actualNumQues']].agg('sum')['actualNumQues']
    test_resp_squished['test_ptotal'] = test_resp_squished['test_correct']/test_resp_squished['test_seen']
    test_resp_squished['test_pplus'] = test_resp_squished['test_correct']/test_resp_squished['test_att']
    
    users = pd.merge(test_resp_squished, panel_calc)

    user_info = pd.pivot(data=users,
        index=['studentId', 'num_panel_tests_taken', 'total_panel', 'total_seen', 'total_att', 'total_correct', 'panel_ptotal', 'panel_pplus'],
        columns = 'activityName',
        values = 'num_seq_taken').reset_index()

    user_info.to_csv(results_path+analysis_name+'_User_Level_Info.csv', index = False)
    
    return user_info

def Generate_file(
    data_path = 'C:\\Users\\CRachuri\\Downloads\\',
    results_path = 'C:\\Users\\CRachuri\\Downloads\\',
    analysis_name = '_',
    Qc_files_data=dict(),
    result_checks=True, # keep always true
    Matched_items=True,
    umatched_items=True,
    duplicate_items=True,
    meta_data=True,
    Ncount_file=True,
    cleaning_log=True,
    score_frequency_count=True,
    response_frequency_count=True
    ):
    response_df = Qc_files_data.get('response_df', pd.DataFrame())
    content_df=Qc_files_data.get('content_df',pd.DataFrame())
    result=Qc_files_data.get('cleaned_response_data',pd.DataFrame())
    Qid_list=Qc_files_data.get('Qid_list',pd.DataFrame())

    if len(Qc_files_data)!=0:
        file_name=results_path+analysis_name
        writer = pd.ExcelWriter(file_name+'_Audit_info_sheets.xlsx', engine='xlsxwriter')
        # checking whether students existed for under no response 
        #result=result.copy() 
        a=result[['studentId','attempted']].drop_duplicates().groupby(['studentId']).agg(attempt_count=('attempted','count')).reset_index()
        df=pd.merge(a[a['attempt_count']==1],result[['studentId','attempted']].drop_duplicates(),on='studentId',how='left')
        df=df[(df['attempt_count']==1) & (df['attempted']==False) ]['studentId']
        df.to_excel(writer, sheet_name='Removed_under_no_response',index=False)
        if (df.empty == True):
            result=Qc_files_data['cleaned_response_data']
        else :
            result=result[~result['studentId'].isin([response_df])]

        # checking the conditions for omitt, responded and not-reached in result file
        if result_checks== True:

            df=result.copy()
            df_resp=df[(df['responseStatus']=='responded') & ((df['response'].isnull())|(df['attempted']==False)|(df['score'].isnull())) ]
            # condition for  not-reached
            df_not_reached=df[(df['responseStatus']=='not-reached') & ((pd.notna(df['response']))|(df['attempted']==True) | (pd.notna(df['score'])))]
            # condition for omitted
            df_omit=df[(df['responseStatus']=='omitted') & ((~df['response'].isin([0,'0',np.nan,None]) | (df['attempted']==True) | (df['score']==1)))]
            df_resp=pd.concat([df_resp,df_not_reached,df_omit])
            

            if (df_resp.empty):
                print('All the conditions for result file is in sync')
            else :
                print('conditions for the result files are not in sync , Keep eye on following students !')
                print(df_resp)

        if cleaning_log==True:
            #pushing the cleaning info file
            file = pd.read_csv(results_path + analysis_name+'_Cleaning_info.txt', sep=';')
            file.astype('str').to_excel(writer, sheet_name='cleaning_log',index=False)

            # pushing cleaning summary info
            file=pd.read_csv(results_path + analysis_name+'_cleaningInfo.csv')
            file.rename(columns={'value':'Sequence Removed'},inplace=True)
            file.to_excel(writer, sheet_name='cleaning_log_summary',index=False)

        # file generation for matched or unmatched Qids list
        if Matched_items==True:
            Qids=Qid_list.copy()
            not_matched_items = pd.merge(Qids, content_df[['contentItemName']].drop_duplicates(), on = 'contentItemName', how = 'left', indicator=True)
            matched_items = not_matched_items[not_matched_items['_merge']=='both'][['contentItemName']]
            matched_items.to_excel(writer, sheet_name='Matched_QIDs_list',index=False)

        if  umatched_items==True:
            Qids=Qid_list.copy()
            not_matched_items = pd.merge(Qids, content_df[['contentItemName']].drop_duplicates(), on = 'contentItemName', how = 'left', indicator=True)
            matched_items = not_matched_items[not_matched_items['_merge']=='both'][['contentItemName']]
            not_matched_items = not_matched_items[not_matched_items['_merge']=='left_only'][['contentItemName']]
            not_matched_items.to_excel(writer, sheet_name='Unmatched_QIDs_list',index=False)


        # for Duplicates versions 
        if duplicate_items==True:
            duplicate_item=content_df[content_df['contentItemName'].isin(content_df[['contentItemName','contentItemId' ,'correctAnswer']].drop_duplicates().groupby(by=['contentItemName']).filter(lambda x: len(x)>1)['contentItemName'])][['contentItemId', 'contentItemName', 'correctAnswer', 'last_modified']]
            if duplicate_item.empty:
                duplicate_item['Considered_for_grading']=""
                duplicate_item['User_count_as_per_versions']=""
                duplicate_item.to_excel(writer, sheet_name='duplicate_items_versions_list',index=False)

            else :
                # making the ranking for answer_count             
                duplicate_item['answer_count']=duplicate_item.groupby(['contentItemName'])['correctAnswer'].transform('nunique')
                duplicate_item.loc[duplicate_item['answer_count']==1,'considered_for_grading']='1'
                #duplicate_item['considered_for_grading']=np.where(duplicate_item['answer_count']==1,1)

                # for considering grading columns 
                duplicate_item.loc[duplicate_item['answer_count']>1, 'rank_modifies'] = duplicate_item[duplicate_item['answer_count']>1].groupby(['contentItemName'])['last_modified'].rank(ascending=False , method='first')
                duplicate_item.loc[duplicate_item['rank_modifies']==1, 'considered_for_grading']=1

                duplicate_item['considered_for_grading'].fillna(value = 0, inplace = True)
                duplicate_item.drop(columns={'answer_count','rank_modifies'},inplace=True)

                # counting user count as per versions 
                user_count=response_df[['contentItemId','contentItemName', 'jasperUserId']].drop_duplicates().groupby(by = ['contentItemId','contentItemName'], as_index = False, dropna=False).agg(User_count_as_per_version = ('jasperUserId', 'nunique'))
                duplicate_item=pd.merge(duplicate_item,user_count,on=['contentItemId','contentItemName'],how='inner')
                
                duplicate_item.to_excel(writer, sheet_name='duplicate_items_versions_list',index=False)
            

        if meta_data==True:

            #Making cor_ans df
            cor_ans = content_df[content_df.groupby(by=['contentItemName'])['last_modified'].rank(method = 'first', ascending=False)==1][['contentItemId', 'contentItemName', 'correctAnswer','countchoices','interactionTypeName']]

            # snap for Answer key changed 
            answer_key_flag=content_df[['contentItemName','correctAnswer']].drop_duplicates().groupby(['contentItemName']).agg(answer_key_count=('contentItemName','count')).reset_index()
            answer_key_flag['Answer_key_changed']=np.where(answer_key_flag['answer_key_count']>1,1,0)
            answer_key_flag.drop(['answer_key_count'],axis=1,inplace=True)

            #multi Item Versions

            multi_item_version=content_df.groupby(['contentItemName']).agg(items_versions_count=('contentItemId','count')).reset_index()
            multi_item_version['multi_item_version']=np.where(multi_item_version['items_versions_count']>1, 1 ,0)
            multi_item_version=pd.merge(answer_key_flag,multi_item_version,on='contentItemName',how='inner')

            # option count changes
            options_changed=content_df[['contentItemName','countchoices']].drop_duplicates().groupby(['contentItemName','countchoices']).agg(Options_count=('countchoices','size')).reset_index()
            options_changed['options_countchoices_changed']=np.where(options_changed['Options_count']>1,1,0)
            options_changed=options_changed[['contentItemName','countchoices','options_countchoices_changed']]
            options_changed=pd.merge(options_changed,multi_item_version,on=['contentItemName'],how='inner')
            cor_ans=pd.merge(cor_ans,options_changed,on=['contentItemName','countchoices'],how='inner')
            cor_ans.to_excel(writer, sheet_name='Item_Meta_data',index=False)

        if Ncount_file==True:
            # Ncount before after cleaning
            raw_ft_items_counts = response_df[['contentItemName', 'jasperUserId']].drop_duplicates().groupby(by = ['contentItemName'], as_index = False, dropna=False).agg(initial_count = ('jasperUserId', 'nunique'))
            #final_items_counts=result.groupby(by=['contentItemName', 'activityName', 'templateId'],as_index=False,dropna=False).agg(after_cleaning=('attempted', 'sum'))[['contentItemName','after_cleaning']]
            final_items_counts=result.groupby(by=['contentItemName','templateId'],as_index=False,dropna=False).agg(after_cleaning=('attempted', 'sum'))[['contentItemName','after_cleaning']]
            count_result=pd.merge(raw_ft_items_counts,final_items_counts,on=['contentItemName'],how='inner')
            count_result.to_excel(writer, sheet_name='Ncount_file',index=False)

        if score_frequency_count==True :
            # count as per the score
            df=result[['studentId','contentItemName','score','responseStatus']].drop_duplicates().copy()
            df.loc[df['responseStatus']=='not-reached','score']='not-reached'
            df.loc[df['responseStatus']=='omitted','score']='omitted'
            df.loc[df['responseStatus']==np.nan ,'score']='omitted'
            df.loc[df['responseStatus'].isnull(),'score']='omitted'
            df.pivot_table(index='contentItemName',columns='score',values='studentId',aggfunc='count').reset_index().to_excel(writer,sheet_name='Score_frequency_count',index=False)

        if response_frequency_count == True :
            # count as per the response 
            df=result[['studentId','contentItemName','response','responseStatus']].drop_duplicates().copy()
            df.loc[df['responseStatus']=='not-reached','response']='not-reached'
            df.loc[df['responseStatus']=='omitted','response']='omitted'
            df.loc[df['responseStatus']==np.nan ,'response']='omitted'
            df.loc[df['responseStatus'].isnull(),'response']='omitted'
            df.pivot_table(index='contentItemName',columns='response',values='responseStatus',aggfunc='count').reset_index().to_excel(writer,sheet_name='Response_frequency_count',index=False)

        writer.close()

def upload_to_drive(Source_path,destination_path):
    for file_name in os.listdir(Source_path):
        shutil.copy(Source_path+file_name,destination_path+file_name)

    print("Files are copied sucessfully")
    print("From directory : {0}\nTo directory: {1} ".format(Source_path,destination_path))
