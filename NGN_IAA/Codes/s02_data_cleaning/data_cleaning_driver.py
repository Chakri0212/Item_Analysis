import pandas as pd
import numpy as np
import sqlalchemy as sql
from IPython.display import display
# import pgpasslib
import sys
import os

# Add the parent directory (where ngn/ lives) to sys.path
sys.path.insert(0, os.path.abspath("."))

# from ngn_items_data_cleaning

from .ngn.salvador import color, teleg_msg, merge_size, ch_dtype ,db_con

from .ngn.scrubbing import describe_dupe_cor_ans, get_item_cor_ans, recode_as_omitted, timing_exclusion, remove_repeat_questions, combine_CIinfo, removed_record_count, clean_item_data

from .ngn.scrubbing import make_user_level_matrices, make_item_level_info, make_activity_level_info, make_user_level_info, resp_cleaning



def run_code(params):
    
    '''
    Cleans the raw project data and saves the cleaned project data

    '''
    print("-- 2. Data Clean --")
    
    ##########
    # Params #
    ##########
    analysis_name = params['report_name']
    data_path='G:\\My Drive\\My_laptop_backup\\DRCR_Q2_2024\\Automation_project\\Git_hub_code\\item_analysis_automation-main\\NGN_IAA\\projects\\{0}\\01_data_pull\\data\\'.format(analysis_name)
    results_path='G:\\My Drive\\My_laptop_backup\\DRCR_Q2_2024\\Automation_project\\Git_hub_code\\item_analysis_automation-main\\NGN_IAA\\projects\\{0}\\02_data_cleaning\\'.format(analysis_name)

    #results_path = f"projects/{analysis_name}/02_data_cleaning/"
    
    
   
    #############
    # Get files #
    #############
    response_df = pd.read_csv(data_path + "response_data.tsv", sep = '\t',parse_dates = ['item_submitted_timestamp']).drop_duplicates(ignore_index=True).dropna(subset=['student_id', 'activity_id','item_section_position','raw_response'],axis=0,ignore_index=True)

    activity_df = pd.read_csv(data_path + 'activity_info.tsv', sep='\t', parse_dates=['timestamp_created', 'timestamp_completed']).drop_duplicates(ignore_index=True).dropna(subset=['student_id', 'activity_id'], axis=0, ignore_index=True)

    content_df = pd.read_csv(data_path + 'content_info_Atom.tsv', sep='\t', parse_dates = ['last_modified']).drop_duplicates(ignore_index=True).dropna(subset=['content_item_name', 'content_item_id'], axis=0, ignore_index=True)
    
    print('input checks was done')
    
    ##############################################
    #### connecting to DB to generate engine #####
    ##############################################
    
    try:
        engine
    except:
        engine = db_con()
    
    #############################################
    # including max_points and correct_answer  #    
    #############################################
    max_points_correct_answer_query =  '''
            select distinct item_id as content_item_id
                    ,item_name as content_item_name
                    , correct_answer 
                    ,max_score as max_points
            from kna.raw_atom_activity.content_items
            where item_id in ({item_ids})
            and item_name in ({item_names})
            group by item_id ,item_name,correct_answer , max_score
            order by item_id , item_name,correct_answer , max_score '''
            
    
    max_points_correct_answer_query = max_points_correct_answer_query.format(item_ids=','.join(f"'{cid}'" for cid in content_df['content_item_id'].unique().tolist()),
    item_names=','.join(f"'{name}'" for name in content_df['content_item_name'].unique().tolist()))

    content_df.drop(columns={'max_points'},inplace=True) ## while repulling correct_answer column from the source rable need to drop first exist column values 

    try :
        max_points_correct_answer_df
    except:
        max_points_correct_answer_df = pd.read_sql(sql = max_points_correct_answer_query, con = engine.raw_connection())

    #### considering only maxscore with respective items once again to avoid any duplicates
    content_df = pd.merge(content_df,max_points_correct_answer_df.groupby(['content_item_name','content_item_id']).agg(max_points=('max_points','max')).reset_index(), on = ['content_item_id', 'content_item_name'], how = 'inner') 
    

    ##############################################
    # Checking whether exist any rational items #
    ##############################################
    
    if content_df[content_df['interaction-type-tag'].apply(lambda x: 'rationale' in str(x).lower())].empty == False:
        print('Incorrect scores existed for rational items in user responses, Re-pulling rational item scores from redshift')
        rational_items=content_df[content_df['interaction-type-tag'].apply(lambda x : 'rationale' in str(x).lower())].copy()
        rational_responses=response_df[response_df['content_item_id'].isin(list(rational_items['content_item_id'].unique()))]
        
        ## formating the code to repull the rational item responses from redshift to avoid any issues with bjorne pull
        with open(os.getcwd()+'\\item_analysis_automation-main\\NGN_IAA\\Codes\\s01_data_pull\\Rational_items_score.sql', "r") as sql_file2:
            rational_query = sql_file2.read()
            sql_file2.close()

            rational_query=rational_query.format(activity_start_date=f"'{pd.Timestamp(params['start_date']).normalize().strftime('%Y-%m-%d %H:%M:%S')}'",
                                 activity_end_date=f"'{pd.Timestamp(params['end_date']).normalize().strftime('%Y-%m-%d %H:%M:%S')}'",
                                 sequence_names=','.join(f"'{name}'" for name in activity_df['template_name'].unique().tolist()),
                                 activity_id=','.join(f"'{aid}'" for aid in rational_responses['activity_id'].unique().tolist()),
                                 rational_items=','.join(f"'{cid}'" for cid in rational_items['content_item_name'].unique().tolist()))

        #  Exporting formated sql query for checking
        with open(results_path+"Rational_items_score.sql", "w") as file:
            file.write(rational_query)

        print('started pulling rational responses from redshift')
        
        try :
            rational_responses_frm_redshift
        except:
            rational_responses_frm_redshift = pd.read_sql(sql = rational_query, con = engine.raw_connection())

        #### Handling rational responses those are pulled from redshift

        rational_responses_frm_redshift = rational_responses_frm_redshift[['student_id','activity_id','item_position','content_item_id','content_item_name','item_status','item_score']].drop_duplicates()
        rational_responses_frm_redshift['student_id']=rational_responses_frm_redshift['student_id'].astype('int64')
        
        #### Exporting rational responses pulled from redshift for checking
        #rational_responses_frm_redshift.to_csv(results_path+'rational_responses_frm_redshift.csv',index=False)

        rational_df=response_df[response_df['content_item_name'].isin(list(rational_items['content_item_name'].unique()))]
        rational_df.drop(columns={'item_score'},inplace=True)
        
        ##### Exporting bjorne pulled rational responses for checking
        #rational_df.to_csv(results_path+'bjorne_rational_responses.csv',index=False)

        response_df=response_df[~response_df['content_item_name'].isin(list(rational_items['content_item_name'].unique()))]
        rational_data=pd.merge(rational_df,rational_responses_frm_redshift,on=['student_id','activity_id','item_position','content_item_id','content_item_name','item_status'],how='inner')
        response_df=pd.concat([response_df,rational_data],axis=0)
    
    ##############################
    # Data Handling: Response df #
    ##############################

    print('checking items score responses')
    
    response_df.rename(columns={'history_db_id': 'historiesDb',
                            'student_id' : 'jasperUserId',
                            'activity_id' : 'sequenceId',
                            'item_position': 'position',
                            'section_title' : 'sectionTitle',
                            'item_section_position' : 'displaySeq',
                            'content_item_id' : 'contentItemId',
                            'content_item_name' : 'contentItemName',
                            'interaction_type': 'interactionType',
                            'milliseconds_used' : 'mSecUsed',
                            'is_scored' : 'scored',
                            #'scored_response' : 'score',
                            'item_score':'score',
                            'raw_response':'response',
                            'field_test' : 'fieldTest',
                           'item_status': 'responseStatus'}, inplace = True)
    response_df['sectionName'] = response_df['sectionTitle']

       #sorting and joining by comma for response in case of multi-correct answer questions
    response_df['contentItemName'] = np.where(pd.isnull(response_df['contentItemName']), response_df['contentItemName'], response_df['contentItemName'].astype('str').str.lower())

    response_df['response'] = response_df['response'].apply(lambda x: resp_cleaning(x) if not pd.isna(x) else x)
    
    ##############################
    # Data Handling: Activity df #
    ##############################
      
    activity_df.rename(columns={'history_db_id': 'historiesDb',
                            'student_id' : 'jasperUserId',
                            'enrollment_id' : 'kbsEnrollmentId',
                            'activity_id' : 'sequenceId',
                            'template_id' : 'templateId',
                            'template_name' : 'sequenceName',
                            'sequence_title' : 'sequenceTitle',
                            'timestamp_created' : 'dateCreated',
                            'timestamp_completed' : 'dateCompleted',
                           'tutor_mode' : 'tutorMode',
                           'status':'sequenceStatus'}, inplace = True)



    ################################################
    # Data Handling: Merge Response & Activity dfs #
    ################################################
    
    print('Response_df size before merging with activity_df :', response_df.shape)
    
    initi_cols = response_df.columns
    
    response_df = pd.merge(response_df.drop(columns=['source_system']), activity_df.drop(columns=['source_system']), on = ['jasperUserId', 'sequenceId', 'historiesDb'], how = 'inner')
    
    print('Response_df size after merging with activity_df:', response_df.shape)

    ##############################
    # Data Handling: Clean EIDs #
    ##############################
    
    #changing kbs_enrollment_id to string as in cleaning function to remove 0 kbseids we are comparing as string
    response_df['kbsEnrollmentId'] = response_df['kbsEnrollmentId'].fillna(0).astype(str)

    ####################################################################################
    # Removing break items from response_df and content_df due to this user are filtering  #
    # Sequences with dupe content items within the same examination session          #
    ####################################################################################
    
    break_items=content_df[(content_df['interaction-type-tag'].isna())& (content_df['count_choices'].isin([np.nan, 0 ]))& (content_df['correct_answer'].isna()) &  (content_df['max_points'].isna())]['content_item_name'].unique() ## check point
    
    if len(break_items)>0:
        print('Break items identified :', break_items)
        response_df = response_df[~response_df['contentItemName'].isin(break_items)]
        print('Response_df size after removing break items :', response_df.shape)
        content_df = content_df[~content_df['content_item_name'].isin(break_items)]
        print('content_df size after removing break items :', content_df.shape)

    
    #############################
    # Data Handling: Content df #
    #############################
    
    sort_item_types = ['bowtie', 'cloze-drag-drop', 'cloze-dropdown', 'cloze-dropdown-rationale', 'cloze-dropdown-table', 'drag-drop-rationale', 'matrix-multiple-select', 'multi-select', 'multiple-response-grouping', 'multiple-select', 'multiple-select-n', 'multiple-select-sata', 'hot-text', 'hot-text-table']

    
    interaction_types = content_df[['content_item_name', 'interaction-type-tag']].dropna().drop_duplicates(ignore_index=True)
    interaction_types = interaction_types.groupby(by=['content_item_name'], as_index = False).agg(interactionTypeName=('interaction-type-tag', lambda x : ', '.join(x)))

    #flag to indicate wheter to sort choices
    interaction_types['sort_choices'] = interaction_types['interactionTypeName'].str.split(', ').apply(lambda x: len(set(x).intersection(sort_item_types)))

    interaction_types['content_item_name'] = np.where(pd.isnull(interaction_types['content_item_name']), interaction_types['content_item_name'], interaction_types['content_item_name'].astype('str').str.lower())
    
    
    content_df = content_df[['source_system', 'content_item_id', 'content_item_name', 'content_item_type', 'count_choices', 'correct_answer', 'max_points', 'last_modified']].drop_duplicates().copy()

    content_df = pd.merge(content_df, interaction_types.rename(columns = {'interactionTypeName' : 'interaction-type-tag'}), on = ['content_item_name'], how = 'left')

    #we should re-order correct answer for the same interaction types that we are doing in response_df else there'll be mismatch
    content_df.loc[content_df['sort_choices']>0, 'correct_answer'] = content_df[content_df['sort_choices']>0]['correct_answer'].apply(lambda x: resp_cleaning(x) if not pd.isna(x) else x)
    content_df.loc[content_df['sort_choices']==0, 'correct_answer'] = content_df[content_df['sort_choices']==0]['correct_answer'].apply(lambda x: str(x).strip() if not pd.isna(x) else x) ## converted cor_ans to string


    content_df['content_item_name'] = np.where(pd.isnull(content_df['content_item_name']), content_df['content_item_name'], content_df['content_item_name'].astype('str').str.lower())

    #Checking if there are any different versions of correct answer for items
    duplicates_items=content_df[content_df['content_item_name'].isin(content_df[['content_item_name', 'correct_answer']].drop_duplicates().groupby(by=['content_item_name']).filter(lambda x: len(x)>1)['content_item_name'])][['content_item_id', 'content_item_name', 'correct_answer', 'last_modified']]

    #Making cor_ans df
    cor_ans = content_df[content_df.groupby(by=['content_item_name'])['last_modified'].rank(method = 'first', ascending=False)==1][['content_item_id', 'content_item_name', 'correct_answer']]
    if duplicates_items.empty == False:
        CI_old_keys=duplicates_items[duplicates_items.groupby(['content_item_name'],as_index=False)['last_modified'].rank(ascending=False)!=1][['content_item_id', 'content_item_name', 'correct_answer']].reset_index(drop=True)
        CI_old_keys.rename(columns={'content_item_id' : 'contentItemId',
                            'content_item_name' : 'contentItemName',
                            'correct_answer':'correctAnswer'},inplace=True)
    else :
        CI_old_keys=pd.DataFrame()

    
    
    ###############################################
    # Data Handling: Merge Response & Content dfs #
    ###############################################
    
    content_df.rename(columns = {'content_item_id' : 'contentItemId',
                            'content_item_name' : 'contentItemName',
                            'interaction-type-tag' : 'interactionTypeName',
                            'count_choices' : 'countchoices',
                            'parent_item_id' : 'parentid',
                            'parent_item_name' : 'parentname',
                            'correct_answer':'correctAnswer'}, inplace = True)

    content_df['contentItemName'] = np.where(pd.isnull(content_df['contentItemName']), content_df['contentItemName'], content_df['contentItemName'].astype('str').str.lower())

    '''
    resp_bef_size = response_df.shape[0]
    
    print('shape of response_df before merging with content_df :', response_df.shape)

    response_df = pd.merge(response_df, content_df,
                           how = 'inner',
                          on = ['contentItemId', 'contentItemName']) 
    
    resp_aft_size = response_df.shape[0]

    print('shape of response_df after merging with content_df :', response_df.shape)

    if (resp_bef_size != resp_aft_size):
        print(color.BOLD + 'Size of response df is not same after merging with content df' + color.END) '''
    
    ##############################################################################################################
    #### New trial for identifying mismatching contentItemId & contentItemName b/w response_df & content_df ####
    ##############################################################################################################
    #mergning with response_df
    resp_bef = response_df.copy()
    print('shape of response_df before merging with content_df :', response_df.shape)

    response_df = pd.merge(response_df, content_df,
                        how = 'outer',
                        on = ['contentItemId', 'contentItemName'],
                        suffixes=('_resp','_cont'),indicator=True)
    
    if (resp_bef.shape[0] != response_df.shape[0]):
        print(color.BOLD + 'Size of response df is not same after merging with content df' + color.END)
        print('='*50)
        print("Mismatched contents in response df")
        response_missmatched_cont=response_df[response_df['_merge']=='left_only'][['contentItemId','contentItemName','interactionType']].drop_duplicates()
        display(response_missmatched_cont)

        print("Mismatched contents in content df")
        contentdf_missmatched_cont=response_df[response_df['_merge']=='right_only'][['contentItemId','contentItemName','interactiontypename']].drop_duplicates()
        display(contentdf_missmatched_cont)
        del response_missmatched_cont,contentdf_missmatched_cont
    else :
        del resp_bef

    response_df=response_df[response_df['_merge']=='both'].drop(columns={'_merge'})
    print('shape of response_df after merging with content_df :', response_df.shape)


    ############
    # Test Map #
    ############

    # Create response_summary
    response_summary = response_df[response_df['sequenceStatus'].str.lower() == 'completed'].groupby(['sequenceId', 'sequenceName', 'jasperUserId'], as_index = False, dropna = False).agg(num_responses = ('contentItemName', 'nunique'))\
    .groupby(['sequenceName'], dropna = False, as_index = False).agg(min_resp = ('num_responses', 'min'),
                                                median_resp = ('num_responses', 'median'),
                                                max_resp = ('num_responses', 'max'),
                                                num_users = ('num_responses', 'count'))
    
    
    test_map = pd.DataFrame()
    test_map['templateId'] =np.nan
    test_map['jasperSequenceName'] =response_summary['sequenceName']
    test_map['sectionName'] = np.nan
    test_map['numQues'] = response_summary['max_resp'] #[90, 90]
    test_map['responseThreshold'] = 0.75 #75% rule 
    test_map['minutesAllowed'] = np.nan # [(72*85)/60]  # all are user inputs if required 
    
    #########################################################
    ###        DB Connection & Data Pool Objects        ###
    #########################################################

    query_frt = """select
        distinct ph.id kbsenrollmentid
    from
        kbs_billing.purchase_history ph
        join bi_reporting.vw_product_detail prd on ph.product_id = prd.product_id
    where
        lower(prd.product_subtype) in ('free trial');
        """

    try:
        frt_enrols
    except:
        frt_enrols = pd.read_sql(sql = query_frt, con = engine.raw_connection())


    ##############
    # Olc enrols #
    ##############
    query_olc = """select
        distinct ph.id kbsenrollmentid
    from
        kbs_billing.purchase_history ph
        join bi_reporting.vw_product_detail prd on ph.product_id = prd.product_id
    where
        lower(prd.product_subtype) in ('online companion')
        ;"""

    try:
        olc_enrols
    except:
        olc_enrols = pd.read_sql(sql = query_olc, con = engine.raw_connection())


    #############
    # Repeaters #
    #############
    query_hsg_repeat = """select
        distinct ph.id kbsenrollmentid
    from
        kbs_billing.purchase_history ph
        join bi_reporting.vw_product_detail prd on ph.product_id = prd.product_id
    where
        ph.initial_delta_k_txn_code in ('404', '405', '406');
        --and ph.created_on >= '2018-01-01';
    """

    try:
        repeaters
    except:
        repeaters = pd.read_sql(sql = query_hsg_repeat, con = engine.raw_connection())
        repeaters['kbsenrollmentid'] = repeaters['kbsenrollmentid'].astype('str')   

    #################
    # Data Pool Obj #
    #################
    # Pass all of the objects into data pool dictionary
    data_pool = {'CI_old_version_dates' : pd.DataFrame(),
    'CI_old_version_list' : pd.DataFrame(),
    'CI_old_keys' : CI_old_keys,
    'frt_enrols' : frt_enrols,
    'olc_enrols' : olc_enrols,
    'repeaters' : repeaters,
    'section_map' : pd.DataFrame(),
    'test_map' : test_map,
    'seqHist_to_exclude' :  pd.DataFrame(),
    'cidf' : pd.DataFrame(),
    'field_test_items' : pd.DataFrame(),
    'ci_cols_to_include' : pd.DataFrame(),
    'interaction_type_list' : pd.DataFrame()}
    
    
    #######################
    # Run Clean Item Data #
    #######################
    result, cleaning_info, rejects_df = clean_item_data(data_path = data_path,
                             results_path = results_path,
                             analysis_name = analysis_name,
                             resp = response_df,
                             remove_users_deleted_sequences = True,
                             remove_dup_CIs = True,
                             remove_no_kbsEID = True,
                             remove_deleted_sequences = True,
                             remove_impo_response_scored = True,
                             remove_impo_timing_seq = True,
                             remove_seq_w_tmq = True,
                             remove_staged_responses = False,
                             remove_FT_items = False,
                             data_pool = data_pool,
                             CI_remove_before_after = 'before', #applicable for old version dates
                             repeat_treatment = 'omit', #default omit
                             mSec_min_threshold = 5000, #there's y/n condition for timing if none provided
                             mSec_max_threshold = None, #so if condition is not needed. # 300000 for comlex
                             sec_min_threshold = None,
                             sec_max_threshold = None,
                             remove_frt_users = True,
                             remove_olc_users = True,
                             remove_repeat_enrolls = True,
                             remove_tutor = True,
                             remove_ada_seq = True,
                             remove_untimed_seq = True,
                             remove_incomplete_seq = False,
                             seq_item_minutes_threshold = None, #input in terms of minutes here
                             seq_section_minutes_threshold = None, #input in terms of minutes here
                             seq_total_minutes_threshold = None, #input in terms of minutes here
                             qbank = False, #if qbank==false and section_map is not empty then sec_num_attempted is validated against given min_items_per_seq from secton_map
                             min_items_per_seq = None, #if qbank==true and section_map provided, sequence_num_attempted is validated against given min_items_per_seq
                             section_calc = True, #if section map provided but min_item_per_seq column not present in it & qbank == false then this gets activated for validating sec_perc_attempted vs section_resp_threshold(comes from section_map)
                             #test_map = test_map, #if section_map is empty this gets activated, and qbank==false then test_resp_threshold comes from test_map
                             #seq_item_resp_threshold = .75,
                             remove_unscored = False,
                             #seqHist_to_exclude = pd.DataFrame(), #should have column sequenceId in it
                             precombined_files = True, #used to add columns from cidf to response_df
                             #cidf = pd.DataFrame(), #pass cidf
                             #interaction_type_list = [], #pass interactionTypeIds in this list
                             #ci_cols_to_include = [], #pass columns names of cidf to include in response_df (ciname, ciid by default added if precombined files is True)
                             remove_repeat_test_administrations = True,
                            remove_seq_wo_dispseq = True,
                            remove_over_time_sequences = True #gains threshold from test_map
                            )

    print('\tcleaning DONE and generated result file')
    
    ###################################
    # Filtering with given items list #
    ###################################

    if os.path.exists(data_path+'Qids List.xlsx'):
        Qids = pd.read_excel(data_path+'Qids List.xlsx', dtype = str)
        # Read in Qids
        
        Qids['QIDs']=Qids['QIDs'].astype('str').str.lower()
        #result_unfiltered = result.copy()

        print('Shape of result before merge :', result.shape)
        print('No. of distinct items :', result['contentItemName'].nunique())
        print('No. of distinct students :', result['studentId'].nunique())
        print('No. of distinct activities :', result['activityId'].nunique())

        result = pd.merge(result, Qids.rename(columns={'QIDs':'contentItemName'}), on = ['contentItemName'], how = 'inner')
        print('=='*10)
        print('Shape of result after merge :', result.shape)
        print('No. of distinct items :', result['contentItemName'].nunique())
        print('No. of distinct students :', result['studentId'].nunique())
        print('No. of distinct activities :', result['activityId'].nunique())

        Qids_order_list=list(Qids['QIDs'])
    
    else :
        Qids_order_list=list(result['contentItemName'].drop_duplicates().tolist())
   

    
    ################################################
    # Export cleaning info & cleaned response_data #
    ################################################
    
    #Exporting cleaned response_data
    result = result.sort_values(by=['studentId', 'activityId', 'displaySeq'], ignore_index= True)
    result.to_csv(results_path+analysis_name+'_responseData.csv', index = False)

    #Exporting cleaning info
    cleaning_info.to_csv(results_path+analysis_name+'_cleaningInfo.csv')

    #Exporting rejects info
    rejects_df.sort_values(by=['studentId', 'templateId'], ignore_index = True).to_csv(results_path+analysis_name+'_rejects_info.csv', index = False)
    
    
    
    ########################################
    # Create metadata: Info Level matrices #
    ########################################
    
    #Reading cleaned data
    all_resp = result.copy()

    all_resp = all_resp.sort_values(by=['activityName', 'displaySeq'], ignore_index=True)

    #col_name:filename
    vars_for_matrix =  {'score':'Item_Scores',
                       'response':'Responses',
                       'mSecUsed':'Milliseconds_per_Item'
                       }

    #making matrices
    big_matrix = make_user_level_matrices(all_resp,
                                   vars_for_matrices = vars_for_matrix,
                                   destination_file_path = results_path,
                                   destination_file_name_prefix = '_User_level_',
                                    analysis_name = analysis_name,
                                    omit_code = '.',
                                    not_seen_code = '-99',
                                    use_display_order = True,
                                   qbank = True,
                                   item_order_list = Qids_order_list
                    )
    
    ########################################
    # Create metadata: Activity Level Info #
    ########################################
    activity_level_info = make_activity_level_info(df = all_resp,
                                              results_path = results_path,
                                              analysis_name = analysis_name)
    
    activity_level_info.to_csv(results_path + analysis_name + '_activity_Level_Info.csv', index = False)
    
    
    ####################################
    # Create metadata: User Level Info #
    ####################################
    user_info = make_user_level_info(df = all_resp,
                                results_path = results_path,
                                analysis_name = analysis_name,
                                test_map = test_map)
    
    user_info.to_csv(results_path + analysis_name + '_User_Level_Info.csv', index = False)
    
    ####################################
    # Create metadata: Item Level Info #
    ####################################
    
    
    #Testing
    df = result.copy()
    df = df[df['repeatOmitted']==False].copy()
        
    #making a seen field for an item to make count_seen
    df['itemSeen'] = df['responseStatus']!='not-reached'

    cidf_summary = df.sort_values(by=['displaySeq']).groupby(by=['contentItemName'], as_index=False, dropna = False).agg(count_att = ('attempted', 'sum'),
                                                                                                                                                        displaySeq=('displaySeq','unique'),
                                                                                                                                                       count_seen = ('itemSeen', 'sum'),
                                                                                                                                                       total_score_pt = ('score', 'sum'),
                                                                                                                                                       first_date = ('dateCreated', 'min'),
                                                                                                                                                       last_date = ('dateCreated', 'max'))

    #merging with cidf_summary
    cidf_summary = pd.merge(cidf_summary, interaction_types.rename(columns = {'content_item_name':'contentItemName'}), on = 'contentItemName', how = 'left')

    cor_ans.rename(columns = {'content_item_name':'contentItemName'}, inplace = True)

    cidf_summary = pd.merge(cidf_summary, cor_ans, how = 'left', on = ['contentItemName'])


    #for count choices
    cidf_summary = pd.merge(cidf_summary, content_df[content_df['contentItemId'].isin(cor_ans['content_item_id'])][['contentItemName', 'countchoices', 'max_points']].drop_duplicates(ignore_index=True), on = ['contentItemName'], how = 'inner')

    #for avg_score & p_value
    cidf_summary['avg_score_pt'] = cidf_summary['total_score_pt']/cidf_summary['count_att']
    cidf_summary['p_value'] = cidf_summary['avg_score_pt']/cidf_summary['max_points']
    cidf_summary['activityName'] =df['activityName']

    #re-working to remove duplicates in interactionTypeName
    cidf_summary['interactionTypeName'] = cidf_summary['interactionTypeName'].str.split(', ').apply(set).str.join(', ')

    #re-arranging columns
    cidf_summary = cidf_summary[['contentItemName','activityName','displaySeq','interactionTypeName', 'count_seen', 'count_att', 'total_score_pt', 'avg_score_pt', 'p_value', 'correct_answer', 'max_points', 'countchoices', 'first_date', 'last_date']]

    #re-arranging the rows
    cidf_summary = cidf_summary.set_index(['contentItemName']).loc[result[['contentItemName']].drop_duplicates().set_index(['contentItemName']).loc[Qids_order_list].reset_index()['contentItemName']].reset_index()

    # exporting the final required cidf_ summary dataFrame
    cidf_summary.to_csv(results_path+analysis_name+'_Content_Item_Info.csv',index=False)
    
    print('\t Sucessfully completed the data cleaning process and generated user files')
    ######################
    # Making Ncount file #
    ######################

    
if __name__ == '__main__': 
    run_code(sys.argv[1])
