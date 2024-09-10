import pandas as pd
import datetime

def batch_cdr():
    #case1: updated data
    #case2: added new data
    #case3: deleted data
    #get cdr_modified_date from last 3 days
    existing_clean_cdr_path = r"batch_job\cdr.parquet"
    new_batch_cdr_path = r"dwh\cdr\cdr_case1.parquet"
    output_clean_cdr_path = r"batch_job\cdr.parquet"

    df_before = pd.read_parquet(existing_clean_cdr_path)
    df_after = pd.read_parquet(new_batch_cdr_path)
    
    df_before['cdr_modified_date'] = pd.to_datetime(df_before['cdr_modified_date'])
    df_after['cdr_modified_date'] = pd.to_datetime(df_after['cdr_modified_date'])
    #get last 3 days modified data
    updated = df_after[df_after['cdr_modified_date'] > datetime.datetime.now() - datetime.timedelta(days=3)]

    #check if updated has data or not
    if updated.shape[0] > 0:
        #unique business id
        u_cdr = df_after['cdr_id'].unique()
        to_update = df_after[df_after['cdr_id'].isin(updated['cdr_id'])]
        #update old data with updated data, covers updated data and new data
        df_before = df_before[~df_before['cdr_id'].isin(updated['cdr_id'])]
        df_before = pd.concat([df_before, to_update])
        #inner join df_before u_cdr to cover deleted data
        df_before = df_before[df_before['cdr_id'].isin(u_cdr)]
        df_before.to_parquet(output_clean_cdr_path, index=False)
        return "data is updated"
    
    return "data is updated"