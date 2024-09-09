#data health
def check_health(df,dtype='all'):
    nulls = df.isnull().sum().reset_index()
    nulls.columns = ['col_name','null_count']
    unique = df.nunique().reset_index()
    unique.columns = ['col_name','unique_count']
    out = nulls.merge(unique,how='left',on='col_name')
    out['row_count'] = df.shape[0]
    out['null_pct'] = out['null_count']/out['row_count']
    out['unique_pct'] = out['unique_count'] / out['row_count']
    out['dupe_count'] = out['row_count'] - out['unique_count']
    out['dupe_pct'] = out['dupe_count']/out['row_count']
    try:
        dtype_int = df.dtypes.reset_index()
        dtype_int.columns = ['col_name','col_dtype']
        dtype_int.col_dtype = dtype_int.col_dtype.astype(str)
        int_col = dtype_int[dtype_int.col_dtype.isin(['int64','float64','datetime64[ns]'])]
        int_col = dtype_int[dtype_int.col_dtype.isin(['int64','float64','datetime64[ns]'])]
        int_info = df[int_col.col_name.values].describe().T.reset_index()
        int_info.columns = ['col_name',	'count','mean',	'min',	'25_pct','50_pct',	'75_pct',	'max','std']
        int_info = int_info[['col_name','mean','std','min','max','25_pct','50_pct','75_pct']]
        int_info['col_type'] = 'numeric'
            
        dtype_obj = df.dtypes.reset_index()
        dtype_obj.columns = ['col_name','col_dtype']
        dtype_obj.col_dtype = dtype_obj.col_dtype.astype(str)
        obj_col = dtype_obj[dtype_obj.col_dtype.isin(['object'])]
        obj_col = df[obj_col.col_name.values].describe().T.reset_index()
        obj_col.columns = ['col_name','count','unique','mode','freq']
        obj_col = obj_col[['col_name','mode','freq']]
        obj_col['col_type'] = 'object'
        out= out[['col_name','null_count','unique_count','row_count','dupe_count','null_pct','unique_pct','dupe_pct']]

        if dtype=='all':
            out_1 = out.merge(int_info,how='left',on='col_name').merge(obj_col,how='left',on='col_name')
            out_1['col_type_x'] = out_1['col_type_x'].fillna(out_1['col_type_y'])
            out_1.rename(columns={'col_type_x':'col_types'},inplace=True)
            return out_1.drop(columns='col_type_y')
        if dtype=='num_only':
            out_2 = out.merge(int_info,how='inner',on='col_name')
            return out_2
        if dtype=='obj_only':
            out_3 = out.merge(obj_col,how='inner',on='col_name')
            return out_3
        if dtype=='basic':
            return out
    except:
        return out
