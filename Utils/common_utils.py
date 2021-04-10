import hashlib


def create_hash_id(df):
    columns = list(df.columns)
    df['hash_key'] = df[columns[0]].str.cat(df[columns[1:]], sep='')
    df['id'] = df['hash_key'].apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
    df = df[['id'] + columns]
    return df


def create_cond_string(key_value_dict, flag='where'):
    upt_list = tuple(zip(key_value_dict.keys(), key_value_dict.values()))
    query_list = ["=\'".join(updt_key_value) + "\'" for updt_key_value in upt_list]
    separator = ' and ' if flag == 'where' else ' , '
    final_string = separator.join(query_list)
    return final_string
