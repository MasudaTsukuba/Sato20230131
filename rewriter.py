def rewrite_where_sql(sql:str,where_value,var):
    if(('WHERE' in sql)):
        #print('A')
        index = sql.find(';')
        re_sql = sql[:index] + ' AND ' + var + ' = ' + where_value + ';'
    else:
        index = sql.find(';')
        re_sql = sql[:index] + ' WHERE ' + var + ' = ' + where_value + ';'
    return re_sql

def rewrite_where_sql_filter(sql:str,filter):
    if(('WHERE' in sql)):
        #print('A')
        index = sql.find(';')
        re_sql = sql[:index] + ' AND ' + filter + ';'
    else:
        index = sql.find(';')
        re_sql = sql[:index] + ' WHERE ' + filter + ';'
    return re_sql