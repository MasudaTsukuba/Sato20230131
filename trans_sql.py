import csv
import rewriter

def f(sql:str, sparql, mapping, filter_list):
    #subject
    # print(sparql)
    # print(mapping)
    trans_URI = []
    if(sparql['subject']['termType']=='Variable'):
        value = sparql['subject']['value']
        sql = sql.replace(mapping['subject'], value)
        trans_URI.append([value, mapping['subject_uri']])

    elif(sparql['subject']['termType']=='NamedNode'):
        value = sparql['subject']['value']
        uri_function = mapping['subject_uri']
        with open('./URI/' + uri_function + '.csv') as g:
            reader = csv.reader(g)
            for row in reader:
                if(value == row[1]):
                     sql_value = row[0]
                     break     
            sql = rewriter.rewrite_where_sql(sql,sql_value,mapping['subject'])

    #object
    if(sparql['object']['termType']=='Variable'):
        value = sparql['object']['value']
        sql = sql.replace(mapping['object'], value)
        trans_URI.append([value, mapping['object_uri']])

        for filter in filter_list:
            if(filter[0] == value):
                sql = rewriter.rewrite_where_sql_filter(sql,filter[1])

    
    elif(sparql['object']['termType']=='NamedNode'):
        value = sparql['object']['value']
        if(mapping['object_uri'] == '-'):
            if(value != mapping['object']):
                return ['No',[]]

        else:
            value = sparql['object']['value']
            uri_function = mapping['object_uri']
            with open('./URI/' + uri_function + '.csv') as g:
                reader = csv.reader(g)
                for row in reader:
                    if(value == row[1]):
                        sql_value = '"' + row[0] + '"'
                        break
            sql = rewriter.rewrite_where_sql(sql,sql_value,mapping['object'])
            # sql = sql.replace(mapping['object'], value)
    
    else: #termTypeが'Literalのとき'
         value = sparql['object']['value']
         uri_function = mapping['object_uri']
         if(uri_function == 'plain'):
           sql = rewriter.rewrite_where_sql(sql,value,mapping['object'])
           sql = sql.replace(mapping['object'], value)

    return [sql,trans_URI]

