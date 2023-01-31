import json
import csv
import sqlite3
import trans_sql
import itertools
from collections import OrderedDict

# ------ ユーザから得て, JSON形式に変換したSPARQLを取り込む --------
json_open = open('query.json', 'r')
query_dict = json.load(json_open)
json_open.close()
# ------------------------------------------------------------

# ------ マッピングデータを使ってSPARQL -> SQL に変換する ----------

#マッピングデータの取り込み
json_open = open('./mapping/proposed/mapping.json', 'r')
mapping_dict = json.load(json_open)
json_open.close()

#出力する変数リストを作成
var_list = []
for i in range(len(query_dict['variables'])):
    var_list.append(query_dict['variables'][i]['value'])

#FILTERの条件リストを作成
filter_list = []
for i in range(len(query_dict['where'])):
    if(query_dict['where'][i]['type'] == 'filter'):
        prefix = query_dict['where'][i]['expression']['args']
        filter_list.append([prefix[0]['value'],prefix[0]['value'] + ' ' + query_dict['where'][i]['expression']['operator'] + ' "' + prefix[1]['value'] + '"'])

#print(var_list)
#print(filter_list)



#SPARQLクエリの各トリプルパターンから候補のSQLクエリを検索
SQL_query = []
transURI_list = []

for i in range(len(query_dict['where'][0]['triples'])):
    SQL_subquery = []
    q_predicate = query_dict['where'][0]['triples'][i]['predicate']['value']
    for j in range(len(mapping_dict)):
        predicate = mapping_dict[j]["predicate"]

        if(q_predicate == predicate):
            sql = mapping_dict[j]["SQL"]
            query = query_dict['where'][0]['triples'][i]
            mapping = mapping_dict[j]
            answer = trans_sql.f(sql,query,mapping,filter_list)
            re_sql = answer[0]
            if(answer[1] != []):
                for i in range(len(answer[1])):
                    transURI_list.append(answer[1][i])
            if(re_sql != 'No'):
                SQL_subquery.append(re_sql)

    insert_SQL = ''
    if(len(SQL_subquery)!= 0):
        for k in range(len(SQL_subquery)):
            insert_SQL = insert_SQL + SQL_subquery[k]
            if(k != len(SQL_subquery) - 1):
                insert_SQL = insert_SQL + ' UNION '
        insert_SQL = insert_SQL.replace(';','') + ';'

    SQL_query.append(insert_SQL)



print(SQL_query)

# ------------------------------------------------------------

# --------- 各SQLクエリを実行 ----------------------------------


dbname = 'data_set/data.db'

conn = sqlite3.connect(dbname)

# SQLiteを操作するためのカーソルを作成
cur = conn.cursor()

results = []
headers = []

# それぞれのクエリの実行し、そのクエリ結果や結果のヘッダーを格納
for q in SQL_query:
    results.append((cur.execute(q).fetchall()))
    headers.append([col[0] for col in cur.description])

join_result = results[0]
join_header = headers[0]

#各クエリ結果をJOINして, ヘッダーをjoin_header, 結果内容をjoin_resultに格納
# for i in range(1,len(results)):
#     for j in range(0,len(join_header[0])):
#         if join_header[j] == headers[i][0]:
#             join_result = [tuple(OrderedDict.fromkeys(r1 + r2)) for r1 in join_result for r2 in results[i] if r1[0] == r2[0]]
#             join_header = list(OrderedDict.fromkeys(join_header + headers[i]))
#             break
#         elif join_header[j] == headers[i][1]:
#             join_result = [tuple(OrderedDict.fromkeys(r1 + r2)) for r1 in join_result for r2 in results[i] if r1[0] == r2[1]]
#             join_header = list(OrderedDict.fromkeys(join_header + headers[i]))
#             break
# print(headers)

#各クエリ結果をJOINして, ヘッダーをjoin_header, 結果内容をjoin_resultに格納
for i in range(1,len(results)):
    # print(i)
    # print(headers[i])
    # print(join_header)
    for j in range(0,len(join_header)):
        if join_header[j] == headers[i][0]:
            join_result = [r1 + r2 for r1 in join_result for r2 in results[i] if r1[j] == r2[0]]
            join_header = list(join_header + headers[i])
            break
        elif len(headers[i]) == 2:
            if(join_header[j] == headers[i][1]):
                join_result = [r1 + r2 for r1 in join_result for r2 in results[i] if r1[j] == r2[1]]
                join_header = list(join_header + headers[i])
                break

#print(join_header)
#print(join_result)

# ------------------------------------------------------------

# --------- SQLクエリ結果をSPARQLクエリ結果に合わせるため、必要に応じて文字列->URIに変換する ----------------------------------
transURI_list = list(set(tuple(i) for i in transURI_list))
transURI_list = [list(i) for i in transURI_list]
#print(transURI_list)

join_result = [list(i) for i in join_result]


#print(join_result)

distinct_head_number_set = []
distinct_head_set = []


#ユーザがSPARQLクエリのSELECT句で指定した変数のみを結果表示するために、join_result何番目に必要な属性があるか調べる
for i in range(len(join_header)):
    flag1 = 0
    flag2 = 0
    for j in range(len(distinct_head_set)):
        if(distinct_head_set[j] == join_header[i]):
            flag1 = 1
            break
    
    if flag1 == 0:
        for j in range(len(var_list)):
            if(join_header[i] == var_list[j]):
                flag2 = 1
                break
            

    if(flag2 == 1):
        distinct_head_number_set.append(i)
        distinct_head_set.append(join_header[i])

print(join_header)
print(distinct_head_number_set)
print(distinct_head_set)
#join_result.append([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14])

fix = 0

if (len(distinct_head_number_set) == 1):
        re_join_result = []
        for row in join_result:
            re_row = [row[distinct_head_number_set[0]]]
            re_join_result.append(re_row)
        join_result = re_join_result

for i in range(len(distinct_head_number_set)-1):
    left = distinct_head_number_set[i] - fix
    right = distinct_head_number_set[i+1] - fix
    print(left)
    print(right)
    print(fix)
    re_join_result = []
    if(i == 0):
        for row in join_result:
            re_row = [row[left]] + list(row[right:])
            re_join_result.append(re_row)
        join_result = re_join_result
        fix = fix + right - 1
    elif(i == len(distinct_head_number_set)-2):
        #print(join_result)
        for row in join_result:
            re_row = list(row[:left+1]) + [row[right]]
            re_join_result.append(re_row)
        join_result = re_join_result
    else:
        for row in join_result:
            re_row = list(row[:left+1]) + list(row[right:])
            re_join_result.append(re_row)
        join_result = re_join_result
        fix = fix + right - left - 1
    #print(re_join_result)

# re_row = [[]]
# k = 0
# for i in range(len(distinct_head_number_set)):
#     print(distinct_head_number_set[i])
#     j = 0
#     for row in join_result:
#         re_row[k][j] = re_row[k][j] + [row[distinct_head_number_set[i]]]
        


#print(join_result)

#結果の表示, output.csvに出力される

for i in range(len(distinct_head_set)):
    for transURI in transURI_list:
        if((distinct_head_set[i] == transURI[0]) & (transURI[1] != 'plain')):
             with open('./URI/' + transURI[1] + '.csv') as g:
                reader = csv.reader(g)
                for row in reader:
                    for j in range(len(join_result)):
                        if(join_result[j][i] == row[0]):
                            join_result[j][i] = row[1]

with open('output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(distinct_head_set)
    writer.writerows(join_result)