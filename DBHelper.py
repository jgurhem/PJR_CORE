import json
import sqlite3
import os
import numpy as np

def extract_set(db_con, val_name):
  cur = db_con.cursor()
  query = f'SELECT DISTINCT auto_all_values.{val_name} FROM auto_filter INNER JOIN auto_all_values ON auto_all_values.id=auto_filter.id WHERE auto_all_values.{val_name} IS NOT NULL;'
  cur.execute(query)
  s = set()
  f = cur.fetchall()
  for i in f:
    if len(i) > 0:
      s.add(i[0])
  return s

def convert_filter_list_to_dic(filter_list):
  dic = dict()
  if filter_list == None:
    return dic
  for value in filter_list:
    split = value.split(':')
    if len(split) == 2:
      k = split[0]
      if k in dic:
        dic[k].update(set(split[1].split(',')))
      else:
        dic[k] = set(split[1].split(','))
  return dic

def read_json_file_raw(filename, filter_dict):
  dbpath = "test.db"
  if os.path.isfile(dbpath):
    os.remove(dbpath)
  con = sqlite3.connect(dbpath)
  cur = con.cursor()
  create_table = 'CREATE TABLE auto_all_values(id INTEGER PRIMARY KEY'

  types = {}
  with open(filename) as fp:
    for cnt, line in enumerate(fp):
      line = line.strip()
      if not line.startswith("{"): continue
      mydict = json.loads(line)
      for k, v in mydict.items():
        if isinstance(v, float):
          types[k] = "FLOAT"
        elif isinstance(v, int):
          types[k] = "INTEGER"
        else:
          types[k] = "TEXT"
  for k, v in types.items():
    create_table += ',' + k + ' ' + v
  create_table += ')'
  cur.execute(create_table)

  with open(filename) as fp:
    for cnt, line in enumerate(fp):
      line = line.strip()
      if not line.startswith("{"): continue
      mydict = json.loads(line)
      columns = ', '.join(mydict.keys())
      placeholders = ':' + ', :'.join(mydict.keys())
      query = 'INSERT INTO auto_all_values (%s) VALUES (%s)' % (columns, placeholders)
      cur.execute(query, mydict)

  filter_query = 'CREATE TABLE auto_filter AS SELECT id FROM auto_all_values'
  if filter_dict:
    filter_query += ' WHERE'
    keys = list(filter_dict.keys())
    if len(keys) > 0:
      filter_query += ' ('
      l = list(filter_dict[keys[0]])
      filter_query += keys[0] + "='" + l.pop() + "'"
      while(len(l) > 0):
        filter_query += ' OR ' + keys[0] + "='" + l.pop() + "'"
      filter_query += ')'
    for ki in range(1, len(keys)):
      filter_query += ' AND ('
      l = list(filter_dict[keys[ki]])
      filter_query += keys[ki] + "='" + l.pop() + "'"
      while(len(l) > 0):
        filter_query += ' OR ' + keys[ki] + "='" + l.pop() + "'"
      filter_query += ')'
  cur.execute(filter_query)

  con.commit()
  return con

def create_case_table(con, relname, case_components):
  cur = con.cursor()
  query = f'CREATE TABLE {relname}_cases AS SELECT DISTINCT '
  for i in case_components:
    query += 'auto_all_values.' + i + ','
  query = query.rstrip(',')
  query += ' FROM auto_filter INNER JOIN auto_all_values ON auto_all_values.id=auto_filter.id;'
  cur.execute(query)

  query = f'CREATE TABLE {relname}_cases_values (id_cases INTEGER, id_values INTEGER)'
  cur.execute(query)

  query = f'SELECT rowid,* FROM {relname}_cases'
  cur.execute(query)
  res = cur.fetchall()
  for i in res:
    query = 'SELECT auto_filter.id FROM auto_filter INNER JOIN auto_all_values ON auto_all_values.id=auto_filter.id '
    if(len(i) > 1):
      query += 'WHERE '
      for j in range(1, len(i) - 1):
        query += 'auto_all_values.' + case_components[j - 1] + "='" + str(i[j]) + "' AND "
      query += 'auto_all_values.' + case_components[len(i) - 2] + "='" + str(i[len(i) - 1]) + "'"
      cur.execute(query)
      res2 = cur.fetchall()
      for r in res2:
        query = f"INSERT INTO {relname}_cases_values VALUES('{i[0]}','{r[0]}')"
        cur.execute(query)
  con.commit()

def compute_stats(con, relname, column):
  cur = con.cursor()
  query = f'CREATE TABLE {relname}_stats (id_cases INTEGER, n INTEGER, min FLAOT, max FLOAT, mean FLOAT, median FLOAT, sum FLOAT, std FLOAT, var FLOAT)'
  cur.execute(query)

  query = f'SELECT rowid FROM {relname}_cases'
  cur.execute(query)
  res = cur.fetchall()
  for i in res:
    query = f'SELECT auto_all_values.{column} FROM {relname}_cases_values INNER JOIN auto_all_values ON auto_all_values.id={relname}_cases_values.id_values WHERE {relname}_cases_values.id_cases={i[0]}'
    cur.execute(query)
    res2 = cur.fetchall()
    res2 = [float(x[0]) for x in res2]
    query = f"INSERT INTO {relname}_stats VALUES({i[0]}, {len(res2)}, {np.min(res2)}, {np.max(res2)}, {np.mean(res2)}, {np.median(res2)}, {np.sum(res2)}, {np.std(res2)}, {np.var(res2)})"
    cur.execute(query)
  con.commit()

def read_json_file(filename, filter_dict, CASE_INFO, column):
  con = read_json_file_raw(filename, filter_dict)
  create_case_table(con, 'auto', CASE_INFO)
  compute_stats(con, 'auto', column)
  return con