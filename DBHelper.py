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

def extract_set_filter(db_con, filter_prefix, val_name):
  cur = db_con.cursor()
  query = f'SELECT DISTINCT auto_all_values.{val_name} FROM {filter_prefix}_filter INNER JOIN auto_all_values ON auto_all_values.id={filter_prefix}_filter.id WHERE auto_all_values.{val_name} IS NOT NULL;'
  cur.execute(query)
  s = set()
  f = cur.fetchall()
  for i in f:
    if len(i) > 0:
      s.add(i[0])
  return s

def extract_set_all_values(db_con, val_name):
  cur = db_con.cursor()
  query = f'SELECT DISTINCT {val_name} FROM auto_all_values WHERE {val_name} IS NOT NULL;'
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
    if len(split) == 1:
      k = split[0]
      if k not in dic:
        dic[k] = set()
    elif len(split) == 2:
      k = split[0]
      if k in dic:
        dic[k].update(set(split[1].split(',')))
      else:
        dic[k] = set(split[1].split(','))
  return dic

def generate_conditions_where(filter_dict):
  filter_query = ''
  for k in filter_dict:
    l = list(filter_dict[k])
    if len(l) > 0:
      filter_query += ' ('
      while(len(l) > 0):
        filter_query += k + "='" + str(l.pop()) + "' OR "
      if filter_query.endswith(' OR '):
        filter_query = filter_query[:-4]
      filter_query += ') AND'
  if filter_query.endswith(' AND'):
    filter_query = filter_query[:-4]
  return filter_query

def create_filter(con, relname, filter_dict):
  cur = con.cursor()
  cur.execute(f'DROP TABLE IF EXISTS {relname}_filter')
  filter_query = f'CREATE TABLE {relname}_filter AS SELECT id FROM auto_all_values'
  if filter_dict:
    filter_query += ' WHERE'
    filter_query += generate_conditions_where(filter_dict)
  cur.execute(filter_query)
  con.commit()

def read_json_file_raw(dbpath, json_input_file):
  if os.path.isfile(dbpath):
    os.remove(dbpath)
  con = sqlite3.connect(dbpath)
  cur = con.cursor()
  create_table = 'CREATE TABLE auto_all_values(id INTEGER PRIMARY KEY'

  types = {}
  with open(json_input_file) as fp:
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

  with open(json_input_file) as fp:
    for cnt, line in enumerate(fp):
      line = line.strip()
      if not line.startswith("{"): continue
      mydict = json.loads(line)
      columns = ', '.join(mydict.keys())
      placeholders = ':' + ', :'.join(mydict.keys())
      query = 'INSERT INTO auto_all_values (%s) VALUES (%s)' % (columns, placeholders)
      cur.execute(query, mydict)
  con.commit()
  return con

def create_case_table(con, relname, case_components):
  cur = con.cursor()
  cur.execute(f'DROP TABLE IF EXISTS {relname}_cases')
  query = f'CREATE TABLE {relname}_cases AS SELECT DISTINCT '
  for i in case_components:
    query += 'auto_all_values.' + i + ','
  query = query.rstrip(',')
  query += f' FROM {relname}_filter INNER JOIN auto_all_values ON auto_all_values.id={relname}_filter.id;'
  cur.execute(query)

  cur.execute(f'DROP TABLE IF EXISTS {relname}_cases_values')
  query = f'CREATE TABLE {relname}_cases_values (id_cases INTEGER, id_values INTEGER)'
  cur.execute(query)

  query = f'INSERT INTO {relname}_cases_values SELECT {relname}_cases.rowid,auto_all_values.id FROM {relname}_cases INNER JOIN auto_all_values ON ('
  for i in case_components:
    query += 'auto_all_values.' + i + ' is ' + f"{relname}_cases." + i + ' AND '
  if query.endswith(' AND '):
    query = query[:-5]
  query += f') INNER JOIN {relname}_filter ON auto_all_values.id={relname}_filter.id;'
  cur.execute(query)
  con.commit()

def compute_stats(con, relname, column):
  cur = con.cursor()
  cur.execute(f'DROP TABLE IF EXISTS {relname}_{column}_stats')
  query = f'CREATE TABLE {relname}_{column}_stats (id_cases INTEGER, n INTEGER, min FLAOT, max FLOAT, mean FLOAT, median FLOAT, sum FLOAT, std FLOAT, var FLOAT)'
  cur.execute(query)

  query = f'SELECT rowid FROM {relname}_cases'
  cur.execute(query)
  res = cur.fetchall()
  for i in res:
    query = f'SELECT auto_all_values.{column} FROM {relname}_cases_values INNER JOIN auto_all_values ON auto_all_values.id={relname}_cases_values.id_values WHERE {relname}_cases_values.id_cases={i[0]}'
    cur.execute(query)
    res2 = cur.fetchall()
    res2 = [float(x[0]) for x in res2]
    query = f"INSERT INTO {relname}_{column}_stats VALUES({i[0]}, {len(res2)}, {np.min(res2)}, {np.max(res2)}, {np.mean(res2)}, {np.median(res2)}, {np.sum(res2)}, {np.std(res2)}, {np.var(res2)})"
    cur.execute(query)
  con.commit()

def read_json_file(dbpath, json_input_file, filter_dict, CASE_INFO, column_list):
  con = read_json_file_raw(dbpath, json_input_file)
  create_filter(con, 'auto', filter_dict)
  create_case_table(con, 'auto', CASE_INFO)
  for column in column_list:
    compute_stats(con, 'auto', column)
  return con
