import numpy as np
from . import DBHelper as dh
import json

def matrix_relation(con, filter_dict, list_cases, list_sub_cases, case_of_interest, value_of_interest, relname, stats, ratios = list()):
  """ Gather data from the database in order to use them in a plot.
  The list of sub cases (list_sub_cases) is used to select the best case formed with the main case and sub case parameters
  using the minimum value of the first statitic put in the stats variable.

  Function parameters:

  con                --- connection to the sqlite3 database
  filter_dict        --- dictionnary where the keys are the parameters to filter
                                           the values are a list of conditions on the parameters
                         ex : {param1:[value1,value2],param2:[value3,value4]}
  list_cases         --- list of parameters to consider as cases, cannot be empty
  list_sub_cases     --- list of parameters to consider as sub cases, can be empty
  case_of_interest   --- parameter to compare to the main cases
  value_of_interest  --- parameter which values will be used as comparison
  relname            --- prefix of the tables used to retrieve data
  stats              --- list of statistics to retrieve from the database
  ratios             --- list of ratios the cases have to respect wheree param1*ratio=param2
                         param1 and param2 have to be included in dict_cases or list_sub_cases
                         ex : ['param1(str),param2(str),ratio(double)', '...', ..]

  Returns:
  A dict where
    -- the keys are a str containing a json dict in which the keys are dict_cases.keys() and the values are the case associted
    -- the values are dictionnaries in which the keys are the values of the case_of_interest
                                             and the values are the value_of_interest for each key

  Metadata for each value of the dict of value_of_interest:

  __{relname}_cases.rowid     --- rowid of the case that has this value
  __{relname}_cases.Nval      --- Number of rows of the database that contribute to this value
  __{relname}_cases.Ncase     --- Number of cases from which the best case is selected
  __sql_get_contributions     --- SQL query to retrieve all the rows of the database that contribute to the associated value

  """

  cur = con.cursor()

  query = f'SELECT DISTINCT {case_of_interest} FROM {relname}_cases WHERE'
  query += dh.generate_conditions_where(filter_dict)
  if query.endswith(' WHERE'):
    query = query[:-6]
  cur.execute(query)
  res = cur.fetchall()
  columns = [x[0] for x in res]

  query = 'SELECT DISTINCT '
  for i in list_cases:
    query += i + ','
  query = query.rstrip(',')
  query += f' FROM {relname}_cases WHERE'
  query += dh.generate_conditions_where(filter_dict)
  if query.endswith(' WHERE'):
    query = query[:-6]
  cur.execute(query)
  rows = cur.fetchall()

  m = dict()
  for tuple_r in rows:
    r = json.dumps(dict(zip(list_cases, tuple_r)))
    m[r] = dict()
    for c in columns:
      m[r][c] = dict()
      query = f'SELECT '
      for i in stats:
        query += f'{relname}_{value_of_interest}_stats.' + i + ','
      query = query.rstrip(',')
      for i in list_sub_cases:
        query += f',{relname}_cases.' + i
      query += f',{relname}_cases.rowid,{relname}_{value_of_interest}_stats.N FROM {relname}_{value_of_interest}_stats INNER JOIN {relname}_cases ON {relname}_{value_of_interest}_stats.rowid={relname}_cases.rowid WHERE '
      query += f"{case_of_interest}='{c}'"
      for i in range(len(list_cases)):
        if tuple_r[i] == None:
          query += f' AND {relname}_cases.' + list_cases[i] + " is null"
        else:
          query += f' AND {relname}_cases.' + list_cases[i] + "='" + str(tuple_r[i]) + "'"
      query += ' AND '
      query += dh.generate_conditions_where(filter_dict, f'{relname}_cases')
      if query.endswith(' AND '):
        query = query[:-5]
      for i in ratios:
        split = i.split(',')
        if len(split) == 3:
          query += f' AND {relname}_cases.{split[0]}*{split[2]}={relname}_cases.{split[1]}'
      cur.execute(query)
      res = cur.fetchall()

      if res != None and len(res) > 0:
        min_pos = np.argmin([float(x[0]) for x in res])
        for i in range(len(stats)):
          m[r][c][stats[i]] = res[min_pos][i]
        for i in range(len(list_sub_cases)):
          m[r][c][list_sub_cases[i]] = res[min_pos][len(stats) + i]
        id_case = res[min_pos][len(stats) + len(list_sub_cases)]
        Nval_case = res[min_pos][len(stats) + len(list_sub_cases) + 1]
        m[r][c][f'__{relname}_cases.rowid'] = id_case
        m[r][c][f'__{relname}_cases.Nval'] = Nval_case
        m[r][c][f'__{relname}_cases.Ncase'] = len(res)
        m[r][c]['__sql_get_contributions'] = f'SELECT * FROM {relname}_cases_values INNER JOIN auto_all_values ON auto_all_values.id={relname}_cases_values.id_values WHERE {relname}_cases_values.id_cases={id_case}'
      else:
        for i in range(len(stats)):
          m[r][c][stats[i]] = None
        for i in range(len(list_sub_cases)):
          m[r][c][list_sub_cases[i]] = None
        m[r][c][f'__{relname}_cases.rowid'] = None
        m[r][c]['__sql_get_contributions'] = None
        m[r][c][f'__{relname}_cases.Nval'] = 0
        m[r][c][f'__{relname}_cases.Ncase'] = 0
  return m, columns


def ordered_cases_relation(con, cases_of_interest, value_of_interest, relname, stats):
  """ Return the rowid of the cases (defined by cases_of_interest) ordered (ascending) by the minimum of value_of_interest.

  Function parameters:

  con                --- connection to the sqlite3 database
  cases_of_interest  --- list of parameters used as base to return the first best cases for the values of these parameters
  value_of_interest  --- parameter which values will be used as comparison
  relname            --- prefix of the tables used to retrieve data
  stats              --- list of statistics to retrieve from the database
  """

  cur = con.cursor()

  query = 'SELECT DISTINCT '
  for i in cases_of_interest:
    query += i + ','
  query = query.rstrip(',')
  query += f' FROM {relname}_cases'
  cur.execute(query)
  interests = cur.fetchall()

  results = dict()
  for interest in interests:
    query = f'SELECT {relname}_cases.rowid'
    for i in stats:
      query += f',{relname}_{value_of_interest}_stats.' + i
    query += f' FROM {relname}_{value_of_interest}_stats INNER JOIN {relname}_cases ON {relname}_{value_of_interest}_stats.rowid={relname}_cases.rowid WHERE'
    for i in range(len(cases_of_interest)):
      query += f' {relname}_cases.' + cases_of_interest[i] + "='" + str(interest[i]) + "' AND"
    if query.endswith(' AND'):
      query = query[:-4]
    cur.execute(query)
    res = cur.fetchall()
    res = sorted(res, key = lambda x : float(x[1]))
    res = [x[0] for x in res]
    results[tuple(interest)] = res
  return results

