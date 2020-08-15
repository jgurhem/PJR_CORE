import numpy as np
from . import DBHelper as dh

def matrix_relation(con, dict_cases, list_sub_cases, case_of_interest, value_of_interest, relname, stats, ratios = list()):
  """ Gather data from the database in order to use them in a plot.
  The list of sub cases (list_sub_cases) is used to select the best case formed with the main case and sub case parameters
  using the minimum value of the first statitic put in the stats variable.

  Function parameters:

  con                --- connection to the sqlite3 database
  dict_cases         --- dictionnary where the keys are the parameters to consider as main cases
                                           the values are a list of conditions on the parameters
                         ex : {param1:[value1,value2],param2:[value3,value4]}
  list_sub_cases     --- list of parameters to consider as sub cases, can be empty
  case_of_interest   --- parameter to compare to the main cases
  value_of_interest  --- parameter which values will be used as comparison
  relname            --- prefix of the tables used to retrieve data
  stats              --- list of statistics to retrieve from the database
  ratios             --- list of ratios the cases have to respect wheree param1*ratio=param2
                         param1 and param2 have to be included in dict_cases or list_sub_cases
                         ex : ['param1(str),param2(str),ratio(double)', '...', ..]
  """

  cur = con.cursor()

  query = f'SELECT DISTINCT {case_of_interest} FROM {relname}_cases'
  cur.execute(query)
  res = cur.fetchall()
  columns = [x[0] for x in res]

  list_cases = list(dict_cases.keys())
  query = 'SELECT DISTINCT '
  for i in list_cases:
    query += i + ','
  query = query.rstrip(',')
  query += f' FROM {relname}_cases WHERE'
  query += dh.generate_conditions_where(dict_cases)
  if query.endswith(' WHERE'):
    query = query[:-6]
  cur.execute(query)
  rows = cur.fetchall()

  m = dict()
  for r in rows:
    m[r] = dict()
    for c in columns:
      query = f'SELECT '
      for i in stats:
        query += f'{relname}_{value_of_interest}_stats.' + i + ','
      query = query.rstrip(',')
      for i in list_sub_cases:
        query += f',{relname}_cases.' + i
      query += f',{relname}_cases.rowid FROM {relname}_{value_of_interest}_stats INNER JOIN {relname}_cases ON {relname}_{value_of_interest}_stats.rowid={relname}_cases.rowid WHERE '
      query += f"{case_of_interest}='{c}'"
      for i in range(len(list_cases)):
        query += f' AND {relname}_cases.' + list_cases[i] + "='" + str(r[i]) + "'"
      for i in ratios:
        split = i.split(',')
        if len(split) == 3:
          query += f' AND {relname}_cases.{split[0]}*{split[2]}={relname}_cases.{split[1]}'
      cur.execute(query)
      res = cur.fetchall()

      if res != None and len(res) > 0:
        min_pos = np.argmin([float(x[0]) for x in res])
        for i in range(len(stats)):
          m[r][str(c) + '_' + stats[i]] = res[min_pos][i]
        for i in range(len(list_sub_cases)):
          m[r][str(c) + '_' + list_sub_cases[i]] = res[min_pos][len(stats) + i]
        id_case = res[min_pos][len(stats) + len(list_sub_cases)]
        m[r][str(c) + '_' + f'{relname}_cases.rowid'] = id_case
        m[r][str(c) + '__sql_get_contributions'] = f'SELECT * FROM {relname}_cases_values INNER JOIN auto_all_values ON auto_all_values.id={relname}_cases_values.id_values WHERE {relname}_cases_values.id_cases={id_case}'
      else:
        for i in range(len(stats)):
          m[r][str(c) + '_' + stats[i]] = None
        for i in range(len(list_sub_cases)):
          m[r][str(c) + '_' + list_sub_cases[i]] = None
        m[r][str(c) + '_' + f'{relname}_cases.rowid'] = None
        m[r][str(c) + '__sql_get_contributions'] = None
  return m, columns
