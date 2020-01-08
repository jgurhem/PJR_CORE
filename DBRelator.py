import numpy as np

def matrix_relation(con, list_cases, list_sub_cases, case_of_interest, value_of_interest, relname, stats):
  """ Gather data from the database in order to use them in a plot.
  The list of sub cases (list_sub_cases) is used to select the best case formed with the main case and sub case parameters
  using the minimum value of the first statitic put in the stats variable.

  Function parameters:

  con                --- connection to the sqlite3 database
  list_cases         --- list of parameters to consider as main cases
  list_sub_cases     --- list of parameters to consider as sub cases, can be empty
  case_of_interest   --- parameter to compare to the main cases
  value_of_interest  --- parameter which values will be used as comparison
  relname            --- prefix of the tables used to retrieve data
  stats              --- list of statistics to retrieve from the database
  """

  cur = con.cursor()

  query = f'SELECT DISTINCT {case_of_interest} FROM {relname}_cases'
  cur.execute(query)
  res = cur.fetchall()
  columns = [float(x[0]) for x in res]

  query = 'SELECT DISTINCT '
  for i in list_cases:
    query += i + ','
  query = query.rstrip(',')
  query += f' FROM {relname}_cases'
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
      query += f' FROM {relname}_{value_of_interest}_stats INNER JOIN {relname}_cases ON {relname}_{value_of_interest}_stats.rowid={relname}_cases.rowid WHERE '
      query += f'{case_of_interest}={c}'
      for i in range(len(list_cases)):
        query += f' AND {relname}_cases.' + list_cases[i] + "='" + str(r[i]) + "'"
      cur.execute(query)
      res = cur.fetchall()
      if res != None and len(res) > 0:
        min_pos = np.argmin([float(x[0]) for x in res])
        for i in range(len(stats)):
          m[r][str(c) + '_' + stats[i]] = res[min_pos][i]
        for i in range(len(list_sub_cases)):
          m[r][str(c) + '_' + list_sub_cases[i]] = res[min_pos][len(stats) + i]
      else:
        for i in range(len(stats)):
          m[r][str(c) + '_' + stats[i]] = None
        for i in range(len(list_sub_cases)):
          m[r][str(c) + '_' + list_sub_cases[i]] = None
  return m, columns
