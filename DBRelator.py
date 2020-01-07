def matrix_relation(con, list_cases, case_of_interest, relname, op):
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
      query = f'SELECT {op} FROM {relname}_stats INNER JOIN {relname}_cases ON {relname}_stats.rowid={relname}_cases.rowid WHERE '
      query += f'{case_of_interest}={c}'
      for i in range(len(list_cases)):
        query += f' AND {relname}_cases.' + list_cases[i] + "='" + str(r[i]) + "'"
      cur.execute(query)
      res = cur.fetchone()
      if res != None:
        m[r][c] = res[0]
      else:
        m[r][c] = None
  return m, columns
