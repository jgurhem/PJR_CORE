from operator import itemgetter

CASES_DATA = ['n', 'min', 'max', 'mean', 'median']

def __process_db(con, relname, value_of_interest, case_names, prefix, casediff):
  cur = con.cursor()
  query = 'SELECT '
  for i in CASES_DATA:
    query += f'{i},'
  query = query.rstrip(',')
  for i in case_names:
    query += f',{i[0]}'
  query += f' FROM {relname}_{value_of_interest}_stats INNER JOIN {relname}_cases ON {relname}_{value_of_interest}_stats.rowid={relname}_cases.rowid'
  cur.execute(query)
  res = cur.fetchall()
  for r in res:
    data = dict()
    for i in range(len(CASES_DATA)):
      data[CASES_DATA[i]] = r[i]
    case = ()
    for i in range(len(case_names)):
      case += (r[i + len(CASES_DATA)],)
    if case not in casediff:
      casediff[case] = dict()
    casediff[case][prefix] = data


def aggregate_data(con1, con2, relname, value_of_interest):
  """ Aggregate results from similar cases of two data bases

  Function parameters:

  con1, con2         --- connections to the sqlite3 databases
  relname            --- prefix of the tables used to compare data
  value_of_interest  --- parameter which values will be used as comparison
  """

  cur1 = con1.cursor()
  cur2 = con2.cursor()
  query = f'SELECT name,type FROM PRAGMA_TABLE_INFO("{relname}_cases") SQL ORDER BY name ASC'
  cur1.execute(query)
  cur2.execute(query)
  res1 = cur1.fetchall()
  res2 = cur2.fetchall()

  if res1 != res2:
    print("error : the two databases do not have the same set of cases")
    sys.exit(1)

  casediff = dict()
  casediff['__case_names'] = res1
  __process_db(con1, relname, value_of_interest, res1, 'db1', casediff)
  __process_db(con2, relname, value_of_interest, res1, 'db2', casediff)
  return casediff


