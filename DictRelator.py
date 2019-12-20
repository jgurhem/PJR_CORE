from . import DictHelper as dh
from .Value import Value

def matrix_relation(input_res, list_cases, case_of_interest, value_of_interest):
  m = dict()
  coi_set = set()
  for d in input_res:
    key = dict()
    for c in list_cases:
      if c in d.keys():
        key[c] = str(d[c])
    coi = d[case_of_interest]
    if value_of_interest in d.keys():
      kstr = str(key)
      if kstr not in m:
        m[kstr] = dict()
      dh.add_val(m[kstr], d[value_of_interest], str(coi))
      coi_set.add(str(coi))
  return m, coi_set

def multi_relation(input_res, list_cases, value_of_interest):
  m = dict()
  for d in input_res:
    key = dict()
    for c in list_cases:
      if c in d.keys():
        key[c] = d[c]
    if value_of_interest in d.keys():
      kstr = str(key)
      if kstr not in m:
        m[kstr] = Value(d[value_of_interest])
      else:
        m[kstr].add(Value(d[value_of_interest]))
  return m
      
