from .Value import Value
import json

class DictFilter:
  def __init__(self, md, info):
    self.__l = dict()
    for i in info:
      if i in md.keys():
        self.__l[i] = md[i]

  def __str__(self):
     return str(self.__l)

  def __repr__(self):
     return str(self)

  def get_dict(self):
     return self.__l

class DictFilterValue:
  def __init__(self, md, info):
    self.__l = dict()
    for i in info:
      if i in md.keys():
        if isinstance(md[i], float):
          self.__l[i] = Value(md[i])
        elif len(md[i]) > 0:
          self.__l[i] = Value(float(md[i]))

  def __str__(self):
     return str(self.__l)

  def get_dict(self):
     return self.__l
#end class DictFilterValue


def extract_set(md, val_name):
  s=set()
  for d in md:
    v = d.get(val_name, None)
    if v != None:
      s.add(v)
  s = sorted(s)
#  print(val_name + " : " + str(s))
  return s

def add_val(md_out, val_from, val_to):
  prev = md_out.get(val_to, None)
  if prev == None:
    md_out[val_to] = Value(val_from)
  else:
    md_out[val_to].add(val_from)

def get_val(md, el, op_type):
    r = md.get(el, None)
    if r == None:
      return r
    else:
      return getattr(r, 'get_' + op_type)()


def __filter_tuple(v, tup):
  for i in tup:
    if str(v) == i: return True
  return False

def __filter(md, fd):
  for k in fd.keys():
    if not __filter_tuple(md[k], fd[k]): return False
  return True

def read_json_file(filename, filter_dict, op_type, CASE_INFO, VALUE_INFO):
  input_res = dict()
  with open(filename) as fp:
    for cnt, line in enumerate(fp):
      line=line.strip()
      if not line.startswith("{"): continue
      mydict=json.loads(line)
      if __filter(mydict, filter_dict):
        c = DictFilter(mydict, CASE_INFO)
        v_dict = DictFilterValue(mydict, VALUE_INFO).get_dict()
        c_str = str(c)
        if c_str not in input_res.keys():
          input_res[c_str] = c.get_dict()
        for k, v in v_dict.items():
          add_val(input_res[c_str], v, k)

  for d in input_res.values():
    for i in VALUE_INFO:
      if i in d.keys():
        d[i] = get_val(d, i, op_type)

  return input_res.values()


def read_json_file_raw(filename, filter_dict, op_type):
  input_res = list()
  with open(filename) as fp:
    for cnt, line in enumerate(fp):
      line=line.strip()
      if not line.startswith("{"): continue
      mydict=json.loads(line)
      if __filter(mydict, filter_dict):
        input_res.append(mydict)
  return input_res
