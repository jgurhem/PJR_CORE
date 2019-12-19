class Value:
  def __init__(self, val):
    if isinstance(val, Value):
      self.__n = val.__n
      self.__sum = val.__sum
      self.__min = val.__min
      self.__max = val.__max
    else:
      self.__n = 1
      self.__sum = val
      self.__min = val
      self.__max = val

  def __str__(self):
    s = '<n=' + str(self.__n)
    s += f'; sum= {self.__sum:.3f}'
    s += f'; mean= {self.get_mean():.3f}'
    s += f'; min= {self.__min:.3f}'
    s += f'; max= {self.__max:.3f}'
    s += '>'
    return s

  def __repr__(self):
    return self.__str__()

  def add(self, val):
    if isinstance(val, Value):
      self.__n += val.__n
      self.__sum += val.__sum
      self.__min = min(self.__min, val.__min)
      self.__max = max(self.__max, val.__max)
    else:
      self.__n += 1
      self.__sum += val
      self.__min = min(self.__min, val)
      self.__max = max(self.__max, val)

  def get_mean(self):
    return self.__sum / self.__n

  def get_min(self):
    return self.__min

  def get_max(self):
    return self.__max

  def get_val(self):
    return self
