import MapReduce
import sys
from collections import defaultdict

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: table
    # value: orderid, then all attributes
    key = record[1]
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: orderid
    # value: all attributes

    #do cross product of all order with lineitem
    for single_list in list_of_values:
      if single_list[0]=='order':
        for single_list2 in list_of_values:
          if single_list2[0]=='line_item':
            mr.emit(single_list+single_list2)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
