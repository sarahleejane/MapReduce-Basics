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
    # key: unique sequence id
    # value: sequence
    mr.emit_intermediate(record[1], record[1])

def reducer(key, list_of_values):
    # value: unique trimmed sequence
    trimmed = list_of_values[0][:-10]

    mr.emit(trimmed)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
