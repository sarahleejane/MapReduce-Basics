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
    #LOOK UP BASIC EXAMPLE OF MATrIX MULT TO CHECK METHOD IN HEAD :)
    #WHERE A.col_num = B.row_num
    if record[0]="a":
        mr.emit_intermediate(record[2], record)
    else:
        mr.emit_intermediate(record[1], record)


def reducer(key, list_of_values):
    # Each tuple will be of the form (i, j, value) where each element is an integer.
    #GROUP BY A.row_num, B.col_num
    #SELECT A.row_num, B.col_num, SUM(A.value*B.value)
    for items in list_of_values:
        if items[0]="a":
            for items1 in list_of_values:
                if items[0] = "b":
                    mr.emit((items[1], items1[2],items[3]*items1[3]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
