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
    # key: personA who has a friend
    # value: friend count
    person_a = record[0]
    person_b = record[1]
    mr.emit_intermediate(person_a + person_b, record)
    r_record = [record[1], record[0]]
    mr.emit_intermediate(person_b + person_a, r_record)

def reducer(key, list_of_values):
    # key: person
    # value: friend count

    #count how many friends
    relationship_count = len(list_of_values)
    if relationship_count == 1:
      mr.emit((list_of_values[0][0],list_of_values[0][1]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
