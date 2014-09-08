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
    person_with_friend = record[0]
    mr.emit_intermediate(person_with_friend, 1)

def reducer(key, list_of_values):
    # key: person
    # value: friend count

    #count how many friends
    friend_count = len(list_of_values)
    mr.emit((key, friend_count))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
