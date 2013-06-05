import MapReduce
import sys

"""
Join Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[1]
    value = record
    #print(key)
    #print(value)
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #print(key)
    #print(list_of_values)
    for i in range(0,len(list_of_values)):
        for j in range(0,len(list_of_values)):
            if i!=j:
	        rec1=list_of_values[i]
		rec2=list_of_values[j]
		#print(rec1[0])
		#print(rec2[0])
		#print("")
		if(rec1[0]!=rec2[0]):
			if(rec1[0]=='order'):
				rec = rec1+rec2
				mr.emit((rec))
			else:
				rec = rec2+rec1
				#mr.emit((key,rec))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
