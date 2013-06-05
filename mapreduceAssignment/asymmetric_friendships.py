import MapReduce
import sys

"""
Friend Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    mr.emit_intermediate(1, (key,value))

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts 
    dict={}
    for i in range(0,len(list_of_values)):
	if dict.has_key(list_of_values[i][0]):
		dict[list_of_values[i][0]].append(list_of_values[i][1])
	else:
		dict[list_of_values[i][0]]=[list_of_values[i][1]]
    #print(dict)
    for i in range(0,len(list_of_values)):
	f1 = list_of_values[i][0]
	f2 = list_of_values[i][1]
	if(dict.has_key(f1) and dict.has_key(f2)):
		if((f2 in dict[f1]) and (f1 in dict[f2])):
			continue
		else:
			mr.emit((f1,f2))
			mr.emit((f2,f1))
	else:
		mr.emit((f1,f2))
		mr.emit((f2,f1))
		

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
