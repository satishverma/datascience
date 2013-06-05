import MapReduce
import sys

"""
Matrix Multiply in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    #A L*M
    #B M*N
    N=5
    L=5
    M=4
    matrixId = record[0]
    if (matrixId=="a"):
	for k in range(0,N):
		mr.emit_intermediate((record[1],k),(record[0],record[1],record[2],record[3]))
    if (matrixId=="b"):
	for k in range(0,L):
		mr.emit_intermediate((record[2],k),(record[0],record[1],record[2],record[3]))

	 

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    print(key)
    print(list_of_values)
    product=0
	
    mr.emit((key,product))
	
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
