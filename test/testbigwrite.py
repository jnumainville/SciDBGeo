import numpy as np
import os

def ArrayToBinary(theArray, binaryFilePath, attributeName='value', yOffSet=0):
    """
    Use Numpy tricks to write a numpy array in binary format with indices 

    input: Numpy 2D array
    output: Numpy 2D array in binary format
    """
    import numpy as np
    # print("Writing out file: %s" % (binaryFilePath))
    col, row = theArray.shape
    
    with open(binaryFilePath, 'w') as fileout:
        #Oneliner that creates the column index. Pull out [y for y in range(col)] to see how it works
        column_index = np.array(np.repeat([y for y in np.arange(0+yOffSet, col+yOffSet) ], row), dtype=np.dtype('int64'))
        #print(column_index)
        #Oneliner that creates the row index. Pull out the nested loop first: [x for x in range(row)]
        #Then pull the full list comprehension: [[x for x in range(row)] for i in range(col)]
        row_index = np.array(np.concatenate([[x for x in range(row)] for i in range(col)]), dtype=np.dtype('int64'))

        #Oneliner for writing out the file
        #Add this to make it a csv tofile(binaryFilePath), "," and modify the open statement to 'w'
        np.core.records.fromarrays([column_index, row_index, theArray.ravel()], dtype=[('y','int64'),('x','int64'),(attributeName,theArray.dtype)]).ravel().tofile(binaryFilePath, ",") 

    
    del column_index, row_index, theArray


row = range(100)
grid = [row for r in row]
array = np.array(grid)
print(array.shape)
binaryPartitionPath = r"c:\work\test.csv"
if os.path.exists(binaryPartitionPath): os.remove(binaryPartitionPath)

binaryFiles = []
for offset, a in enumerate(np.array_split(array, 10)):
    print(offset, offset * a.shape[0])
    binaryPartitionPath = r"c:\work\test_%s.csv" % (offset)
    if os.path.exists(binaryPartitionPath): os.remove(binaryPartitionPath)
    ArrayToBinary(a, binaryPartitionPath, 'mask', offset * a.shape[0])
    #break
    binaryFiles.append(binaryPartitionPath)

with open(r"c:\work\test.csv", "w") as fo:
 for bFile in binaryFiles:
      with open(bFile,'r') as fi: fo.write(fi.read())
