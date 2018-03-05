import scidb
import numpy as np
sdb = scidb.iquery()

def CreateLoadArray(sdb, tempRastName, attribute_name, rasterValueDataType):
    """
    Create the loading 1D array
    """
    try: 
        sdb.query("create array %s <y1:int64, x1:int64, %s:%s> [xy=0:*,?,?]" % (tempRastName, attribute_name, rasterValueDataType) )
    except:
        #Silently deleting temp arrays
        sdb.query("remove(%s)" % (tempRastName))
        sdb.query("create array %s <y1:int64, x1:int64, %s:%s> [xy=0:*,?,?]" % (tempRastName, attribute_name,rasterValueDataType) ) 

def LoadOneDimensionalArray(sdb, sdb_instance, tempRastName, rasterValueDataType, binaryLoadPath):
    """
    Function for loading GDAL data into a single dimension
    """
    try:
        query = "load(%s, '%s' ,%s, '(int64, int64, %s)') " % (tempRastName, binaryLoadPath, sdb_instance, rasterValueDataType)
        sdb.query(query)
        return 1
    except:
        print("Error Loading DimensionalArray")
        print(query)
        return 0

def ArrayDimension(anyArray):
    """
    Return the number of rows and columns for 2D or 3D array
    """
    if anyArray.ndim == 2:
        return anyArray.shape
    else:
        return anyArray.shape[1:]

def WriteArray(theArray, csvPath, attributeName='value', bandID=0):
    """
    This function uses numpy tricks to write a numpy array in binary format with indices 
    """

    col, row = ArrayDimension(theArray)
    print(theArray)
    with open(csvPath, 'wb') as fileout:

        #Oneliner that creates the column index. Pull out [y for y in range(col)] to see how it works
        column_index = np.array(np.repeat([y for y in range(col)], row), dtype=np.dtype('int64'))
        
        #Oneliner that creates the row index. Pull out the nested loop first: [x for x in range(row)]
        #Then pull the full list comprehension: [[x for x in range(row)] for i in range(col)]
        row_index = np.array(np.concatenate([[x for x in range(row)] for i in range(col)]), dtype=np.dtype('int64'))
        
        if bandID >=1:
            #Generate the bandID
            band_index = np.array([bandID for z in column_index])
            
            fileout.write( np.core.records.fromarrays([band_index, column_index, row_index, theArray.ravel()], \
                dtype=[('band','int64'),('x','int64'),('y','int64'),(attributeName.split(":")[0],theArray.dtype)]).ravel().tobytes() )

        elif len(attributeName.split(",")) > 1:
            #Making a list of attributes
            attributesList = [('x','int64'),('y','int64')]
            for name in attributeName.split(","):
                attName, attType = name.split(":")
                attributesList.append( (attName.strip(), attType.strip()) )
            
            #Making a list of numpy arrays. Splitting the NDimensional array by number of attributes
            arrayList = [column_index, row_index]
            for attArray in np.split(theArray, len(attributeName.split(",")), axis=0):
                arrayList.append(attArray.ravel())

            #z = [i.ravel() for i in np.split(c, 2, axis=0)]
            fileout.write( np.core.records.fromarrays(arrayList, dtype=attributesList ).ravel().tobytes() )
        else:
            fileout.write( np.core.records.fromarrays([column_index, row_index, theArray.ravel()], dtype=[('x','int64'),('y','int64'),(attributeName,theArray.dtype)]).ravel().tobytes() )

    del column_index, row_index, theArray

def RedimensionAndInsertArray(sdb, tempArray, SciDBArray, xOffSet=0, yOffSet=0):
    """
    Function for redimension and inserting data from the temporary array into the destination array
    """
    try:
        #sdb.query("insert(redimension(apply( {A}, y, y1+{yOffSet}, x, x1+{xOffSet} ), {B} ), {B})",A=tempRastName, B=rasterArrayName, yOffSet=RasterMetadata[k]["yOffSet"], xOffSet=RasterMetadata[k]["xOffSet"])    
        query = "insert(redimension(apply( %s, y, y1+%s, x, x1+%s ), %s ), %s)" % (tempArray, yOffSet, xOffSet, SciDBArray, SciDBArray)
        sdb.query(query)
    except:
        print("Failing on inserting data into array")
        print(query)

#######################


valuesRegion = np.array( [  [1,1,4,4,4], 
                            [1,1,4,4,4],
                            [6,6,4,4,4],
                            [6,6,6,6,6],
                            [6,6,6,6,6] ], dtype=np.dtype('int8'))

valuesArray = np.array( [   [2,2,2,2,2], 
                            [3,2,3,1,1],
                            [8,3,1,3,3],
                            [8,3,3,2,2],
                            [8,1,2,4,5] ], dtype=np.dtype('int8'))

SciDBArray = 'reclass_values'
temprastName = 'reclass_load2'
attributeName = 'value'
rasterValueDataType = 'int8'
csvPath = '/mnt/reclass_values.sdb'
binaryLoadPath = '/data/04489/dhaynes/reclass_values.sdb'

CreateLoadArray(sdb, temprastName , attributeName, rasterValueDataType)
WriteArray(valuesArray, csvPath, attributeName)
LoadOneDimensionalArray(sdb, '-2', temprastName, rasterValueDataType, binaryLoadPath)
RedimensionAndInsertArray(sdb, temprastName, SciDBArray)
