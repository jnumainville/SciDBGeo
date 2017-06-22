import numpy as np
from collections import defaultdict
from itertools import groupby, cycle, product
from scidbpy import connect
import os, argparse


def LoadOverlapArray():
    pass
    #This code is for inputing a multidimensional array with overlaps
    #if version_num == 0:
        #tempArrayName = 'rasterload'
        #Create new 1D array with new attribute value
        #sdb.query("create array %s <x:int64, y:int64, value:%s> [xy=0:%s,?,0]" % (tempArrayName, rasterValueDataType, aWidth*aHeight-1) )
        #sdb.query("store(redimension(apply({A}, x, x1, y, y1), {B}), {B})", A=tempRastName, B=tempArrayName)

        #Statement for creating Final Raster 
        #sdb.query("create array %s <value:%s> [y=0:%s,?,0; x=0:%s,?,0] using %s " %  (rasterArrayName, rasterValueDataType, width-1, height-1, tempArrayName))
        #sdb.query("remove(rasterload)")

def CreateDestinationArray(sdb, rasterArrayName, attribute, rasterValueDataType, height, width, chunk):
    """
    Create the destination array
    """
    try:           
        sdb.query("create array %s <%s:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, attribute, rasterValueDataType, height-1, chunk, width-1, chunk) )
    except:
        print("Array %s already exists. Removing" % (rasterArrayName))
        sdb.query("remove(%s)" % (rasterArrayName))
        sdb.query("create array %s <%s:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, attribute, rasterValueDataType, height-1, chunk, width-1, chunk) )

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

def ArrayMetadata(width, height, chunk, tiles):
    """
    This function gathers all the metadata necessary
    The loops are 
    """
    import math
    from collections import OrderedDict
    RasterReads = OrderedDict()
    rowMax = 0
    
    for y_version, yOffSet in enumerate(range(0, height, chunk)):
        rowsRemaining = height - y_version*chunk

        #If this is not a short read, then read the correct size.
        if rowsRemaining > chunk*tiles: rowsRemaining = chunk
        
        for x_version, xOffSet in enumerate(range(0, width, chunk*tiles)):
            version_num = rowMax+x_version
            columnsRemaining = width - x_version*chunk*tiles
            
            #If this is not a short read, then read the correct size.
            if columnsRemaining > chunk*tiles : columnsRemaining = chunk*tiles

            #print(rowsRemaining, columnsRemaining, version_num, x_version,y_version,)
            RasterReads[str(version_num)] = OrderedDict([ ("xOffSet",xOffSet), ("yOffSet",yOffSet), ("xWindow", columnsRemaining), ("yWindow", rowsRemaining) ])
        
        rowMax += math.ceil(width/(chunk*tiles))

    return RasterReads


def ReadGDALFile(sdb, rasterArrayName, rasterPath, tempOutDirectory, tempSciDBLoad, attribute="value", chunk=1000, tiles=1,overlap=0):
    from osgeo import gdal
    from gdalconst import GA_ReadOnly
    import timeit
    import math

    raster = gdal.Open(rasterPath, GA_ReadOnly)
    width = raster.RasterXSize 
    height  = raster.RasterYSize
    RasterMetadata = ArrayMetadata(width, height, chunk, tiles)
    print("Width: %s, Height: %s, TotalPixels: %s" % (width, height, width*height))
    #yWindow = xWindow = chunk
    # for version_num, y in enumerate(range(0, height,yWindow)):
    #     rowsRemaining = height - version_num*yWindow

    #NumberOfIterations = math.ceil(width/chunk) * math.ceil(height/chunk)
    #rowMax = 0
    
    # for y_version, y in enumerate(range(0, height, yWindow)):
    #     rowsRemaining = height - y_version*yWindow
        
    #     for x_version, x in enumerate(range(0, width, xWindow)):
    #         columnsRemaining = width - x_version*xWindow
            
    #         scidbVersion = rowMax+x_version
            
#        
    for scidbVersion, k in enumerate(RasterMetadata.keys()):
        totalstart = timeit.default_timer()
        tempRastName = 'temprast_%s' % (scidbVersion)
        csvPath = '%s/%s.sdbbin' % (tempOutDirectory,tempRastName)
        csvPath2 = '%s/%s_2.sdbbin' % (tempOutDirectory,tempRastName)
        rArray = raster.ReadAsArray(xoff=RasterMetadata[k]["xOffSet"], yoff=RasterMetadata[k]["yOffSet"], xsize=RasterMetadata[k]["xWindow"], ysize=RasterMetadata[k]["yWindow"])
        
             
    #         #Series of optional read statements.. 1 is a full read, 2 is a ragged x read, 3 is a ragged y read, 4 is a ragged x & t read
    #         if rowsRemaining >= yWindow and columnsRemaining >= xWindow:
    #             rArray = raster.ReadAsArray(xoff=x, yoff=y, xsize=xWindow, ysize=yWindow)
    #         elif rowsRemaining >= yWindow and columnsRemaining < xWindow:
    #             rArray = raster.ReadAsArray(xoff=x, yoff=y, xsize=columnsRemaining, ysize=yWindow)
    #         elif rowsRemaining < yWindow and columnsRemaining >= xWindow:
    #             rArray = raster.ReadAsArray(xoff=x, yoff=y, xsize=xWindow, ysize=rowsRemaining)
    #         elif rowsRemaining < yWindow and columnsRemaining < xWindow:
    #             rArray = raster.ReadAsArray(xoff=x, yoff=y, xsize=columnsRemaining, ysize=rowsRemaining)

            #print(rowsRemaining, columnsRemaining, scidbVersion, rowMax+x_version, x_version,y_version,)
            
        # #Start timing

        # #If then statement to account for final short read
        # # if rowsRemaining >= yWindow:    
        # #     rArray = raster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=yWindow)
        # # else:
        # #     rArray = raster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=rowsRemaining)

        #Create final destination array
        rasterValueDataType = rArray.dtype
        if scidbVersion == 0: CreateDestinationArray(sdb, rasterArrayName, attribute, rasterValueDataType, height, width, chunk)     

        #Write the Array to Binary file, data is written out Column/Y, Row/X, Value
        # start = timeit.default_timer()      
        # aWidth, aHeight = WriteMultiDimensionalArray(rArray, csvPath2)
        # os.chmod(csvPath2, 0o755)
        # stop = timeit.default_timer()
        # writeBinaryTime = stop-start
        
        start = timeit.default_timer()
        WriteArray(rArray, csvPath)
        stop = timeit.default_timer()
        writeBinaryTime = stop-start
        os.chmod(csvPath, 0o755)

        #print("Time numpy iterator: %s, Time for Numpy tricks: %s" % (writeBinaryTime, writeDataTime))
        #Create the array, which will hold the read in data. Y/Column and X/Row coordinates are different on purpose
        CreateLoadArray(sdb, tempRastName, attribute, rasterValueDataType)

        #Time the loading of binary file
        start = timeit.default_timer()
        binaryLoadPath = '%s/%s.sdbbin' % (tempSciDBLoad,tempRastName )
        #sdb.query("load(%s,'%s', -2, '(int64, int64, int64)' )" % (tempRastName, binaryLoadPath))
        sdb.query("load(%s,'%s', -2, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, rasterValueDataType))
        stop = timeit.default_timer() 
        loadBinaryTime = stop-start

        #Time the redimension
        start = timeit.default_timer()
        sdb.query("insert(redimension(apply( {A}, y, y1+{yOffSet}, x, x1+{xOffSet} ), {B} ), {B})",A=tempRastName, B=rasterArrayName, yOffSet=RasterMetadata[k]["yOffSet"], xOffSet=RasterMetadata[k]["xOffSet"])
        stop = timeit.default_timer() 
        redimensionArrayTime = stop-start

        #Clean up the temporary files
        if scidbVersion >= 1: CleanUpTemp(sdb, rasterArrayName, scidbVersion, csvPath, tempRastName)
        
        totalstop = timeit.default_timer()    
        

        print('Completed %s of %s' % (scidbVersion+1, len(RasterMetadata)) )

        if scidbVersion == 0:
            NumberOfIterations = len(RasterMetadata)
            totalTime = totalstop - totalstart
            print('Took %s seconds to complete' % (totalTime))
            print("Writing time: %s, Loading time: %s, Redimension time: %s " % (writeBinaryTime, loadBinaryTime, redimensionArrayTime) )
            print('Estimated time to load (%s) = time %s * loop %s' % ( totalTime*NumberOfIterations,  totalTime, NumberOfIterations) )
            print('Estimated time in hours: %s ' % ( totalTime*NumberOfIterations/60/60) )
            CleanUpTemp(sdb, rasterArrayName, scidbVersion, csvPath, tempRastName)
# #        if scidbVersion >= 300 : break
#         rowMax += x_version+1

def WriteArray(theArray, csvPath):
    """

    """
    col, row = theArray.shape
    with open(csvPath, 'wb') as fileout:

        thecolumns =[y for y in range(col)]
        column_index = np.array(np.repeat(thecolumns, row), dtype=np.dtype('int64'))
        
        therows = [x for x in range(row)]
        allrows = [therows for i in range(col)]
        row_index = np.array(np.concatenate(allrows), dtype=np.dtype('int64'))

        values = theArray.ravel()
        vdatatype = theArray.dtype
    
        dataset = np.core.records.fromarrays([column_index, row_index, values], names='y,x,value', dtype='int64, int64, uint8')
        #dataset = np.array(np.vstack((column_index, rows_index, values))
        #print(dataset.dtype)
        fileout.write(dataset.ravel().tobytes())
        #fileout.write(dataset.ravel(order='F').tostring())



    return 

def WriteMultiDimensionalArray(rArray, csvPath ):
    '''This function write the multidimensional array as a binary '''
    with open(csvPath, 'wb') as fileout:
        arrayHeight, arrayWidth = rArray.shape
        it = np.nditer(rArray, flags=['multi_index'], op_flags=['readonly'])
        byteValues = []
        for counter, pixel in enumerate(it):
            col, row = it.multi_index
            #if counter < 100: print("column: %s, row: %s" % (col, row))

            indexvalue = np.array([col,row], dtype=np.dtype('int64'))
            byteValues.append(indexvalue.tobytes())
            byteValues.append(it.value.tobytes())
            #fileout.write( indexvalue.tobytes() )
            #fileout.write( it.value.tobytes() )
        bytesTile = b"".join(byteValues)
        fileout.write(bytesTile)
    return(arrayHeight, arrayWidth)
    
def CleanUpTemp(sdb, rasterArrayName, version_num, csvPath, tempRastName):
    'Remove all temporary files'
    if version_num > 0:
       sdb.query("remove_versions(%s, %s)" % (rasterArrayName, version_num))
    sdb.query("remove(%s)" % (tempRastName))
    os.remove(csvPath)

def argument_parser():
    parser = argparse.ArgumentParser(description="Load GDAL dataset into SciDB")   
    parser.add_argument('-SciDBArray', required=True, dest='SciArray')
    parser.add_argument('-RasterPath', required=True, dest='Raster')
    parser.add_argument('-Host', required=False, default=None, dest='Host')
    parser.add_argument('-Chunksize', required=False, dest='Chunk', type=int, default=1000)
    parser.add_argument('-Overlap', required=False, dest='Overlap', type=int, default=0)
    parser.add_argument('-Tiles', required=False, dest='Tiles', type=int, default=5)
    parser.add_argument('-att_name', required=False, dest='Attributes', default="value")
    parser.add_argument('-tempOut', required=False, dest='OutPath', default='/home/scidb/scidb_data/0/0')
    parser.add_argument('-SciDBLoad', required=False, dest='SciDBLoadPath', default='/home/scidb/scidb_data/0/0')
    
    return parser

# def GDALtoSciDB():
#     chunkSize = 100000
#     chunkOverlap = 0
#     yWindow = 100
#     rasterArrayName = 'MERIS_2010'
#     rasterPath = '/home/04489/dhaynes/data/ESACCI_300m_2010.tif'
#     sdb = connect('http://iuwrang-xfer2.uits.indiana.edu:8080')
    

if __name__ == '__main__':
    args = argument_parser().parse_args()
    if os.path.exists(args.Raster):
        #if args.Host = 'localhost'
        sdb = connect(args.Host)
        #tempFileOutPath = '/mnt'
        #tempFileSciDBLoadPath = '/data/04489/dhaynes'
        if sdb:
            #tempFileSciDBLoadPath = tempFileOutPath = '/home/scidb/scidb_data/0/0'
            ReadGDALFile(sdb, args.SciArray, args.Raster, args.OutPath, args.SciDBLoadPath, args.Attributes, args.Chunk, args.Tiles, args.Overlap)
        else:
            print('Not Valid connection: %s' % (args.Host))
    else:
        print("Not a valid Raster Path")


