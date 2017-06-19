import multiprocessing as mp
import itertools
import numpy as np


def GDALReader(inParams ):
    """

    """

    #sdb = connect('http://iuwrang-xfer2.uits.indiana.edu:8080')
    import os
    from scidbpy import connect
    from osgeo import gdal
    from gdalconst import GA_ReadOnly
    
    #print(type(inParams), len(inParams))
    rasterPath = inParams[0]
    numProcesses = inParams[1]
    yWindow, base, width, datatype, n = inParams[2]
    #print("multiprocessing")

    current = mp.current_process()
    SciDBInstance = AssignSciDBInstance(current._identity[0], numProcesses )

    raster = gdal.Open(rasterPath, GA_ReadOnly)
    rArray = raster.ReadAsArray(xoff=0, yoff=base, xsize=width, ysize=yWindow)

    rasterBinaryFileName = "temprast_%s" % (n)
    
    #print(rArray.shape, os.getpid())
    basePath = "/".join(rasterPath.split("/")[:-1])
    rasterBinaryFilePath = "%s/%s.sdbbin" % (basePath, rasterBinaryFileName)
    
    print(current._identity[0], SciDBInstance, rArray.shape, rasterBinaryFilePath)

    #WriteMultiDimensionalArray(rArray, rasterBinaryFilePath)
    #LoadOneDimensionalArray(sdb, SciDBInstance, tempRastName, rasterValueDataType, rasterBinaryFilePath)    

def AssignSciDBInstance(aProcess,theProcesses):
    """
    This might need to be deleted, unless people hae multiple instances they don't want to use
    """
    scidbInstances = [i+1 for i in range(theProcesses)]

    instance = scidbInstances[scidbInstances.index(aProcess)]
    #print(instance)

    return instance


def RasterPrep(rasterPath, readWindow, sdbHost, destArrayName ):
    """
    This function does an initial read on the raster file and creates the necessary metadata
    """
    from osgeo import gdal
    from gdalconst import GA_ReadOnly
    from collections import OrderedDict
    
    raster = gdal.Open(rasterPath, GA_ReadOnly)
    width = raster.RasterXSize 
    height  = raster.RasterYSize
    rArray = raster.ReadAsArray(xoff=0, yoff=0, xsize=width, ysize=1)
    rasterValueDataType = rArray.dtype

    from scidbpy import connect
    sdb =connect(sdbHost)
    CreateDestinationArray(sdb, width, height, destArrayName, rasterValueDataType)
    del sdb 

    NumberOfIterations = int(round( height/float(readWindow) +.5))

    rasterMetadata = OrderedDict( [(str(n), {"ReadWindow": height - n*readWindow, "Base": n*readWindow, "Width": width, "DataType": rasterValueDataType }) for n in range(NumberOfIterations)] )

    #Setting the readwindow to the default, except for the last short read
    for r in rasterMetadata:
        if rasterMetadata[r]["ReadWindow"] > readWindow: rasterMetadata[r]["ReadWindow"] = readWindow

    #rasterMetadata = orderedDict(  [(str(n): "ReadWindow": height - version_num*yWindow)]) 
    del raster
    return(rasterMetadata)


def argument_parser():
    """
    return arguments
    """
    import argparse
    parser = argparse.ArgumentParser(description= "multiprocessing module for loading GDAL read data into SciDB with multiple instances")    
    parser.add_argument("--processes", required =True, help="Number of SciDB Instances for parallel data loading", dest="processes")    
    parser.add_argument("--host", required =True, help="SciDB host for connection", dest="host")    
    parser.add_argument("--raster", required =True, help="Input file path for the raster", dest="rasterPath")    
    parser.add_argument("--dest", required =True, help="Name of the destination array", dest="rasterName")
    parser.add_argument("--window", required =False, help="Size in rows of the read window, default: 100", dest="window", default=100)
    parser.add_argument("--chunk", required =False, help="Chunk size for the destination array, default: 100,000", dest="chunk", default=100000)
    parser.add_argument("--overlap", required =False, help="Chunk overlap size. Adding overlap increases data loading time. defalt: 0", dest="overlap", default=0)
    
    return parser

    #python GDALtoSciDB_multiprocessing.py --processes 2 --raster /home/david/data/glc2000.tif --dest glc2000 --window 100 --chunk 10000 
    #chunkSize = 100000
    #chunkOverlap = 0
    #yWindow = 100
    #rasterArrayName = 'glc2000'
    #rasterPath = '/home/04489/dhaynes/glc2000.tif'
    #tempFilePath = '/home/04489/dhaynes'

def CreateDestinationArray(sdb, width, height, rasterArrayName, rasterValueDataType):
    """

    """
    try:
        #Create final destination array           
        sdb.query("create array %s <value:%s> [y=0:%s,?,0; x=0:%s,?,0]" %  (rasterArrayName, rasterValueDataType, width-1, height-1) )
    except:
        print("Found existing array with same name removing...")
        sdb.query("remove(%s)" % (rasterArrayName))
        sdb.query("create array %s <value:%s> [y=0:%s,?,0; x=0:%s,?,0]" %  (rasterArrayName, rasterValueDataType, width-1, height-1) )



def WriteMultiDimensionalArray(rArray, binaryFilePath):
    """
    This function write the multidimensional array as a binary file to be loaded into SciDB

    """

    with open(binaryFilePath, 'wb') as fileout:
        arrayHeight, arrayWidth = rArray.shape
        it = np.nditer(rArray, flags=['multi_index'], op_flags=['readonly'])
        for counter, pixel in enumerate(it):
            row, col = it.multi_index

            indexvalue = np.array([row,col], dtype=np.dtype('int64'))

            fileout.write( indexvalue.tobytes() )
            fileout.write( it.value.tobytes() )

def LoadOneDimensionalArray(sdb, sdb_instance, tempRastName, rasterValueDataType, binaryLoadPath):
    try:
        start = timeit.default_timer()
        sdb.query("create array %s <x1:int64, y1:int64, value:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType) )

        sdb.query("create array %s <x1:int64, y1:int64, value:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType) )
        
#         #Time the loading of binary file
#         start = timeit.default_timer()
#         binaryLoadPath = '%s/%s.sdbbin' % (tempSciDBLoad,tempRastName )
        sdb.query("load(%s,'%s', %s, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, sdb_instance, rasterValueDataType))
        stop = timeit.default_timer()
        print(stop-start)
    except:
        print("Error Loading DimensionalArray")

#def WriteBinaryFile(rasterBinaryFileName):

#     for version_num, y in enumerate(range(0, height,yWindow)):
#         tempRastName = 'temprast_%s' % (version_num)
#         csvPath = '%s/%s.sdbbin' % (tempOutDirectory,rasterBinaryFileName)
#         rowsRemaining = height - version_num*yWindow

#         #Start timing
#         totalstart = timeit.default_timer()
#         #If then statement to account for final short read
#         if rowsRemaining >= yWindow:    
#             rArray = raster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=yWindow)
#         else:
#             rArray = raster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=rowsRemaining)

#         rasterValueDataType = rArray.dtype

#         if version_num == 0:
#             
#             #pass
        
#         #Write the Array to Binary file
#         start = timeit.default_timer()      
#         aWidth, aHeight = WriteMultiDimensionalArray(rArray, csvPath)
#         os.chmod(csvPath, 0o755)
#         stop = timeit.default_timer()
#         writeBinaryTime = stop-start
                    
#         #Create the array, which will hold the read in data. X and Y coordinates are different on purpose 
#         sdb.query("create array %s <x1:int64, y1:int64, value:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType) )
        
#         #Time the loading of binary file
#         start = timeit.default_timer()
#         binaryLoadPath = '%s/%s.sdbbin' % (tempSciDBLoad,tempRastName )
#         sdb.query("load(%s,'%s', -2, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, rasterValueDataType))
#         stop = timeit.default_timer() 
#         loadBinaryTime = stop-start

def main(pyVersion, numProcesses, rasterFilePath):
    """
    This function creates the pool based upon the number of SciDB instances and the generates the parameters for each Python instance
    """
    pool = mp.Pool(numProcesses)

    if pyVersion[0] > 2:
        pool.map_async(GDALReader, zip(itertools.repeat(rasterFilePath), itertools.repeat(numProcesses), 
        ( (arrayReadSettings[r]["ReadWindow"], arrayReadSettings[r]["Base"], arrayReadSettings[r]["Width"], arrayReadSettings[r]["DataType"], r) for r in arrayReadSettings)   )  )

    else:
        pool.map_async(GDALReader, itertools.izip(itertools.repeat(rasterFilePath), itertools.repeat(numProcesses),  ( (arrayReadSettings[r]["ReadWindow"], arrayReadSettings[r]["Base"], arrayReadSettings[r]["Width"], arrayReadSettings[r]["DataType"], r) for r in arrayReadSettings)   )  )

    pool.close()
    pool.join()



if __name__ == '__main__':
    import sys
    pythonVersion = sys.version_info

    args = argument_parser().parse_args()
    arrayReadSettings = RasterPrep(args.rasterPath, int(args.window), args.host, args.rasterName)
    main(pythonVersion, int(args.processes), args.rasterPath)

    