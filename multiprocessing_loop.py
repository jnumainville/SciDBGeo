import multiprocessing as mp
import itertools
import numpy as np


def GDALReader(inParams ):
    """

    """

    sdb = connect('http://iuwrang-xfer2.uits.indiana.edu:8080')
    import os
    from osgeo import gdal
    from gdalconst import GA_ReadOnly
    
    rasterPath = inParams[0]
    numProcesses = inParams[1]
    yWindow, base, width, datatype, n = inParams[2]

    current = mp.current_process()
    SciDBInstance = AssignSciDBInstance(current._identity[0], numProcesses )

    raster = gdal.Open(rasterPath, GA_ReadOnly)
    rArray = raster.ReadAsArray(xoff=0, yoff=base, xsize=width, ysize=yWindow)

    rasterBinaryFileName = "temprast_%s" % (n)
    
    basePath = "/".join(rasterPath.split("/")[:-1])
    rasterBinaryFilePath = "%s/%s.sdbbin" % (basePath, rasterBinaryFileName)
    
    print(current._identity[0], SciDBInstance, rArray.shape, rasterBinaryFilePath)

    WriteBinaryMultiDimensionalArray(rArray, rasterBinaryFilePath)
    os.chmod(rasterBinaryFilePath, 0o755)

    LoadOneDimensionalArray(sdb, SciDBInstance, tempRastName, rasterValueDataType, rasterBinaryFilePath)    
    

def AssignSciDBInstance(aProcess,theProcesses):
    """
    This might need to be deleted, unless people hae multiple instances they don't want to use
    """
    scidbInstances = [i+1 for i in range(theProcesses)]

    instance = scidbInstances[scidbInstances.index(aProcess)]

    return instance


def RasterPrep(rasterPath, readWindow, ):
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

    NumberOfIterations = int(round( height/float(readWindow) +.5))

    rasterMetadata = OrderedDict( [(str(n), {"ReadWindow": height - n*readWindow, "Base": n*readWindow, "Width": width, "DataType": rasterValueDataType }) for n in range(NumberOfIterations)] )

    #Setting the readwindow to the default, except for the last short read
    for r in rasterMetadata:
        if rasterMetadata[r]["ReadWindow"] >= readWindow:
            rasterMetadata[r]["ReadWindow"] = 100

    del raster
    return(rasterMetadata)

def PrepareMultiDimensionalArray(sdb, width, height, rasterArrayName, rasterValueDataType, chunkSize):
    """
    Create the multidimensional destination array
    """

    #Create final destination array           
    sdb.query("create array %s <value:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, rasterValueDataType, width-1, height-1, chunkSize) )
    pass


def WriteBinaryMultiDimensionalArray(rArray, binaryFilePath):
    """
    This function write the multidimensional array as a binary file to be loaded into SciDB

    """

    with open(binaryFilePath, 'wb') as fileout:
        it = np.nditer(rArray, flags=['multi_index'], op_flags=['readonly'])
        for counter, pixel in enumerate(it):
            row, col = it.multi_index

            indexvalue = np.array([row,col], dtype=np.dtype('int64'))

            fileout.write( indexvalue.tobytes() )
            fileout.write( it.value.tobytes() )

def LoadOneDimensionalArray(sdb, sdb_instance, tempRastName, rasterValueDataType, binaryLoadPath)
    try:
        start = timeit.default_timer()
        sdb.query("create array %s <x1:int64, y1:int64, value:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType) )

        sdb.query("create array %s <x1:int64, y1:int64, value:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType) )
        
#       #Time the loading of binary file
        sdb.query("load(%s,'%s', %s, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, sdb_instance, rasterValueDataType))
        stop = timeit.default_timer()
        print(stop-start)
    except:
        print("Error Loading DimensionalArray")

def work(theValue):
    theMin, theMax = theValue
    for i in range(theMin, theMax):
        print i


if __name__ == '__main__':
    processes = 2
    pool = mp.Pool(processes)
    rasterFilePath = r'/home/david/data/glc2000.tif'
    readWindow = 100
    arrayReadSettings = RasterPrep(rasterFilePath, readWindow, )
    
    rasterFileList = [rasterFilePath for r in  range(len(arrayReadSettings)) ]

    pool.map_async(GDALReader, itertools.izip(itertools.repeat(rasterFilePath), itertools.repeat(processes),
                                              ((arrayReadSettings[r]["ReadWindow"], arrayReadSettings[r]["Base"],
                                                arrayReadSettings[r]["Width"], arrayReadSettings[r]["DataType"], r) for
                                               r in arrayReadSettings)))

    pool.close()
    pool.join()

