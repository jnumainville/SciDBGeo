import multiprocessing as mp
import itertools
import numpy as np


def GDALReader(inParams ):
    """

    """

    #sdb = connect('http://iuwrang-xfer2.uits.indiana.edu:8080')
    import os
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

    WriteMultiDimensionalArray(rArray, rasterBinaryFilePath)

def AssignSciDBInstance(aProcess,theProcesses):
    """
    This might need to be deleted, unless people hae multiple instances they don't want to use
    """
    scidbInstances = [i+1 for i in range(theProcesses)]

    instance = scidbInstances[scidbInstances.index(aProcess)]
    #print(instance)

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

    #rasterMetadata = orderedDict(  [(str(n): "ReadWindow": height - version_num*yWindow)]) 
    del raster
    return(rasterMetadata)


# def argsparse():
#     """

#     """
#     chunkSize = 100000
#     chunkOverlap = 0
#     yWindow = 100
#     rasterArrayName = 'glc2000'
#     rasterPath = '/home/04489/dhaynes/glc2000.tif'
#     tempFilePath = '/home/04489/dhaynes'

def PrepareMultiDimensionalArray(sdb, width, height, rasterArrayName, rasterValueDataType):
    """

    """

    #Create final destination array           
    #sdb.query("create array %s <value:%s> [y=0:%s,?,0; x=0:%s,?,0]" %  (rasterArrayName, rasterValueDataType, width-1, height-1) )
    pass


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
    #GDALReader(rasterPath, width, base, ReadWindow)
    #pool.map_async(GDALReader, (rasterFileList, arrayReadSettings) )

    pool.map_async(GDALReader, itertools.izip(itertools.repeat(rasterFilePath), itertools.repeat(processes),  ( (arrayReadSettings[r]["ReadWindow"], arrayReadSettings[r]["Base"], arrayReadSettings[r]["Width"], arrayReadSettings[r]["DataType"], r) for r in arrayReadSettings)   )  )

    # theValues = [(i,i+10) for i in range(1, 100, 10)]
    #pool.map_async(work, theValues)
    #pool.start()
    pool.close()
    pool.join()

