import multiprocessing as mp
import itertools
import numpy as np

class RasterReader(object):

    def __init__(self, RasterPath, scidbHost, scidbArray, attribute, chunksize, tiles):
        """
        Initializethe class RasterReader
        """
        self.width, self.height, self.datatype = self.GetRasterDimensions(RasterPath)
        self.RasterMetadata = self.CreateArrayMetadata(scidbArray, self.width, self.height, chunksize, tiles, attribute)
        
        #self.CreateDestinationArray(scidbHost, scidbArray, attribute, self.datatype, self.height, self.width, chunksize)


    def GetMetadata(self, scidbInstances,rasterFilePath, outPath, loadPath, host):
        """
        Iterator for the class
        Input: 
        scidbInstance = SciDB Instance IDs
        rasterFilePath = Absolute Path to the GeoTiff
        """
        for key, process, filepath, outDirectory, loadDirectory, host in zip(self.RasterMetadata.keys(), itertools.cycle(scidbInstances), itertools.repeat(rasterFilePath), itertools.repeat(outPath), itertools.repeat(loadPath), itertools.repeat(host)):
            yield self.RasterMetadata[key], process, filepath, outDirectory, loadDirectory, host
        
    def GenerateAttributesInfo(self, attribute):
        """
        still working on this, 
        I want get the attributes len and compare it with value types then make a tuple
        """
        if len(attribute) > 1: pass

    
    def GetRasterDimensions(self, thePath):
        """
        Function gets the dimensions of the raster file 
        """
        from osgeo import gdal
        from gdalconst import GA_ReadOnly

        raster = gdal.Open(thePath, GA_ReadOnly)
        width = raster.RasterXSize 
        height  = raster.RasterYSize
        rArray = raster.ReadAsArray(xoff=0, yoff=0, xsize=1, ysize=1)
        rasterValueDataType= rArray.dtype

        del raster

        return width, height, rasterValueDataType

    def CreateDestinationArray(self, theURL, rasterArrayName, attribute, rasterValueDataType, height, width, chunk):
        """
        Function creates the final destination array
        """
        from scidbpy import connect
        sdb = connect(theURL)
        
        try:           
            sdb.query("create array %s <%s:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, attribute, rasterValueDataType, height-1, chunk, width-1, chunk) )
        except:
            print("Array %s already exists. Removing" % (rasterArrayName))
            sdb.query("remove(%s)" % (rasterArrayName))
            sdb.query("create array %s <%s:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, attribute, rasterValueDataType, height-1, chunk, width-1, chunk) )

        del sdb 


    def CreateArrayMetadata(self, theArray, width, height, chunk=1000, tiles=1, attribute='value'):
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
                RasterReads[str(version_num)] = OrderedDict([ ("xOffSet",xOffSet), ("yOffSet",yOffSet), \
                    ("xWindow", columnsRemaining), ("yWindow", rowsRemaining), ("version", version_num), ("attribute", attribute), ("scidbArray", theArray)])
            
            rowMax += math.ceil(width/(chunk*tiles))

        return RasterReads



def GDALReader(inParams):
    """

    """
    from scidbpy import connect
    
    
    import os
    from scidbpy import connect
    from osgeo import gdal
    from gdalconst import GA_ReadOnly
    
    sdb = connect('http://iuwrang-xfer2.uits.indiana.edu:8080')
    
    print(inParams)
    theMetadata = inParams[0]
    theInstance = inParams[1]
    theRasterPath = inParams[2]
    theSciDBOutPath = inParams[3]
    theSciDBLoadPath = inParams[4]


    #print(theMetadata, theInstance, thePath)
    
    # # yWindow, base, width, datatype, n = inParams[2]
    # # #print("multiprocessing")

    # # current = mp.current_process()
    # # SciDBInstance = AssignSciDBInstance(current._identity[0], numProcesses )

    raster = gdal.Open(theRasterPath, GA_ReadOnly)
    array = raster.ReadAsArray(xoff=theMetadata['xOffSet'], yoff=theMetadata['yOffSet'], xsize=theMetadata['xWindow'], ysize=theMetadata['yWindow'])
    rasterValueDataType = array.dtype

    tempArray = "temprast_%s" % (theMetadata['version'])
    rasterBinaryFilePath = "%s/%s.sdbbin" % (theSciDBOutPath, tempArray)
    rasterBinaryLoadPath = "%s/%s.sdbbin" % (theSciDBLoadPath, tempArray)
    
    #print(current._identity[0], SciDBInstance, rArray.shape, rasterBinaryFilePath)

    WriteMultiDimensionalArray(array, rasterBinaryFilePath)
    CreateLoadArray(sdb, tempArray, theMetadata['attribute'], rasterValueDataType)
    if LoadOneDimensionalArray(sdb, theInstance, tempArray, rasterValueDataType, rasterBinaryLoadPath):
        RedimensionAndInsertArray(sdb, tempArray, theMetadata['scidbArray'], theMetadata['xOffSet'], theMetadata['yOffSet'])    
    
    RemoveTempArray(sdb, tempArray)

    del raster

def AssignSciDBInstance(aProcess,theProcesses):
    """
    This might need to be deleted, unless people hae multiple instances they don't want to use
    """
    scidbInstances = [i+1 for i in range(theProcesses)]

    instance = scidbInstances[scidbInstances.index(aProcess)]
    #print(instance)

    return instance


# def RasterPrep(rasterPath, readWindow, sdbHost, destArrayName ):
#     """
#     This function does an initial read on the raster file and creates the necessary metadata
#     """
#     from osgeo import gdal
#     from gdalconst import GA_ReadOnly
#     from collections import OrderedDict
    
#     raster = gdal.Open(rasterPath, GA_ReadOnly)
#     width = raster.RasterXSize 
#     height  = raster.RasterYSize
#     rArray = raster.ReadAsArray(xoff=0, yoff=0, xsize=width, ysize=1)
#     rasterValueDataType = rArray.dtype



#     NumberOfIterations = int(round( height/float(readWindow) +.5))

#     rasterMetadata = OrderedDict( [(str(n), {"ReadWindow": height - n*readWindow, "Base": n*readWindow, "Width": width, "DataType": rasterValueDataType }) for n in range(NumberOfIterations)] )

#     #Setting the readwindow to the default, except for the last short read
#     for r in rasterMetadata:
#         if rasterMetadata[r]["ReadWindow"] > readWindow: rasterMetadata[r]["ReadWindow"] = readWindow

#     #rasterMetadata = orderedDict(  [(str(n): "ReadWindow": height - version_num*yWindow)]) 
#     del raster
#     return(rasterMetadata)


    #python GDALtoSciDB_multiprocessing.py --processes 2 --raster /home/david/data/glc2000.tif --dest glc2000 --window 100 --chunk 10000 
    #chunkSize = 100000
    #chunkOverlap = 0
    #yWindow = 100
    #rasterArrayName = 'glc2000'
    #rasterPath = '/home/04489/dhaynes/glc2000.tif'
    #tempFilePath = '/home/04489/dhaynes'

# def CreateDestinationArray(sdb, width, height, rasterArrayName, rasterValueDataType):
#     """

#     """
#     try:
#         #Create final destination array           
#         sdb.query("create array %s <value:%s> [y=0:%s,?,0; x=0:%s,?,0]" %  (rasterArrayName, rasterValueDataType, width-1, height-1) )
#     except:
#         print("Found existing array with same name removing...")
#         sdb.query("remove(%s)" % (rasterArrayName))
#         sdb.query("create array %s <value:%s> [y=0:%s,?,0; x=0:%s,?,0]" %  (rasterArrayName, rasterValueDataType, width-1, height-1) )



def WriteMultiDimensionalArray(rArray, binaryFilePath):
    """
    This function write the multidimensional array as a binary file to be loaded into SciDB

    """

    with open(binaryFilePath, 'wb') as fileout:
        arrayHeight, arrayWidth = rArray.shape
        it = np.nditer(rArray, flags=['multi_index'], op_flags=['readonly'])
        byteValues = []
        for counter, pixel in enumerate(it):
            col, row = it.multi_index

            indexvalue = np.array([col,row], dtype=np.dtype('int64'))
            byteValues.append(indexvalue.tobytes())
            byteValues.append(it.value.tobytes())

        bytesTile = b"".join(byteValues)
        fileout.write(bytesTile)

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
    except:
        print("Error Loading DimensionalArray")
        print(query)

def RemoveTempArray(sdb, tempRastName):
    sdb.query("remove(%s)" % (tempRastName))

def RedimensionAndInsertArray(sdb, tempArray, scidbArray, xOffSet, yOffSet):
    """
    Function for redimension and inserting data from the temporary array into the destination array
    """
    try:
        #sdb.query("insert(redimension(apply( {A}, y, y1+{yOffSet}, x, x1+{xOffSet} ), {B} ), {B})",A=tempRastName, B=rasterArrayName, yOffSet=RasterMetadata[k]["yOffSet"], xOffSet=RasterMetadata[k]["xOffSet"])    
        query = "insert(redimension(apply( %s, y, y1+%s, x, x1+%s ), %s ), %s" % (tempArray, yOffSet, xOffSet, ScidbArray, scidbArray)
        sdb.query(query)
    except:
        print("Failing on inserting data into array")

#         start = timeit.default_timer()
#         sdb.query("create array %s <x1:int64, y1:int64, value:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType) )

#         sdb.query("create array %s <x1:int64, y1:int64, value:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType) )

        
# #         #Time the loading of binary file
# #         start = timeit.default_timer()
# #         binaryLoadPath = '%s/%s.sdbbin' % (tempSciDBLoad,tempRastName )
#         sdb.query("load(%s,'%s', %s, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, sdb_instance, rasterValueDataType))
#         stop = timeit.default_timer()
#         print(stop-start)


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

def main(pyVersion, SciDBHost, Rasters, SciDBInstances, rasterFilePath, SciDBOutPath, SciDBLoadPath):
    """
    This function creates the pool based upon the number of SciDB instances and the generates the parameters for each Python instance
    """
    pool = mp.Pool(len(SciDBInstances))

    if pyVersion[0] > 2:

        pool.map_async(GDALReader, (r for r in Rasters.GetMetadata(scidbInstances, rasterFilePath, SciDBOutPath, SciDBLoadPath, SciDBHost)  ))
        #pool.map_async(GDALReader, zip(itertools.repeat(rasterFilePath), itertools.repeat(numProcesses), 
        #( (arrayReadSettings[r]["ReadWindow"], arrayReadSettings[r]["Base"], arrayReadSettings[r]["Width"], arrayReadSettings[r]["DataType"], r) for r in arrayReadSettings)   )  )

    else:
        pool.map_async(GDALReader, itertools.izip(itertools.repeat(rasterFilePath), itertools.repeat(numProcesses),  ( (arrayReadSettings[r]["ReadWindow"], arrayReadSettings[r]["Base"], arrayReadSettings[r]["Width"], arrayReadSettings[r]["DataType"], r) for r in arrayReadSettings)   )  )

    pool.close()
    pool.join()


def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "multiprocessing module for loading GDAL read data into SciDB with multiple instances")    
    parser.add_argument("-Instances", required =True, nargs='*', type=int, help="Number of SciDB Instances for parallel data loading", dest="instances")    
    parser.add_argument("-Host", required =True, help="SciDB host for connection", dest="host", default="localhost")    
    parser.add_argument("-RasterPath", required =True, help="Input file path for the raster", dest="rasterPath")    
    parser.add_argument("-ScidbArray", required =True, help="Name of the destination array", dest="rasterName")
    parser.add_argument("-AttributeNames", required =True, help="Name of the destination array", dest="attributes", default="value")
    parser.add_argument("-Tiles", required =False, type=int, help="Size in rows of the read window, default: 1", dest="tiles", default=1)
    parser.add_argument("-Chunk", required =False, type=int, help="Chunk size for the destination array, default: 1,000", dest="chunk", default=1000)
    parser.add_argument("-Overlap", required =False, type=int, help="Chunk overlap size. Adding overlap increases data loading time. defalt: 0", dest="overlap", default=0)
    parser.add_argument('-TempOut', required=False, default='/home/scidb/scidb_data/0/0', dest='OutPath',)
    parser.add_argument('-SciDBLoadPath', required=False, default='/home/scidb/scidb_data/0/0', dest='SciDBLoadPath')
    
    return parser


if __name__ == '__main__':
    import sys
    pythonVersion = sys.version_info

    args = argument_parser().parse_args()
    RasterInformation = RasterReader(args.rasterPath, args.host, args.rasterName, args.attributes, args.chunk, args.tiles)
    main(pythonVersion, RasterInformation, args.host, args.instances, args.rasterPath, args.OutPath, args.SciDBLoadPath)
    
    # for r in RasterInformation.GetMetadata(args.instances, args.rasterPath):
    #     print(r)
    

    #RasterPath, SciDBHost, SciDBArray, attribute, chunksize, tiles
    #arrayReadSettings = RasterPrep(args.rasterPath, int(args.window), args.host, args.rasterName)
    #main(pythonVersion, int(args.processes), args.rasterPath)

    
