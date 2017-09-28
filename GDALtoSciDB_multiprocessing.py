# from pathos.multiprocessing import ProcessingPool
# import dill, pathos

import multiprocessing as mp
import itertools, timeit
import numpy as np
import math, csv, os, gc, sys
from osgeo import gdal
from gdalconst import GA_ReadOnly
from collections import OrderedDict

class RasterReader(object):

    def __init__(self, RasterPath, scidbHost, scidbArray, attribute, chunksize, tiles):
        """
        Initialize the class RasterReader
        """
        self.width, self.height, self.datatype = self.GetRasterDimensions(RasterPath)
        self.RasterMetadata = self.CreateArrayMetadata(scidbArray, self.width, self.height, chunksize, tiles, attribute)
        self.CreateDestinationArray(scidbHost, scidbArray, attribute, self.datatype, self.height, self.width, chunksize)


    def GetMetadata(self, scidbInstances, rasterFilePath, outPath, loadPath, host):

        """
        Generator for the class
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
        if theURL=="NoSHIM":
            import scidb
            sdb = scidb.iquery()
        else:
            from scidbpy import connect
            sdb = connect(theURL)
        
        try:           
            sdb.query("create array %s <%s:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, attribute, rasterValueDataType, height-1, chunk, width-1, chunk) )
            print("Created array %s" % (rasterArrayName))
        except:
            print("******Array %s already exists. Removing" % (rasterArrayName))
            sdb.query("remove(%s)" % (rasterArrayName))
            sdb.query("create array %s <%s:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, attribute, rasterValueDataType, height-1, chunk, width-1, chunk) )

        del sdb 


    def CreateArrayMetadata(self, theArray, width, height, chunk=1000, tiles=1, attribute='value'):
        """
        This function gathers all the metadata necessary
        The loops are 
        """
        
        RasterReads = OrderedDict()
        rowMax = 0
        
        for y_version, yOffSet in enumerate(range(0, height, chunk)):
            #Adding +y_version will stagger the offset
            rowsRemaining = height - (y_version*chunk + y_version)

            #If this is not a short read, then read the correct size.
            if rowsRemaining > chunk: rowsRemaining = chunk
            
            for x_version, xOffSet in enumerate(range(0, width, chunk*tiles)):
                version_num = rowMax+x_version
                #Adding +x_version will stagger the offset
                columnsRemaining = width - (x_version*chunk*tiles + x_version)
                
                #If this is not a short read, then read the correct size.
                if columnsRemaining > chunk*tiles : columnsRemaining = chunk*tiles

                #print(rowsRemaining, columnsRemaining, version_num, x_version,y_version,)
                RasterReads[str(version_num)] = OrderedDict([ ("xOffSet",xOffSet+x_version), ("yOffSet",yOffSet+y_version), \
                    ("xWindow", columnsRemaining), ("yWindow", rowsRemaining), ("version", version_num), \
                    ("attribute", attribute), ("scidbArray", theArray), ("y_version", y_version), ("chunk", chunk) ])

            
            rowMax += math.ceil(width/(chunk*tiles))

        for r in RasterReads.keys():
            DictionaryKeys = RasterReads[r]
            DictionaryKeys["Loops"] = len(RasterReads)
            #print(DictionaryKeys)
        return RasterReads

def GlobalRasterLoading(theSHIM, theMetadata, theDict):
    """
    This function is built in an effort to separate the redimensioning from the loading.
    I think redimensioning affects the performance of the load if they run concurrently
    """

    if theSHIM == "NoSHIM":
        import scidb
        sdb = scidb.iquery()
    else:
        from scidbpy import connect  
        #sdb = connect('http://iuwrang-xfer2.uits.indiana.edu:8080')
        sdb = connect(theSHIM)

    theData = theMetadata.RasterMetadata
    for theKey in theMetadata.RasterMetadata.keys():
        start = timeit.default_timer()
        tempArray = "temprast_%s" % (theData[theKey]['version'])
        RedimensionAndInsertArray(sdb, tempArray, theData[theKey]['scidbArray'], theData[theKey]['xOffSet'], theData[theKey]['yOffSet'])    
        stop = timeit.default_timer()
        redimensionTime = stop-start
        
        RemoveTempArray(sdb, tempArray)
        print("Inserted version %s of %s" % (theData[theKey]['version'], theData[theKey]["Loops"] ))
        dataLoadingTime = (redimensionTime * theData[theKey]["Loops"]) / 60 
        if theData[theKey]['version'] == 0: print("Estimated time for redimensioning the dataset in minutes %s: RedimensionTime: %s" % ( dataLoadingTime, redimensionTime))
        if theData[theKey]['version'] > 1: 
            #sdb.query("remove_versions(%s, %s)" % (theMetadata['scidbArray'], theMetadata['version']))
            versions = sdb.versions(theData[theKey]['scidbArray'])
            if len(versions) > 1: 
                print("Versions you could remove: %s" % (versions))
                for v in versions[:-1]:
                    try:
                        sdb.query("remove_versions(%s, %s)" % (theData[theKey]['scidbArray'], v) )
                    except:
                        print("Couldn't remove version %s from array %s" % (v, theData[theKey]['scidbArray']) )
        
        #Add the redimension time to the TimeDictionary
        #timeDictionary  = {str(i[0]):{"version": i[0], "writeTime": i[1], "loadTime": i[2] }
        theDict[ str(theData[theKey]['version']) ]['redimensionTime'] = redimensionTime

    return theDict
# @profile
def GDALReader(inParams):
    """
    This is the main worker function, but I need to split it up
    """

    theMetadata = inParams[0]
    theInstance = inParams[1]
    theRasterPath = inParams[2]
    theSciDBOutPath = inParams[3]
    theSciDBLoadPath = inParams[4]

    if inParams[5] == "NoSHIM":
        import scidb
        sdb = scidb.iquery()
    else:
        from scidbpy import connect  
        #sdb = connect('http://iuwrang-xfer2.uits.indiana.edu:8080')
        sdb = connect(inParams[5])


    #print(theMetadata, theInstance, theRasterPath)
    
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
    
    start = timeit.default_timer()
    #WriteMultiDimensionalArray(array, rasterBinaryFilePath)
    #print(rasterBinaryFilePath)
    WriteArray(array, rasterBinaryFilePath)
    stop = timeit.default_timer()
    writeTime = stop-start
    #del raster, array
    #print(theMetadata['version'], writeTime) #locals())

    ### Used to evaluate the amount of memory used ####    
    # if theMetadata['version'] == 0:
    #     totalMemory = sum([sys.getsizeof(k) for k in locals().keys()])/10**6
    #     print("Total Memory used: %s" % (totalMemory ))

    # ### Would be a good diagnostic tool if I could get an estimate on writing time
    # if writeTime > 2:     
    #     # for k in locals().keys():
    #     #     print("Object name: %s, size of object: %s" % (k, sys.getsizeof(k) ))
    #     totalMemory = sum([sys.getsizeof(k) for k in locals().keys()])/10**6
    #     print("Total Memory used: %s" % (totalMemory) )
    # return (theMetadata['version'], writeTime, -999, -999)

    os.chmod(rasterBinaryFilePath, 0o755)
    CreateLoadArray(sdb, tempArray, theMetadata['attribute'], rasterValueDataType)
    start = timeit.default_timer()

    if LoadOneDimensionalArray(sdb, theInstance, tempArray, rasterValueDataType, rasterBinaryLoadPath):
        stop = timeit.default_timer()
        loadTime = stop-start
        
        # # start = timeit.default_timer()
        # # RedimensionAndInsertArray(sdb, tempArray, theMetadata['scidbArray'], theMetadata['xOffSet'], theMetadata['yOffSet'])    
        # # stop = timeit.default_timer()
        # # redimensionTime = stop-start
        
        # RemoveTempArray(sdb, tempArray)
        # print("Loaded version %s of %s" % (theMetadata['version'], theMetadata["Loops"] ))
        dataLoadingTime = ((writeTime + loadTime) * theMetadata["Loops"]) / 60 
        if theMetadata['version'] == 0: print("Estimated time for loading in minutes %s: WriteTime: %s, LoadTime: %s" % ( dataLoadingTime, writeTime, loadTime))
        # if theMetadata['version'] > 1: 
        #     #sdb.query("remove_versions(%s, %s)" % (theMetadata['scidbArray'], theMetadata['version']))
        #     versions = sdb.versions(theMetadata['scidbArray'])
        #     if len(versions) > 1: 
        #         print("Versions you could remove: %s" % (versions))
        #         for v in versions[:-1]:
        #             try:
        #                 sdb.query("remove_versions(%s, %s)" % (theMetadata['scidbArray'], v) )
        #             except:
        #                 print("Couldn't remove version %s from array %s" % (v, theMetadata['scidbArray']) )
        #                 pass

        os.remove(rasterBinaryFilePath)
        gc.collect()
        return (theMetadata['version'], writeTime, loadTime)

    else:
        print("Error Loading")
        os.remove(rasterBinaryFilePath)
        return (theMetadata['version'], -999, -999)
    
    
def WriteMultiDimensionalArray(rArray, binaryFilePath):
    """
    This is an old depreciated function for writing the multidimensional array as a binary file to be loaded into SciDB
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


def WriteArray(theArray, csvPath):
    """
    This function uses numpy tricks to write a numpy array in binary format with indices 
    """
    col, row = theArray.shape
    with open(csvPath, 'wb') as fileout:

        #Oneliner that creates the column index. Pull out [y for y in range(col)] to see how it works
        column_index = np.array(np.repeat([y for y in range(col)], row), dtype=np.dtype('int64'))
        
        #Oneliner that creates the row index. Pull ou the nested loop first: [x for x in range(row)]
        #Then pull the full list comprehension: [[x for x in range(row)] for i in range(col)]
        row_index = np.array(np.concatenate([[x for x in range(row)] for i in range(col)]), dtype=np.dtype('int64'))

        fileout.write( np.core.records.fromarrays([column_index, row_index, theArray.ravel()], dtype=[('x','int64'),('y','int64'),('value',theArray.dtype)]).ravel().tobytes() )

    del column_index, row_index, theArray

def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv
    """
    
    thekeys = list(theDictionary.keys())
    
    with open(filePath, 'w') as csvFile:
        fields = list(theDictionary[thekeys[0]].keys())
        theWriter = csv.DictWriter(csvFile, fieldnames=fields)
        theWriter.writeheader()

        for k in theDictionary.keys():
            theWriter.writerow(theDictionary[k])

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

def RemoveTempArray(sdb, tempRastName):
    """
    Remove the temporary array
    """
    sdb.query("remove(%s)" % (tempRastName))

def RedimensionAndInsertArray(sdb, tempArray, SciDBArray, xOffSet, yOffSet):
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


def main(pyVersion, Rasters, SciDBHost, rasterFilePath, SciDBOutPath, SciDBLoadPath):
    """
    This function creates the pool based upon the number of SciDB instances and the generates the parameters for each Python instance
    """
    query = sdb.queryAFL("list('instances')")
    SciDBInstances = list( range(len(query.splitlines())) )
    #pool = ProcessingPool(len(SciDBInstances))  #Pathos module
    pool = mp.Pool(len(query.splitlines()), maxtasksperchild=1)    #Multiprocessing module

    if pyVersion[0] > 2:
        try:
            results = pool.imap(GDALReader, (r for r in Rasters.GetMetadata(SciDBInstances, rasterFilePath, SciDBOutPath, SciDBLoadPath, SciDBHost)  ))
        except:
            print(multiprocessing.get_logger())
        #pool.map_async(GDALReader, zip(itertools.repeat(rasterFilePath), itertools.repeat(numProcesses), 
        #( (arrayReadSettings[r]["ReadWindow"], arrayReadSettings[r]["Base"], arrayReadSettings[r]["Width"], arrayReadSettings[r]["DataType"], r) for r in arrayReadSettings)   )  )
        #print(results, dir(results))
        timeDictionary  = {str(i[0]):{"version": i[0], "writeTime": i[1], "loadTime": i[2] } for i in results}

        return timeDictionary
        

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
    #parser.add_argument("-Instances", required =True, nargs='*', type=int, help="Number of SciDB Instances for parallel data loading", dest="instances", default=0)    
    parser.add_argument("-Host", required =True, help="SciDB host for connection", dest="host", default="localhost")
    #If host = NoSHIM, then use the cmd iquery   
    parser.add_argument("-RasterPath", required =True, help="Input file path for the raster", dest="rasterPath")    
    parser.add_argument("-SciDBArray", required =True, help="Name of the destination array", dest="rasterName")
    parser.add_argument("-AttributeNames", required =True, help="Name of the attribute(s) for the destination array", dest="attributes", default="value")
    parser.add_argument("-Tiles", required =False, type=int, help="Size in rows of the read window, default: 1", dest="tiles", default=1)
    parser.add_argument("-Chunk", required =False, type=int, help="Chunk size for the destination array, default: 1,000", dest="chunk", default=1000)
    parser.add_argument("-Overlap", required =False, type=int, help="Chunk overlap size. Adding overlap increases data loading time. default: 0", dest="overlap", default=0)
    parser.add_argument("-TempOut", required=False, default='/home/scidb/scidb_data/0/0', dest='OutPath',)
    parser.add_argument("-SciDBLoadPath", required=False, default='/home/scidb/scidb_data/0/0', dest='SciDBLoadPath')
    parser.add_argument("-CSV", required =False, help="Create CSV file", dest="csv", default="None")
    #parser.add_argument("-r", required =False, action="store_true",  help="randomly load data into the array", dest="randomload", default=False)
    return parser


if __name__ == '__main__':
    #Main function
    pythonVersion = sys.version_info
    args = argument_parser().parse_args()
    start = timeit.default_timer()
    RasterInformation = RasterReader(args.rasterPath, args.host, args.rasterName, args.attributes, args.chunk, args.tiles)

    WriteFile("/media/sf_scidb/glc_raster_reads6.csv", RasterInformation.RasterMetadata)
    timeDictionary = main(pythonVersion, RasterInformation, args.host, args.rasterPath, args.OutPath, args.SciDBLoadPath)
    allTimesDictionary = GlobalRasterLoading(args.host, RasterInformation, timeDictionary)

    if args.csv:
        WriteFile(args.csv, allTimesDictionary)

    stop = timeit.default_timer()
    print("Finished. Time to complete %s minutes" % ((stop-start)/60))
    # for r in RasterInformation.GetMetadata(args.instances, args.rasterPath,args.OutPath, args.SciDBLoadPath, args.host):
    #     print(r)

   
