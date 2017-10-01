# -*- coding: utf-8 -*-
"""
Created on Fri Dec 02 15:02:14 2016
A command line tool for conducting Zonal Statistics in SciDB

@author: dahaynes
"""


from osgeo import ogr, gdal
import scidbpy, timeit, csv, argparse, os, re
from collections import OrderedDict
import numpy as np
import multiprocessing as mp
import itertools

def world2Pixel(geoMatrix, x, y):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the pixel location of a geospatial 
    """
    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]
    yDist = geoMatrix[5]
    rtnX = geoMatrix[2]
    rtnY = geoMatrix[4]
    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / xDist)
    
    return (pixel, line)
    

def RasterizePolygon(inRasterPath, outRasterPath, vectorPath):
    """
    This function will Rasterize the Polygon based off the inRasterPath provided. 
    This only creates a memory raster
    The rasterization process uses the shapfile attribute ID
    """
    
    #The array size, sets the raster size 
    inRaster = gdal.Open(inRasterPath)
    rasterTransform = inRaster.GetGeoTransform()
    pixel_size = rasterTransform[1]
    
    #Open the vector dataset
    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()
    geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()
    
    outTransform= [geomMin_X, rasterTransform[1], 0, geomMax_Y, 0, rasterTransform[5] ]
    
    rasterWidth = int((geomMax_X - geomMin_X) / pixel_size)
    rasterHeight = int((geomMax_Y - geomMin_Y) / pixel_size)

    memDriver = gdal.GetDriverByName('MEM')
    theRast = memDriver.Create('', rasterWidth, rasterHeight, 1, gdal.GDT_Int16)
      
    theRast.SetProjection(inRaster.GetProjection())
    theRast.SetGeoTransform(outTransform)
    
    band = theRast.GetRasterBand(1)
    band.SetNoDataValue(-999)

    #If you want to use another shapefile field you need to change this line
    gdal.RasterizeLayer(theRast, [1], theLayer, options=["ATTRIBUTE=ID"])
    
    bandArray = band.ReadAsArray()
    del theRast, inRaster

    return bandArray

def GlobalJoin_SummaryStats(sdb, SciDBArray, rasterValueDataType, tempSciDBLoad, tempRastName, minY, minX, maxY, maxX, verbose=False):
    """
    SciDB Summary Stats
    1. Make an empty raster "Mask "that matches the SciDBArray
    2. Load the data into a 1D array
    3. Redimension and insert data into the mask array
    4. Conduct a global join using the between operators
    """
    import re    
    tempArray = "mask"

    #afl = sdb.afl
    #theArray = afl.show(SciDBArray)
    #results = theArray.contents()

    results = sdb.queryAFL("show(%s)" % (SciDBArray))
    results = results.decode("utf-8")
    #print(results)
    #SciDBArray()\n[('polygon<x:int64,y:int64,id:int16> [xy=0:*:0:1000000]')]\n
    #[('GLC2000<value:uint8> [x=0:40319:0:100000; y=0:16352:0:100000]')]
    
    #R = re.compile(r'\<(?P<attributes>[\S\s]*?)\>(\s*)\[(?P<dim_1>\S+)(;\s|,\s)(?P<dim_2>\S+)(\])')
    R = re.compile(r'\<(?P<attributes>[\S\s]*?)\>(\s*)\[(?P<dim_1>\S+)(;\s|,\s)(?P<dim_2>[^\]]+)')
    results = results.lstrip('results').strip()
    match = R.search(results)
    
    try:
        A = match.groupdict()
        schema = A['attributes']
        dimensions = "[%s; %s]" % (A['dim_1'], A['dim_2'])
    except:
        print(results)
        raise 

    try:
        sdbquery = r"create array %s <id:%s> %s" % (tempArray, rasterValueDataType, dimensions)
        sdb.query(sdbquery)
    except:
        print(sdbquery)
        sdb.query("remove(%s)" % tempArray)
        sdbquery = r"create array %s <id:%s> %s" % (tempArray, rasterValueDataType, dimensions)
        sdb.query(sdbquery)

    
    #Write the array in the correct location
    start = timeit.default_timer()
    sdbquery ="insert(faster_redimension(apply({A}, x, x1+{xOffSet}, y, y1+{yOffSet}, value, id), {B} ), {B})".format( A=tempRastName, B=tempArray, yOffSet=minY, xOffSet=minX)
    sdb.query(sdbquery)
    stop = timeit.default_timer()
    insertTime = stop-start
    if verbose: print(sdbquery , insertTime)

    #between(GLC2000, 4548, 6187, 7332, 12662)
    start = timeit.default_timer()
    sdbquery = "grouped_aggregate(join(between(%s, %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), id)" % (SciDBArray, minY, minX, maxY, maxX, tempArray, minY, minX, maxY, maxX)
    sdb.query(sdbquery)
    stop = timeit.default_timer()
    queryTime = stop-start
    sdb.queryResults(sdbquery, r"/home/04489/dhaynes/%s_states2.csv" % (SciDBArray) )
    if verbose: print(sdbquery, queryTime)
    sdb.query("remove(%s)" % tempArray)
    sdb.query("remove(%s)" % tempRastName)

    return insertTime, queryTime

def ArrayToBinary(theArray, yOffSet=0):
    """
    Use Numpy tricks to write a numpy array in binary format with indices 

    input: Numpy 2D array
    output: Numpy 2D array in binary format
    """
    col, row = theArray.shape
    
    thecolumns =[y for y in np.arange(0+yOffSet, col+yOffSet)]
    column_index = np.array(np.repeat(thecolumns, row), dtype=np.dtype('int64'))
    
    therows = [x for x in np.arange(row)]
    allrows = [therows for i in np.arange(col)]
    row_index = np.array(np.concatenate(allrows), dtype=np.dtype('int64'))

    values = theArray.ravel()
    vdatatype = theArray.dtype

    arraydatatypes = 'int64, int64, %s' % (vdatatype)
    dataset = np.core.records.fromarrays([column_index, row_index, values], names='y,x,value', dtype=arraydatatypes)

    return dataset
    #return dataset.ravel().tobytes()
        
    

def WriteMultiDimensionalArray(rArray, csvPath, xOffset=0, yOffset=0 ):
    '''
    This function write the multidimensional array as a binary 
    '''
    import numpy as np
    with open(csvPath, 'wb') as fileout:
        arrayHeight, arrayWidth = rArray.shape
        it = np.nditer(rArray, flags=['multi_index'], op_flags=['readonly'])
        for counter, pixel in enumerate(it):
            col, row = it.multi_index
            #if counter < 100: 
            #print("y/column: %s, x/row: %s" % (col + yOffset, row + xOffset))
            indexvalue = np.array([col + yOffset, row + xOffset], dtype=np.dtype('int64'))

            fileout.write( indexvalue.tobytes() )
            fileout.write( it.value.tobytes() )
   
    return(arrayHeight, arrayWidth)


def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv
    """
    
    thekeys = list(theDictionary.keys())
    
    with open(filePath, 'w') as csvFile:
        fields = list(theDictionary[thekeys[0]].keys())
        #fields.append("test")
        #print(fields)
        theWriter = csv.DictWriter(csvFile, fieldnames=fields)
        theWriter.writeheader()

        for k in theDictionary.keys():
            #theDictionary[k].update({"test": k})
            #print(theDictionary)
            theWriter.writerow(theDictionary[k])

def QueryResults():
    """
    Function to perform the Zonal Analysis can get back the results
    """

    afl = sdb.afl
    result = afl.grouped_aggregate(afl.join(polygonSciDBArray.name, afl.subarray(SciDBArray, ulY, ulX, lrY, lrX)), max("value"), "f0")
    #query = "grouped_aggregate(join(%s,subarray(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), f0)" % (polygonSciDBArray.name, SciDBArray, ulY, ulX, lrY, lrX)


def LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, dim1="y", dim2="x", verbose=False, loadMode=-2):
    """
    Function Loads 1D array data into sciDB
    in : 
        sdb connection
        tempSciDBLoad - path for loading scidbdata
        tempRastName - Name for loading raster dataset
        rasterValeDataType - Numpy value type
        dim1 = name of the dimension (default = x) 
        dim2 = name of the dimension (default = y) 
    out : 
        binaryLoadPath : complete path to where the file is written (*.scidb)
    """

    #chunksize = int(input("Please input chunksize: "))
    #if isinstance(chunksize, int):

    #binaryLoadPath = '%s/%s.scidb' % (tempSciDBLoad,tempRastName )
    try:
        sdbquery = "create array %s <%s:int64, %s:int64, id:%s> [xy=0:*,?,?]" % (tempRastName, dim1, dim2, rasterValueDataType)
        sdb.query(sdbquery)
    except:
        sdb.query("remove(%s)" % (tempRastName))
        sdbquery = "create array %s <%s:int64, %s:int64, id:%s> [xy=0:*,?,?]" % (tempRastName, dim1, dim2, rasterValueDataType)
        sdb.query(sdbquery)

    start = timeit.default_timer()


    sdbquery = "load(%s,'%s', %s, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, loadMode, rasterValueDataType)
    sdb.query(sdbquery)
    stop = timeit.default_timer()
    loadTime = stop-start
    if verbose: print(sdbquery , loadTime)

    return binaryLoadPath, loadTime

def EquiJoin_SummaryStats(sdb, SciDBArray, tempRastName, rasterValueDataType, tempSciDBLoad, minY, minX, maxY, maxX, verbose=False):
    """
    1. Load the polygon array in as a 1D array, shifted correctly
    2. Peform EquiJoin using the between
    Example (equi_join(between(GLC2000, 4548, 6187, 7331, 12661)
    grouped_aggregate(equi_join(between(GLC2000, 4548, 6187, 7332, 12662), polygon), 'left_names=x,y', 'right_names=x,y'), min(value), max(value), avg(value), count(value), id)

    """
    binaryLoadPath = "%s/%s.scidb" % (tempSciDBLoad, tempRastName)
    binaryLoadPath, loadTime = LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, 'y', 'x', verbose, -1)
    
    start = timeit.default_timer()
    sdbquery = "grouped_aggregate(equi_join(between(%s, %s, %s, %s, %s), %s, 'left_names=x,y', 'right_names=x,y', 'algorithm=hash_replicate_right'), min(value), max(value), avg(value), count(value), id)" % (SciDBArray, minY, minX, maxY, maxX, tempRastName) 
    sdb.query(sdbquery) 
    stop = timeit.default_timer()
    queryTime = stop-start
    if verbose: print(sdbquery, queryTime)
    
    return loadTime, queryTime

def SubArray_SummaryStats(sdb, polygonSciDBArrayName, SciDBArray, minX, minY, maxX, maxY, verbose=False):
    """
    Dimension 1 = Y ulY:4548 to lrY:7332 
    Dimension 2 = X ulX:6187 to lrX:12662 

    """

    #Raster Summary Stats
    query = "grouped_aggregate(join(%s,subarray(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), f0)" % (polygonSciDBArrayName, SciDBArray, minY, minX, maxY, maxX)
    start = timeit.default_timer()
    if verbose: print(query)
    results = sdb.query(query)
    stop = timeit.default_timer()
    queryTime = stop-start

    return queryTime

def WriteBinaryFile(params):
    """
    This function writes a binary file
    """

    binaryPath = params[0]
    datastore, chunk = params[1]

    binaryPartitionPath = "%s/%s/p_zones.scidb" % (binaryPath, datastore)
    #print(binaryPartitionPath)
    with open(binaryPartitionPath, 'wb') as fileout:
        fileout.write(chunk.ravel().tobytes())

def ParallelProcessing(params):
    """
    This function wraps around the ArrayToBinary and WriteBinaryFile and 
    """

    binaryPath = params[0]
    yOffSet = params[1]
    #print(yOffSet)
    datastore, arrayChunk = params[2]

    binaryPartitionPath = "%s/%s/p_zones.scidb" % (binaryPath, datastore)
    #print(binaryPartitionPath)
    
    with open(binaryPartitionPath, 'wb') as fileout:
        fileout.write(ArrayToBinary(arrayChunk, yOffSet).ravel().tobytes())
        print(binaryPartitionPath)

def ZonalStats(NumberofTests, boundaryPath, rasterPath, SciDBArray, sdb, statsMode=1, filePath=None, verbose=False):
    "This function conducts zonal stats in SciDB"
    
    outDictionary = OrderedDict()

    for t in range(NumberofTests):
        theTest = "test_%s" % (t+1)
        #outDictionary[theTest]

        vectorFile = ogr.Open(boundaryPath)
        theLayer = vectorFile.GetLayer()
        geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()

        inRaster = gdal.Open(rasterPath)
        rasterTransform = inRaster.GetGeoTransform()

        start = timeit.default_timer()
        rasterizedArray = RasterizePolygon(rasterPath, r'/home/scidb/scidb_data/0/0/nothing.tiff', boundaryPath)
        rasterValueDataType = rasterizedArray.dtype
        stop = timeit.default_timer()
        rasterizeTime = stop-start
        print("Rasterization time %s for file %s" % (rasterizeTime, boundaryPath ))
        

        ulX, ulY = world2Pixel(rasterTransform, geomMin_X, geomMax_Y)
        lrX, lrY = world2Pixel(rasterTransform, geomMax_X, geomMin_Y)
        
        if verbose:
            print("Rasterized Array columns:%s, rows: %s" % (rasterizedArray.shape[0], rasterizedArray.shape[1]))
            print("Dimension 1 = Y ulY:%s to lrY:%s " % ( ulY, lrY))
            print("Dimension 2 = X ulX:%s to lrX:%s " % ( ulX, lrX))
            print("Number of pixels: %s" % ( format( rasterizedArray.shape[0]*rasterizedArray.shape[1], ',d')) )

            

        if statsMode == 1:
            #Transfering Raster Array to SciDB
            
            chunksize = int(input("Please input chunksize: "))
            if isinstance(chunksize, int):
                start = timeit.default_timer()
                polygonSciDBArray = sdb.from_array(rasterizedArray, instance_id=0, persistent=False, chunk_size=chunksize) 
                stop = timeit.default_timer()
                transferTime = stop-start
            
            #polygonSciDBArray = sdb.from_array(rasterizedArray, dim_low=(4000,5000), dim_high=(5000,7000), instance_id=0, chunk_size=1000) 
            #name="states"
        
            queryTime = SubArray_SummaryStats(sdb, polygonSciDBArray.name, SciDBArray, ulX, ulY, lrX, lrY, verbose)

        elif statsMode == 2:
            csvPath = '/home/scidb/scidb_data/0/0/polygon.scidb'
            WriteMultiDimensionalArray(rasterizedArray, csvPath, ulX, ulY )    
            tempRastName = csvPath.split('/')[-1].split('.')[0]
            tempSciDBLoad = '/'.join(csvPath.split('/')[:-1])
            transferTime, queryTime = EquiJoin_SummaryStats(sdb, SciDBArray, tempRastName, rasterValueDataType, tempSciDBLoad, ulY, ulX, lrY, lrX, verbose)

        elif statsMode == 3:
            csvPath = '/home/scidb/scidb_data/0/0/zones.scidb'
            WriteMultiDimensionalArray(rasterizedArray, csvPath)
            tempSciDBLoad = '/'.join(csvPath.split('/')[:-1])
            tempRastName = csvPath.split('/')[-1].split('.')[0]

            binaryLoadPath = "%s/%s.scidb" % (tempSciDBLoad, tempRastName)
            LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, "y1", "x1", verbose, -2)

            transferTime, queryTime = GlobalJoin_SummaryStats(sdb, SciDBArray, rasterValueDataType, binaryLoadPath, tempRastName, ulY, ulX, lrY, lrX, verbose)

        elif statsMode == 4:
            #This is the serial version
            print("Serial Version of Zonal Stats")
            binaryPath = '/home/scidb/scidb_data/0' #/storage/0
            print("Converting to Binary")
            tempRastName = 's_zones'
            start = timeit.default_timer()
            binaryArray = ArrayToBinary(rasterizedArray)
            stop = timeit.default_timer()
            print("Took: %s" % (stop-start))

            print("Writing Binary File")
            start = timeit.default_timer()
            binaryLoadPath = "%s/s_zones.scidb" % (binaryPath)
            with open(binaryLoadPath, 'wb') as fileout:
                fileout.write(binaryArray.ravel().tobytes())
                print(binaryLoadPath)
            stop = timeit.default_timer()
            print("Took: %s" % (stop-start))    
            
            print("Loading 1D File")
            start = timeit.default_timer()
            #binaryLoadPath = "p_zones.scidb" #'/data/projects/services/scidb/scidbtrunk/stage/DB-mydb/0/0'  #binaryPartitionPath.split("/")[-1]
            LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, "y1", "x1", verbose, -2)
            stop = timeit.default_timer()
            print("Took: %s" % (stop-start))    
            
            transferTime, queryTime = GlobalJoin_SummaryStats(sdb, SciDBArray, rasterValueDataType, '', tempRastName, ulY, ulX, lrY, lrX, verbose)
                
        elif statsMode == 5:
            #This is the full parallel mode
            print("Parallel Version of Zonal Stats")
            import scidb
            sdb = scidb.iquery()
            query = sdb.queryAFL("list('instances')")
            SciDBInstances = len(query.splitlines())-1

            tempRastName = 'p_zones'
            binaryPath = '/storage' #'/home/scidb/scidb_data/0'
            
            pool = mp.Pool(SciDBInstances)

            print("Partitioning Array")
            start = timeit.default_timer()
            chunkedArrays = np.array_split(rasterizedArray, SciDBInstances, axis=0)
            stop = timeit.default_timer()
            print("Took: %s" % (stop-start))

            #This is super ugly, but I can't think of the one liner!
            allColumns = [c.shape[0] for c in chunkedArrays]
            yOffSet = [0]
            z = 0
            for c in allColumns:
                z += c
                yOffSet.append(z)
            #Remove the last item 
            yOffSet.pop()

            print("Converting to Binary and Writing Files in Parallel")
            start = timeit.default_timer()
            results = pool.imap(ParallelProcessing, zip( itertools.repeat(binaryPath), itertools.cycle(yOffSet), ((p, chunk) for p, chunk in enumerate(chunkedArrays))  )    )
            pool.close()
            pool.join()
            stop = timeit.default_timer()
            print("Took: %s" % (stop-start))            
            
            print("Loading...")
            start = timeit.default_timer()
            binaryLoadPath = "p_zones.scidb" #'/data/projects/services/scidb/scidbtrunk/stage/DB-mydb/0'  #binaryPartitionPath.split("/")[-1]
            LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, "y1", "x1", verbose, -1)
            stop = timeit.default_timer()
            print("Took: %s" % (stop-start))
            
            transferTime, queryTime = GlobalJoin_SummaryStats(sdb, SciDBArray, rasterValueDataType, '', tempRastName, ulY, ulX, lrY, lrX, verbose)

        print("Zonal Analyis time %s, for file %s, Query run %s " % (queryTime, boundaryPath, t+1 ))
        if verbose: print("Redimension Time: %s" % (transferTime)  )
        outDictionary[theTest] = OrderedDict( [ ("test",theTest), ("SciDBArrayName",SciDBArray), ("BoundaryFilePath",boundaryPath), ("transfer_time",transferTime), ("rasterization_time",rasterizeTime), ("query_time",queryTime), ("total_time",transferTime+rasterizeTime+queryTime) ] )
    

    if filePath:
        WriteFile(filePath, outDictionary)
    print("Finished")


def CheckFiles(*argv):
    "This function checks files to make sure they exist"
    for i in argv:
        if not os.path.exists(i): 
            print("FilePath %s does not exist" % (i) )
            return False
    return True

def argument_parser():
    parser = argparse.ArgumentParser(description="Conduct SciDB Zonal Stats")   
    parser.add_argument('-SciDBArray', required=True, dest='SciArray')
    parser.add_argument('-Raster', required=True, dest='Raster')
    parser.add_argument('-Shapefile', required=True, dest='Shapefile')
    parser.add_argument('-Tests', type=int, help="Number of tests you want to run", required=False, default=3, dest='Runs')
    parser.add_argument('-Mode', help="This allows you to choose the mode of analysis you want to conduct", type=int, default=1, required=True, dest='mode')
    parser.add_argument('-CSV', required=False, dest='CSV')
    parser.add_argument('-v', required=False, action="store_true", default=False, dest='verbose')
    parser.add_argument('-Host', required=False, help="SciDB host for connection", dest="host", default="http://localhost:8080")     

    # group = parser.add_mutually_exclusive_group()
    # group.add_argument("-v", "--verbose", action="store_true")
    # group.add_argument("-q", "--quiet", action="store_true")   
    return parser

if __name__ == '__main__':
    args = argument_parser().parse_args()
    if CheckFiles(args.Shapefile, args.Raster):
        if args.host == "NoSHIM":
            import scidb
            sdb = scidb.iquery()
            print("No SHIM")
        else:
            from scidbpy import connect  
            #sdb = connect('http://iuwrang-xfer2.uits.indiana.edu:8080')
            sdb = connect(args.host)
        ZonalStats(args.Runs, args.Shapefile, args.Raster, args.SciArray, sdb, args.mode, args.CSV, args.verbose)
    else:
        print(args.Shapefile, args.Raster)

