# -*- coding: utf-8 -*-
"""
A command line tool for conducting Zonal Statistics in SciDB
"""

from osgeo import ogr, gdal
import scidb, timeit, csv, argparse, os
from collections import OrderedDict
import numpy as np
import multiprocessing as mp
import itertools


def world2Pixel(geoMatrix, x, y):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate the pixel location of a geospatial

    Input:
        geoMatrix = The matrix to use for the calculation
        x = The x dimension to use
        y = The y dimension to use

    Output:
        A tuple in the following format:
            (pixel value, line value)
    """
    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]

    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / xDist)

    return abs(pixel), abs(line)


def RasterizePolygon(inRasterPath, vectorPath):
    """
    This function will Rasterize the Polygon based off the inRasterPath provided. 
    This only creates a memory raster
    The rasterization process uses the shapfile attribute ID

    Input:
        inRasterPath = The path where the raster is
        vectorPath = The path to the vector dataset

    Output:
        The band array
    """

    # The array size, sets the raster size
    inRaster = gdal.Open(inRasterPath)
    rasterTransform = inRaster.GetGeoTransform()
    pixel_size = rasterTransform[1]

    # Open the vector dataset
    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()
    geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()

    # Transform the polygon
    outTransform = [geomMin_X, rasterTransform[1], 0, geomMax_Y, 0, rasterTransform[5]]

    rasterWidth = int((geomMax_X - geomMin_X) / pixel_size)
    rasterHeight = int((geomMax_Y - geomMin_Y) / pixel_size)

    memDriver = gdal.GetDriverByName('MEM')
    theRast = memDriver.Create('', rasterWidth, rasterHeight, 1, gdal.GDT_Int16)

    theRast.SetProjection(inRaster.GetProjection())
    theRast.SetGeoTransform(outTransform)

    band = theRast.GetRasterBand(1)
    band.SetNoDataValue(-999)

    # If you want to use another shapefile field you need to change this line
    gdal.RasterizeLayer(theRast, [1], theLayer, options=["ATTRIBUTE=ID"])

    bandArray = band.ReadAsArray()
    del theRast, inRaster

    return bandArray


def GlobalJoin_SummaryStats(sdb, SciDBArray, rasterValueDataType, tempSciDBLoad, tempRastName, minY, minX, maxY, maxX,
                            verbose=False):
    """
    SciDB Summary Stats
    1. Make an empty raster "Mask "that matches the SciDBArray
    2. Load the data into a 1D array
    3. Redimension and insert data into the mask array
    4. Conduct a global join using the between operators

    Input:
        sdb = The SciDB connection to use
        SciDBArray = The sciDB array to use
        rasterValueDataType = The data type of the raster value
        tempSciDBLoad = Temporary SciDB loading array
        tempRastName = Name of the remporary array
        minY = Minimum y dimension
        minX = Minimum x dimension
        maxY = Maximum y dimension
        maxX = Maximum x dimension
        verbose = True to print times, false otherwise

    Output:
        A tuple in the following format:
            (insertion time, query time)
    """
    import re
    tempArray = "mask"

    # Show array, search through it
    results = sdb.queryAFL("show(%s)" % SciDBArray)
    results = results.decode("utf-8")

    R = re.compile(r'\<(?P<attributes>[\S\s]*?)\>(\s*)\[(?P<dim_1>\S+)(;\s|,\s)(?P<dim_2>[^\]]+)')
    results = results.lstrip('results').strip()
    match = R.search(results)

    try:
        A = match.groupdict()
        dimensions = "[%s; %s]" % (A['dim_1'], A['dim_2'])
    except:
        print(results)
        raise

    # Create the array, removing on failure
    sdbquery = None
    try:
        sdbquery = r"create array %s <id:%s> %s" % (tempArray, rasterValueDataType, dimensions)
        sdb.query(sdbquery)
    except:
        print(sdbquery)
        sdb.query("remove(%s)" % tempArray)
        sdbquery = r"create array %s <id:%s> %s" % (tempArray, rasterValueDataType, dimensions)
        sdb.query(sdbquery)

    # Write the array in the correct location
    start = timeit.default_timer()
    sdbquery = "insert(redimension(apply({A}, x, x1+{xOffSet}, y, y1+{yOffSet}, value, id), {B} ), {B})".format(
        A=tempRastName, B=tempArray, yOffSet=minY, xOffSet=minX)
    sdb.query(sdbquery)
    stop = timeit.default_timer()
    insertTime = stop - start
    if verbose:
        print(sdbquery, insertTime)

    start = timeit.default_timer()
    sdbquery = "grouped_aggregate(join(between(%s, %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), min(value), " \
               "max(value), avg(value), count(value), id)" % (SciDBArray, minY, minX, maxY, maxX, tempArray, minY, minX,
                                                              maxY, maxX)
    sdb.query(sdbquery)
    stop = timeit.default_timer()
    queryTime = stop - start
    if verbose:
        print(sdbquery, queryTime)
    sdb.query("remove(%s)" % tempArray)
    sdb.query("remove(%s)" % tempRastName)

    return insertTime, queryTime


def ArrayToBinary(theArray, yOffSet=0):
    """
    Use Numpy tricks to write a numpy array in binary format with indices 

    input:
        theArray = Numpy 2D array
        yOffSet = Y offset to start at

    output:
        Numpy 2D array in binary format
    """
    col, row = theArray.shape

    thecolumns = [y for y in np.arange(0 + yOffSet, col + yOffSet)]
    column_index = np.array(np.repeat(thecolumns, row), dtype=np.dtype('int64'))

    therows = [x for x in np.arange(row)]
    allrows = [therows for i in np.arange(col)]
    row_index = np.array(np.concatenate(allrows), dtype=np.dtype('int64'))

    values = theArray.ravel()
    vdatatype = theArray.dtype

    arraydatatypes = 'int64, int64, %s' % vdatatype
    dataset = np.core.records.fromarrays([column_index, row_index, values], names='y,x,value', dtype=arraydatatypes)

    return dataset


def WriteMultiDimensionalArray(rArray, csvPath, xOffset=0, yOffset=0):
    """
    This function write the multidimensional array as a binary

    Input:
        rArray = Array to write
        csvPath = CSV path to write to
        xOffset = X offset for starting writing
        yOffset = Y offset for starting writing

    Output:
        A tuple containing the array height and width
    """
    import numpy as np
    with open(csvPath, 'wb') as fileout:
        arrayHeight, arrayWidth = rArray.shape
        it = np.nditer(rArray, flags=['multi_index'], op_flags=['readonly'])
        for counter, pixel in enumerate(it):
            # Write the chunk to the csvPath
            col, row = it.multi_index
            indexvalue = np.array([col + yOffset, row + xOffset], dtype=np.dtype('int64'))

            fileout.write(indexvalue.tobytes())
            fileout.write(it.value.tobytes())

    return arrayHeight, arrayWidth


def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv

    Input:
        filePath = File path to write to
        theDictionary = Dictionary to write

    Output:
        None
    """

    thekeys = list(theDictionary.keys())

    with open(filePath, 'w') as csvFile:
        fields = list(theDictionary[thekeys[0]].keys())

        theWriter = csv.DictWriter(csvFile, fieldnames=fields)
        theWriter.writeheader()

        for k in theDictionary.keys():
            theWriter.writerow(theDictionary[k])


def LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, dim1="y", dim2="x", verbose=False,
                     loadMode=-2):
    """
    Function Loads 1D array data into sciDB

    input:
        sdb = sdb connection
        tempRastName = Name for loading raster dataset
        binaryLoadPath = path for loading binary scidbdata
        rasterValeDataType = Numpy value type
        dim1 = name of the dimension (default = x) 
        dim2 = name of the dimension (default = y)
        verbose = Whether or not to print results
        loadMode = Which mode to use for SciDB loading

    output:
        Tuple of complete path to where the file is written
            (path *.scidb, load time)
    """

    # Create the array, removing it if already exists
    try:
        sdbquery = "create array %s <%s:int64, %s:int64, id:%s> [xy=0:*,?,?]" % (tempRastName, dim1, dim2,
                                                                                 rasterValueDataType)
        sdb.query(sdbquery)
    except:
        sdb.query("remove(%s)" % tempRastName)
        sdbquery = "create array %s <%s:int64, %s:int64, id:%s> [xy=0:*,?,?]" % (tempRastName, dim1, dim2,
                                                                                 rasterValueDataType)
        sdb.query(sdbquery)

    start = timeit.default_timer()

    # Run the load
    sdbquery = "load(%s,'%s', %s, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, loadMode,
                                                             rasterValueDataType)
    sdb.query(sdbquery)
    stop = timeit.default_timer()
    loadTime = stop - start
    if verbose:
        print(sdbquery, loadTime)

    return binaryLoadPath, loadTime


def EquiJoin_SummaryStats(sdb, SciDBArray, tempRastName, rasterValueDataType, tempSciDBLoad, minY, minX, maxY, maxX,
                          verbose=False):
    """
    1. Load the polygon array in as a 1D array, shifted correctly
    2. Peform EquiJoin using the between
    Example (equi_join(between(GLC2000, 4548, 6187, 7331, 12661)
    grouped_aggregate(equi_join(between(GLC2000, 4548, 6187, 7332, 12662), polygon), 'left_names=x,y',
        'right_names=x,y'), min(value), max(value), avg(value), count(value), id)

    Input:
        sdb = Connection to a SciDB instance
        SciDBArray = Names of the SciDB array to process on
        tempRastName = Temporary raster name
        rasterValueDataType = Type of the raster
        tempSciDBLoad = Temporary SciDB load array
        minY = Minimum y to process on
        minX = Minimum X to process on
        maxX = Maximum X to process on
        maxY = Maximum Y to process on
        verbose = Whether or not to write results

    Output:
        The loading time and query time in a tuple
    """
    binaryLoadPath = "%s/%s.scidb" % (tempSciDBLoad, tempRastName)
    binaryLoadPath, loadTime = LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, 'y', 'x',
                                                verbose, -1)

    start = timeit.default_timer()
    sdbquery = "grouped_aggregate(equi_join(between(%s, %s, %s, %s, %s), %s, 'left_names=x,y', 'right_names=x,y', " \
               "'algorithm=hash_replicate_right'), min(value), max(value), avg(value), count(value), id)" % \
               (SciDBArray, minY, minX, maxY, maxX, tempRastName)
    sdb.query(sdbquery)
    stop = timeit.default_timer()
    queryTime = stop - start
    if verbose:
        print(sdbquery, queryTime)

    return loadTime, queryTime


def SubArray_SummaryStats(sdb, polygonSciDBArrayName, SciDBArray, minX, minY, maxX, maxY, verbose=False):
    """
    Dimension 1 = Y ulY:4548 to lrY:7332 
    Dimension 2 = X ulX:6187 to lrX:12662

    Input:
        sdb = Connection to a SciDB instance
        polygonSciDBArrayName = Name of the polygon array
        SciDBArray = SciDBArray to process
        minY = Minimum y to process on
        minX = Minimum X to process on
        maxX = Maximum X to process on
        maxY = Maximum Y to process on
        verbose = Whether or not to write results

    Output:
        The time to query
    """

    # Raster Summary Stats
    query = "grouped_aggregate(join(%s,subarray(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), " \
            "count(value), f0)" % (polygonSciDBArrayName, SciDBArray, minY, minX, maxY, maxX)
    start = timeit.default_timer()
    if verbose:
        print(query)
    results = sdb.query(query)
    stop = timeit.default_timer()
    queryTime = stop - start

    return queryTime


def WriteBinaryFile(params):
    """
    This function writes a binary file

    Input:
        params = A list containing the following, in order:
            binaryPath = Path to write to
            datastore, chunk = where the data is stored, the chunk to write

    Output:
        None
    """

    binaryPath = params[0]
    datastore, chunk = params[1]

    folder = "%s/%s" % (binaryPath, datastore)
    if not os.path.exists(folder):
        os.mkdir(folder)

    binaryPartitionPath = "%s/p_zones.scidb" % folder
    with open(binaryPartitionPath, 'wb') as fileout:
        fileout.write(chunk.ravel().tobytes())


def ParallelProcessing(params):
    """
    This function wraps around the ArrayToBinary and WriteBinaryFile

    Input:
        params = A list containing the following, in order:
            binaryPath = The path to write to
            yOffSet = The offset for the y dimension
            datastore, arrayChunk = Where the data is stored, the chunk to write

    Output:
        None
    """

    binaryPath = params[0]
    yOffSet = params[1]
    datastore, arrayChunk = params[2]

    folder = "%s/%s" % (binaryPath, datastore)
    if not os.path.exists(folder):
        os.mkdir(folder)

    binaryPartitionPath = "%s/p_zones.scidb" % folder

    with open(binaryPartitionPath, 'wb') as fileout:
        fileout.write(ArrayToBinary(arrayChunk, yOffSet).ravel().tobytes())
        print(binaryPartitionPath)


def ZonalStats(NumberofTests, boundaryPath, rasterPath, SciDBArray, sdb, statsMode=1, filePath=None, verbose=False,
               csvPath=None, binaryPath=None):
    """
    This function conducts zonal stats in SciDB

    Input:
        NumberofTests = Number of tests to run
        boundaryPath = Path to the shape file
        rasterPath = Path to the raster file
        SciDBArray = The array name
        sdb = Connection to a SciDB instance
        statsMode = Mode of analysis to conduct
        filePath = Path to CSV file
        verbose = Whether or not to use verbose version
        csvPath = CSV path for options 2 and 3
        binaryPath = path to store temporary binary files in for modes 4 and 5

    Output:
        None
    """

    outDictionary = OrderedDict()

    for t in range(NumberofTests):
        theTest = "test_%s" % (t + 1)

        vectorFile = ogr.Open(boundaryPath)
        theLayer = vectorFile.GetLayer()
        geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()

        inRaster = gdal.Open(rasterPath)
        rasterTransform = inRaster.GetGeoTransform()
        # Check the time for rasterization
        start = timeit.default_timer()
        rasterizedArray = RasterizePolygon(rasterPath, boundaryPath)
        rasterValueDataType = rasterizedArray.dtype
        stop = timeit.default_timer()
        rasterizeTime = stop - start
        print("Rasterization time %s for file %s" % (rasterizeTime, boundaryPath))

        ulX, ulY = world2Pixel(rasterTransform, geomMin_X, geomMax_Y)
        lrX, lrY = world2Pixel(rasterTransform, geomMax_X, geomMin_Y)

        if verbose:
            print("Rasterized Array columns:%s, rows: %s" % (rasterizedArray.shape[0], rasterizedArray.shape[1]))
            print("Dimension 1 = Y ulY:%s to lrY:%s " % (ulY, lrY))
            print("Dimension 2 = X ulX:%s to lrX:%s " % (ulX, lrX))
            print("Number of pixels: %s" % (format(rasterizedArray.shape[0] * rasterizedArray.shape[1], ',d')))

        queryTime = None
        transferTime = None

        if statsMode == 3:
            # Use GlobalJoin summary stats
            WriteMultiDimensionalArray(rasterizedArray, csvPath)
            tempSciDBLoad = '/'.join(csvPath.split('/')[:-1])
            tempRastName = csvPath.split('/')[-1].split('.')[0]

            binaryLoadPath = "%s/%s.scidb" % (tempSciDBLoad, tempRastName)
            LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, "y1", "x1", verbose, -2)

            transferTime, queryTime = GlobalJoin_SummaryStats(sdb, SciDBArray, rasterValueDataType, binaryLoadPath,
                                                              tempRastName, ulY, ulX, lrY, lrX, verbose)

        elif statsMode == 4:
            # This is the serial version
            print("Serial Version of Zonal Stats")
            print("Converting to Binary")
            tempRastName = 's_zones'
            start = timeit.default_timer()
            binaryArray = ArrayToBinary(rasterizedArray)
            stop = timeit.default_timer()
            print("Took: %s" % (stop - start))

            print("Writing Binary File")
            start = timeit.default_timer()
            print("BIN %s" % binaryPath)
            binaryLoadPath = "%s/s_zones.scidb" % binaryPath
            with open(binaryLoadPath, 'wb') as fileout:
                fileout.write(binaryArray.ravel().tobytes())
                print(binaryLoadPath)
            stop = timeit.default_timer()
            print("Took: %s" % (stop - start))

            print("Loading 1D File")
            start = timeit.default_timer()
            LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, "y1", "x1", verbose, -2)
            stop = timeit.default_timer()
            print("Took: %s" % (stop - start))

            transferTime, queryTime = GlobalJoin_SummaryStats(sdb, SciDBArray, rasterValueDataType, '',
                                                              tempRastName, ulY, ulX, lrY, lrX, verbose)

        elif statsMode == 5:
            # This is the full parallel mode
            print("Parallel Version of Zonal Stats")
            import scidb
            sdb = scidb.iquery()
            query = sdb.queryAFL("list('instances')")
            SciDBInstances = len(query.splitlines()) - 1

            tempRastName = 'p_zones'

            pool = mp.Pool(SciDBInstances)

            print("Partitioning Array")
            start = timeit.default_timer()
            chunkedArrays = np.array_split(rasterizedArray, SciDBInstances, axis=0)
            stop = timeit.default_timer()
            print("Took: %s" % (stop - start))

            # This is super ugly, but I can't think of the one liner!
            allColumns = [c.shape[0] for c in chunkedArrays]
            yOffSet = [0]
            z = 0
            for c in allColumns:
                z += c
                yOffSet.append(z)
            # Remove the last item
            yOffSet.pop()

            print("Converting to Binary and Writing Files in Parallel")
            start = timeit.default_timer()
            results = pool.imap(ParallelProcessing, zip(itertools.repeat(binaryPath), itertools.cycle(yOffSet),
                                                        ((p, chunk) for p, chunk in enumerate(chunkedArrays))))
            pool.close()
            pool.join()
            stop = timeit.default_timer()
            print("Took: %s" % (stop - start))

            print("Loading...")
            start = timeit.default_timer()
            binaryLoadPath = "p_zones.scidb"
            LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, "y1", "x1", verbose, -1)
            stop = timeit.default_timer()
            print("Took: %s" % (stop-start))

            transferTime, queryTime = GlobalJoin_SummaryStats(sdb, SciDBArray, rasterValueDataType, '', tempRastName,
                                                              ulY, ulX, lrY, lrX, verbose)

        print("Zonal Analysis time %s, for file %s, Query run %s " % (queryTime, boundaryPath, t + 1))
        if verbose:
            print("Redimension Time: %s" % transferTime)
        outDictionary[theTest] = OrderedDict([("test", theTest), ("SciDBArrayName", SciDBArray),
                                              ("BoundaryFilePath", boundaryPath), ("transfer_time", transferTime),
                                              ("rasterization_time", rasterizeTime), ("query_time", queryTime),
                                              ("total_time", transferTime + rasterizeTime + queryTime)])

    if filePath:
        WriteFile(filePath, outDictionary)
    print("Finished")


def CheckFiles(*argv):
    """
    This function checks files to make sure they exist

    Input:
        argv = Filepaths to check

    Output:
        False if a path does not exist, True otherwise
    """
    for i in argv:
        if not os.path.exists(i):
            print("FilePath %s does not exist" % i)
            return False
    return True


def argument_parser():
    """
    Parse arguments and return Arguments

    Input:
        None

    Output:
        The argument parser
    """

    parser = argparse.ArgumentParser(description="Conduct SciDB Zonal Stats")
    parser.add_argument('-SciDBArray', required=True, dest='SciArray')
    parser.add_argument('-Raster', required=True, dest='Raster')
    parser.add_argument('-Shapefile', required=True, dest='Shapefile')
    parser.add_argument('-Tests', type=int, help="Number of tests you want to run", required=False, default=3,
                        dest='Runs')
    parser.add_argument('-Mode', help="This allows you to choose the mode of analysis you want to conduct", type=int,
                        default=1, required=True, dest='mode')
    parser.add_argument('-CSV', required=False, dest='CSV')
    parser.add_argument('-v', required=False, action="store_true", default=False, dest='verbose')
    parser.add_argument('-Host', required=False, help="SciDB host for connection", dest="host",
                        default="http://localhost:8080")
    parser.add_argument('-ZoneCSV', required=False, help='CSV path for options 2 and 3', dest='zoneCSV')
    parser.add_argument('-Binary', required=False, help='Binary path for tmp files for options 4 and 5', dest='binary')
    return parser


if __name__ == '__main__':
    """
        Entry point for SciDB_ZonalStats_CL
        This is a command line script for conducting zonal stats    
    """
    args = argument_parser().parse_args()
    if CheckFiles(args.Shapefile, args.Raster):
        if args.host == "NoSHIM":
            import scidb

            sdb = scidb.iquery()
            print("No SHIM")
        else:
            from scidbpy import connect

            sdb = connect(args.host)
        ZonalStats(args.Runs, args.Shapefile, args.Raster, args.SciArray, sdb, args.mode, args.CSV, args.verbose, args.zoneCSV, args.binary)
    else:
        print(args.Shapefile, args.Raster)
