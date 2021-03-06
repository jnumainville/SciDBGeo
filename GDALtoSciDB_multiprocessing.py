import multiprocessing as mp
import itertools, timeit
import numpy as np
import math, csv, os, gc
from osgeo import gdal
from gdalconst import GA_ReadOnly
from collections import OrderedDict


class RasterReader(object):

    def __init__(self, RasterPath, scidbArray, attribute, chunksize, tiles):
        """
        Initialize the class RasterReader

        Input:
            RasterPath = Input file path for the raster
            scidbArray = Name of the destination array
            attribute = Name of the attribute(s) for the destination array
            chunksize = Chunk size for the destination array
            tiles = Size in rows of the read window

        Output:
            A initialization of the RasterReader class
        """

        self.width, self.height, self.datatype, self.numbands = self.GetRasterDimensions(RasterPath)
        self.AttributeNames = attribute
        self.AttributeString, self.RasterArrayShape = self.RasterShapeLogic(attribute)
        self.RasterMetadata = self.CreateArrayMetadata(scidbArray, self.width, self.height, chunksize, tiles,
                                                       self.AttributeString, self.numbands)
        self.CreateDestinationArray(scidbArray, self.height, self.width, chunksize)

    def RasterShapeLogic(self, attributeNames):
        """
        This function will provide the logic for determining the shape of the raster

        Input:
            attributeNames = Name of the attribute(s) for the destination array

        Output:
            A tuple in the following format:
                (attribute string, type of array)
        """
        if len(attributeNames) >= 1 and self.numbands > 1:
            # Each pixel value will be a new attribute
            attributes = ["%s:%s" % (i, self.datatype) for i in attributeNames]
            if len(attributeNames) == 1:
                attString = " ".join(attributes)
                arrayType = 3

            else:
                attString = ", ".join(attributes)
                arrayType = 2

        else:
            # Each pixel value will be a new band and we must loop.
            # Not checking for 2 attribute names that will crash
            attString = "%s:%s" % (attributeNames[0], self.datatype)
            arrayType = 1

        return attString, arrayType

    def GetMetadata(self, scidbInstances, rasterFilePath, outPath, loadPath, band):
        """
        Generator for the class, iteratively yielding

        Input: 
            scidbInstances = SciDB Instance IDs
            rasterFilePath = Absolute Path to the GeoTiff
            outPath = The out path for the SciDB instance
            loadPath = The path to load from for the SciDB instance
            band = The band number to get the data from

        Output:
            Yield a tuple in the following format:
                (raster metadata for a specific key, running process, filepath to raster, dictionary out, dictionary for
                 loading, band)
        """

        for key, process, filepath, outDirectory, loadDirectory, band in zip(self.RasterMetadata.keys(),
                                                                             itertools.cycle(scidbInstances),
                                                                             itertools.repeat(rasterFilePath),
                                                                             itertools.repeat(outPath),
                                                                             itertools.repeat(loadPath),
                                                                             itertools.repeat(band)):
            yield self.RasterMetadata[key], process, filepath, outDirectory, loadDirectory, band

    def GetRasterDimensions(self, thePath):
        """
        Function gets the dimensions of the raster file

        Input:
            thePath = Input file path for the raster

        Output:
            A tuple in the following format:
             (width of the raster, height of the raster, data type of the raster, number of bands of the raster)
        """
        raster = gdal.Open(thePath, GA_ReadOnly)

        # Extract the data type
        rasterValueDataType = None
        if raster.RasterCount > 1:
            rArray = raster.ReadAsArray(xoff=0, yoff=0, xsize=1, ysize=1)
            rasterValueDataType = rArray[0][0].dtype

        elif raster.RasterCount == 1:
            rasterValueDataType = raster.ReadAsArray(xoff=0, yoff=0, xsize=1, ysize=1)[0].dtype

        if rasterValueDataType == 'float32':
            rasterValueDataType = 'float'
        numbands = raster.RasterCount
        width = raster.RasterXSize
        height = raster.RasterYSize
        print(rasterValueDataType)

        del raster

        return width, height, rasterValueDataType, numbands

    def CreateDestinationArray(self, rasterArrayName, height, width, chunk):
        """
        Function creates the final destination array.
        Updated to handle 3D arrays.

        Input:
            rasterArrayName = Name of the destination array
            height = The height of the raster
            width = The width of the raster
            chunk = The size of the chunks

        Output:
            None
        """

        import scidb
        sdb = scidb.iquery()

        # Attempt to create array, removing the previous one if it exists
        if self.RasterArrayShape <= 2:
            myQuery = "create array %s <%s> [y=0:%s,%s,0; x=0:%s,%s,0]" % (rasterArrayName, self.AttributeString,
                                                                           height - 1, chunk, width - 1, chunk)
        else:
            myQuery = "create array %s <%s> [band=0:%s,1,0; y=0:%s,%s,0; x=0:%s,%s,0]" % (rasterArrayName,
                                                                                          self.AttributeString,
                                                                                          self.numbands - 1, height - 1,
                                                                                          chunk, width - 1, chunk)

        try:
            sdb.query(myQuery)
        except:
            print("*****  Array %s already exists. Removing ****" % rasterArrayName)
            sdb.query("remove(%s)" % rasterArrayName)
            print("here2")
            sdb.query(myQuery)
            print("here3")

        del sdb

    def CreateArrayMetadata(self, theArray, width, height, chunk=1000, tiles=1, attribute='value', band=1):
        """
        This function gathers all the metadata necessary from an array

        Input:
            theArray = Name of the destination array
            width = Width of the raster
            height = Height of the raster
            chunk = Chunk size for the destination array
            tiles = Size in rows of the read window
            attribute = Attribute string
            band = The number of bands

        Output:
            The raster reads
        """

        RasterReads = OrderedDict()
        rowMax = 0

        for y_version, yOffSet in enumerate(range(0, height, chunk)):
            # Adding +y_version will stagger the offset
            rowsRemaining = height - (y_version * chunk + y_version)

            # If this is not a short read, then read the correct size.
            if rowsRemaining > chunk:
                rowsRemaining = chunk

            for x_version, xOffSet in enumerate(range(0, width, chunk * tiles)):
                version_num = rowMax + x_version
                # Adding +x_version will stagger the offset
                columnsRemaining = width - (x_version * chunk * tiles + x_version)

                # If this is not a short read, then read the correct size.
                if columnsRemaining > chunk * tiles:
                    columnsRemaining = chunk * tiles

                RasterReads[str(version_num)] = OrderedDict([("xOffSet", xOffSet), ("yOffSet", yOffSet),
                                                             ("xWindow", columnsRemaining), ("yWindow", rowsRemaining),
                                                             ("version", version_num),
                                                             ("array_type", self.RasterArrayShape),
                                                             ("attribute", attribute), ("scidbArray", theArray),
                                                             ("y_version", y_version), ("chunk", chunk),
                                                             ("bands", band)])

            rowMax += math.ceil(width / (chunk * tiles))

        for r in RasterReads.keys():
            RasterReads[r]["loops"] = len(RasterReads)

            if len(RasterReads[r]["attribute"].split(" ")) < RasterReads[r]["bands"]:
                RasterReads[r]["iterate"] = RasterReads[r]["bands"]
            else:
                RasterReads[r]["iterate"] = 0

        return RasterReads


def GlobalRasterLoading(sdb, theMetadata, theDict):
    """
    This function is built in an effort to separate the redimensioning from the loading.
    Redimensioning might affect the performance of the load if they run concurrently

    Input:
        sdb = SciDB instance
        theMetadata = Class with raster information
        theDict = Dictionary containing timing information for MetaData

    Output:
        Original theDict with redimension information attached
    """

    theData = theMetadata.RasterMetadata
    for theKey in theMetadata.RasterMetadata.keys():
        # Time the redimensioning
        start = timeit.default_timer()
        tempArray = "temprast_%s" % (theData[theKey]['version'])
        RedimensionAndInsertArray(sdb, tempArray, theData[theKey]['scidbArray'], theData[theKey]['array_type'],
                                  theData[theKey]['xOffSet'], theData[theKey]['yOffSet'])
        stop = timeit.default_timer()
        redimensionTime = stop - start

        RemoveTempArray(sdb, tempArray)
        print("Inserted version %s of %s" % (theData[theKey]['version'], theData[theKey]["Loops"]))
        dataLoadingTime = (redimensionTime * theData[theKey]["Loops"]) / 60
        if theData[theKey]['version'] == 0:
            print("Estimated time for redimensioning the dataset in minutes %s: RedimensionTime: %s seconds" %
                  (dataLoadingTime, redimensionTime))
        if theData[theKey]['version'] > 1:
            versions = sdb.versions(theData[theKey]['scidbArray'])
            # Attempt to remove excess versions
            if len(versions) > 1:
                print("Versions you could remove: %s" % versions)
                for v in versions[:-1]:
                    try:
                        sdb.query("remove_versions(%s, %s)" % (theData[theKey]['scidbArray'], v))
                    except:
                        print("Couldn't remove version %s from array %s" % (v, theData[theKey]['scidbArray']))

        # Add the redimension time to the TimeDictionary
        theDict[str(theData[theKey]['version'])]['redimensionTime'] = redimensionTime

    return theDict


def GDALReader(inParams):
    """
    This is the main worker function.
    Split up Loading and Redimensioning. Only Loading is multiprocessing

    Input:
        inParams = A tuple or list containing the following:
            theMetadata = Metadata for the reading
            theInstance = Instance to read from
            theRasterPath = Path to the raster to read
            theSciDBOutPath = Out path for SciDB processing
            theSciDBLoadPath = Load path for SciDB processing
            bandIndex = Index of the band to process on

    Output:
        A tuple in the following format:
            (metadata for the raster, write time for the raster, load time for the raster)
    """
    theMetadata = inParams[0]
    theInstance = inParams[1]
    theRasterPath = inParams[2]
    theSciDBOutPath = inParams[3]
    theSciDBLoadPath = inParams[4]
    bandIndex = inParams[5]

    from scidb import iquery, Statements
    sdb = iquery()
    sdb_statements = Statements(sdb)

    tempArray = "temprast_%s" % (theMetadata['version'])
    rasterBinaryFilePath = "%s/%s.sdbbin" % (theSciDBOutPath, tempArray)
    rasterBinaryLoadPath = "%s/%s.sdbbin" % (theSciDBLoadPath, tempArray)

    print("xoffset: %s, yOffSet: %s, xWindow: %s, yWindow: %s " % (theMetadata['xOffSet'], theMetadata['yOffSet'],
                                                                   theMetadata['xWindow'], theMetadata['yWindow']))

    raster = gdal.Open(theRasterPath, GA_ReadOnly)
    if bandIndex:
        # This code is for multibanded arrays, with z (band) dimension.
        print("**** Reading band %s" % bandIndex)
        band = raster.GetRasterBand(bandIndex)
        array = band.ReadAsArray(xoff=theMetadata['xOffSet'], yoff=theMetadata['yOffSet'],
                                 win_xsize=theMetadata['xWindow'], win_ysize=theMetadata['yWindow'])
        rasterBinaryFilePath = "%s/band%s_%s.sdbbin" % (theSciDBOutPath, bandIndex, tempArray)
        rasterBinaryLoadPath = "%s/band%s_%s.sdbbin" % (theSciDBLoadPath, bandIndex, tempArray)
        tempArray = "temprast_band%s_%s" % (bandIndex, theMetadata['version'])
    else:
        array = raster.ReadAsArray(xoff=theMetadata['xOffSet'], yoff=theMetadata['yOffSet'],
                                   xsize=theMetadata['xWindow'], ysize=theMetadata['yWindow'])

    # Time the array write
    start = timeit.default_timer()
    WriteArray(array, rasterBinaryFilePath, theMetadata['array_type'], theMetadata['attribute'], bandIndex)
    stop = timeit.default_timer()
    writeTime = stop - start

    # Process depending on array type
    if theMetadata['array_type'] == 2:
        items = ["%s:%s" % (attribute.split(":")[0].strip() + "1", attribute.split(":")[1].strip()) for attribute in
                 theMetadata['attribute'].split(",")]
        pseudoAttributes = ", ".join(items)
    else:
        pseudoAttributes = "%s:%s" % (theMetadata['attribute'].split(":")[0].strip() + "1",
                                      theMetadata['attribute'].split(":")[1].strip())

    os.chmod(rasterBinaryFilePath, 0o755)
    # Support multiple attributes or 2D and 3D arrays
    sdb_statements.CreateLoadArray(tempArray, theMetadata['attribute'], theMetadata['array_type'])
    start = timeit.default_timer()

    if sdb_statements.LoadOneDimensionalArray(theInstance, tempArray, pseudoAttributes, theMetadata['array_type'],
                                              rasterBinaryLoadPath):
        stop = timeit.default_timer()
        loadTime = stop - start

        dataLoadingTime = ((writeTime + loadTime) * theMetadata["loops"]) / 60
        if theMetadata['version'] == 0:
            print("Estimated time for loading in minutes %s: WriteTime: %s, LoadTime: %s" % (dataLoadingTime, writeTime,
                                                                                             loadTime))

        # Clean up
        gc.collect()

        RedimensionAndInsertArray(sdb, tempArray, theMetadata['scidbArray'], theMetadata['array_type'],
                                  theMetadata['xOffSet'], theMetadata['yOffSet'])

        return theMetadata['version'], writeTime, loadTime

    else:
        print("Error Loading")
        return theMetadata['version'], -999, -999


def ArrayDimension(anyArray):
    """
    Return the number of rows and columns for 2D or 3D array

    Input:
        anyArray = The input array to measure

    Output:
        The shape of the array
    """
    if anyArray.ndim == 2:
        return anyArray.shape
    else:
        return anyArray.shape[1:]


def WriteArray(theArray, binaryFilePath, arrayType, attributeName='value', bandID=0):
    """
    This function uses numpy tricks to write a numpy array in binary format with indices

    Input:
        theArray = Array to write
        binaryFilePath = File path to write to
        arrayType = Type of the array
        attributeName = Attribute to write
        bandID = ID of band to write

    Output:
        None
    """
    print("Writing %s" % binaryFilePath)
    col, row = ArrayDimension(theArray)
    with open(binaryFilePath, 'wb') as fileout:

        # Oneliner that creates the column index. Pull out [y for y in range(col)] to see how it works
        column_index = np.array(np.repeat([y for y in range(col)], row), dtype=np.dtype('int64'))

        # Oneliner that creates the row index. Pull out the nested loop first: [x for x in range(row)]
        # Then pull the full list comprehension: [[x for x in range(row)] for i in range(col)]
        row_index = np.array(np.concatenate([[x for x in range(row)] for i in range(col)]), dtype=np.dtype('int64'))

        if arrayType == 3:
            # Raster has multiple attributes, with a z (band) dimension
            # Raster band reader for GDAL begins as 1, but the dimension range is 0:numbands-1
            band_index = np.array([bandID - 1 for c in column_index])

            np.core.records.fromarrays([band_index, column_index, row_index, theArray.ravel()],
                                       dtype=[('band', 'int64'), ('x', 'int64'), ('y', 'int64'),
                                              (attributeName.split(":")[0],
                                               theArray.dtype)]).ravel().tofile(binaryFilePath)

        elif arrayType == 2:
            # Raster has multiple attributes, but it is 2D
            # Making a list of attribute names with data types

            attributesList = [('x', 'int64'), ('y', 'int64')]
            for dim, name in enumerate(attributeName.split(",")):
                attName, attType = name.split(":")
                attributesList.append((attName.strip(), attType.strip()))

            # Making a list of numpy arrays
            arrayList = [column_index, row_index]
            for attArray in np.array_split(theArray, dim + 1, axis=0):
                arrayList.append(attArray.reshape(attArray.shape[1:]).ravel())

            np.core.records.fromarrays(arrayList, dtype=attributesList).ravel().tofile(binaryFilePath)

        elif arrayType == 1:
            # A single band GeoTiff
            np.core.records.fromarrays([column_index, row_index, theArray.ravel()],
                                       dtype=[('x', 'int64'), ('y', 'int64'),
                                              (attributeName, theArray.dtype)]).ravel().tofile(binaryFilePath)

        else:
            print("ERROR")

    del column_index, row_index, theArray


def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv

    Input:
        filePath = Path to write to
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


def CreateLoadArray(sdb, tempRastName, attribute_name, rasterArrayType):
    """
    Create the loading array

    Input:
        sdb = Instance to create array in
        tempRastName = Name of the array to create
        attribute_name = Attribute to create with
        rasterArrayType = Type of the array to create

    Output:
        None
    """
    theQuery = None
    if rasterArrayType <= 2:
        theQuery = "create array %s <y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)
    elif rasterArrayType == 3:
        theQuery = "create array %s <z1:int64, y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)

    try:
        sdb.query(theQuery)
    except:
        # Silently deleting temp arrays
        sdb.query("remove(%s)" % tempRastName)
        sdb.query(theQuery)


def RedimensionAndInsertArray(sdb, tempArray, SciDBArray, RasterArrayShape, xOffSet, yOffSet):
    """
    Function for redimension and inserting data from the temporary array into the destination array

    Input:
        sdb = SciDB instance to redimension and insert into
        tempArray = Temporary array to insert into
        SciDBArray = Array data
        RasterArrayShape = Shape of the array to redimension and insert
        xOffSet = Offset in x for redimension
        yOffSet = Offset in y for redimension

    Output:
        None
    """
    query = None
    if RasterArrayShape <= 2:
        # Raster has one or more attributes but the array is 2D
        query = "insert(redimension(apply( %s, y, y1+%s, x, x1+%s ), %s ), %s)" % (tempArray, yOffSet, xOffSet,
                                                                                   SciDBArray, SciDBArray)

    elif RasterArrayShape == 3:
        # Raster has multiple attributes, with a z (band) dimension in 3D
        query = "insert(redimension(apply(%s, band, z1, y, y1+%s, x, x1+%s ), %s ), %s)" % (tempArray, yOffSet,
                                                                                            xOffSet, SciDBArray,
                                                                                            SciDBArray)

    try:
        sdb.query(query)
    except:
        print("Failing on inserting data into array")
        print(query)


def RemoveTempArray(sdb, tempRastName):
    """
    Remove the temporary array

    Input:
        sdb = SciDB instance to remove from
        tempRastName = Temporary raster to remove

    Output:
        None
    """
    sdb.query("remove(%s)" % tempRastName)


def GetNumberofSciDBInstances():
    """
    Get the number of running SciDB instances

    Input:
        None

    Output:
        Number of running SciDB instances
    """

    import scidb
    sdb = scidb.iquery()

    query = sdb.queryAFL("list('instances')")
    numInstances = len(query.splitlines()) - 1
    numInstances = list(range(numInstances))
    return numInstances


def MultiProcessLoading(Rasters, rasterFilePath, SciDBOutPath, SciDBLoadPath):
    """
    This function creates the pool based upon the number of SciDB instances and the generates the parameters for each
    Python instance

    Input:
        Rasters = Class with raster information
        rasterFilePath = Absolute path to the raster
        SciDBOutPath = The out path for the SciDB instance
        SciDBLoadPath = The path to load from for the SciDB instance

    Output:
        Dictionary containing the following:
            Dictionaries containing the following keys:
                version = version from raster
                writeTime = write time from raster metadata
                loadTime = load time from raster metadata
    """
    SciDBInstances = GetNumberofSciDBInstances()
    pool = mp.Pool(len(SciDBInstances), maxtasksperchild=1)

    aKey = list(Rasters.RasterMetadata.keys())[0]
    results = None
    try:
        if not Rasters.RasterMetadata[aKey]['iterate']:
            # Read in metadata from the rasters
            results = pool.imap(GDALReader, (r for r in Rasters.GetMetadata(SciDBInstances, rasterFilePath,
                                                                            SciDBOutPath, SciDBLoadPath, 0)))
        else:
            # Loop though the metadata reads
            for bandNum in range(1, Rasters.RasterMetadata[aKey]['iterate'] + 1):
                results = pool.imap(GDALReader, (r for r in Rasters.GetMetadata(SciDBInstances, rasterFilePath,
                                                                                SciDBOutPath, SciDBLoadPath, bandNum)))
    except:
        print(mp.get_logger())

    timeDictionary = {str(i[0]): {"version": i[0], "writeTime": i[1], "loadTime": i[2]} for i in results}
    return timeDictionary


def argument_parser():
    """
    Parse arguments and return Arguments

    Input:
        None

    Output:
        The argument parser
    """
    import argparse

    parser = argparse.ArgumentParser(description="multiprocessing module for loading GDAL read data into SciDB with "
                                                 "multiple instances")
    # If host = NoSHIM, then use the cmd iquery
    # parser.add_argument("-Host", required=True, help="SciDB host for connection", dest="host", default="localhost")
    parser.add_argument("-r", required=True, help="Input file path for the raster", dest="rasterPath")
    parser.add_argument("-a", required=True, help="Name of the destination array", dest="arrayName")
    parser.add_argument("-n", required=True, nargs='*', help="Name of the attribute(s) for the destination array",
                        dest="attributes", default="value")
    parser.add_argument("-t", required=False, type=int, help="Size in rows of the read window, default: 8",
                        dest="tiles", default=8)
    parser.add_argument("-c", required=False, type=int, help="Chunk size for the destination array, default: 1,000",
                        dest="chunk", default=1000)
    parser.add_argument("-o", required=False, type=int,
                        help="Chunk overlap size. Adding overlap increases data loading time. default: 0",
                        dest="overlap", default=0)
    parser.add_argument("-TempOut", required=False, default='/home/scidb/scidb_data/0/0', dest='OutPath')
    parser.add_argument("-SciDBLoadPath", required=False, default='/home/scidb/scidb_data/0/0', dest='SciDBLoadPath')
    parser.add_argument("-csv", required=False, help="Create CSV file", dest="csv", default="None")
    parser.add_argument("-p", required=False, help="Parallel Redimensioning", dest="parallel", default="None")

    return parser


if __name__ == '__main__':
    """
        Entry point for GDALtoSciDB_multiprocessing
        This script loads given datasets into a SciDB cluster using 1 or more SciDB instances
    """
    args = argument_parser().parse_args()
    start = timeit.default_timer()

    # Read in and store raster information with given arguments
    RasterInformation = RasterReader(args.rasterPath, args.arrayName, args.attributes, args.chunk, args.tiles)

    # Load the data in parallel if requested
    timeDictionary = MultiProcessLoading(RasterInformation, args.rasterPath, args.OutPath, args.SciDBLoadPath)
    allTimesDictionary = None
    if not args.parallel:
        allTimesDictionary = GlobalRasterLoading(args.host, RasterInformation, timeDictionary)

    if args.csv and allTimesDictionary != None: 
        WriteFile(args.csv, allTimesDictionary)



    stop = timeit.default_timer()

    print("Finished. Time to complete %s minutes" % ((stop-start)/60))
