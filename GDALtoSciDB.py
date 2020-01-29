import numpy as np
from scidbpy import connect
import os


def LoadOverlapArray():
    pass


def CreateDestinationArray(sdb, rasterArrayName, attribute, rasterValueDataType, height, width, chunk):
    """
    Create the destination array

    Input:
    sdb = connection to the SciDB host
    rasterArrayName = Name of the destination array
    attribute = Name of the destination array
    rasterValueDataType = Data type of the raster array
    height = Height of the raster
    width = Width of the raster
    chunk = Chunk size for the destination array

    Output:
    None
    """
    try:
        sdb.query("create array %s <%s:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" % (rasterArrayName, attribute,
                                                                          rasterValueDataType, height - 1, chunk,
                                                                          width - 1, chunk))
    except:
        print("Array %s already exists. Removing" % (rasterArrayName))
        sdb.query("remove(%s)" % (rasterArrayName))
        sdb.query("create array %s <%s:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" % (rasterArrayName, attribute,
                                                                          rasterValueDataType, height - 1, chunk,
                                                                          width - 1, chunk))


def CreateLoadArray(sdb, tempRastName, attribute_name, rasterValueDataType):
    """
    Create the loading 1D array
    Input:
    sdb = Connection to the SciDB host
    tempRastName = Name of the temporary raster
    attribute_name = Name of the destination array
    rasterValueDataType = Data type of the raster array

    Output:
    None
    """
    try:
        sdb.query("create array %s <y1:int64, x1:int64, %s:%s> [xy=0:*,?,?]" % (tempRastName, attribute_name,
                                                                                rasterValueDataType))
    except:
        # Silently deleting temp arrays
        sdb.query("remove(%s)" % tempRastName)
        sdb.query("create array %s <y1:int64, x1:int64, %s:%s> [xy=0:*,?,?]" % (tempRastName, attribute_name,
                                                                                rasterValueDataType))


def ArrayMetadata(width, height, chunk, tiles):
    """
    This function gathers all the metadata necessary

    Input:
    width = Width of the raster
    height = Height of the raster
    chunk = Chunk size for the destination array
    tiles = Size in rows of the read window

    Output:
    A dictionary of raster reads
    """
    import math
    from collections import OrderedDict
    rasterReads = OrderedDict()
    rowMax = 0

    for y_version, yOffSet in enumerate(range(0, height, chunk)):
        rowsRemaining = height - y_version * chunk

        # If this is not a short read, then read the correct size.
        if rowsRemaining > chunk * tiles: rowsRemaining = chunk

        for x_version, xOffSet in enumerate(range(0, width, chunk * tiles)):
            version_num = rowMax + x_version
            columnsRemaining = width - x_version * chunk * tiles

            # If this is not a short read, then read the correct size.
            if columnsRemaining > chunk * tiles: columnsRemaining = chunk * tiles

            rasterReads[str(version_num)] = OrderedDict([("xOffSet", xOffSet), ("yOffSet", yOffSet),
                                                         ("xWindow", columnsRemaining), ("yWindow", rowsRemaining)])

        rowMax += math.ceil(width / (chunk * tiles))

    return rasterReads


def ReadGDALFile(sdb, rasterArrayName, rasterPath, tempOutDirectory, tempSciDBLoad, attribute="value", chunk=1000,
                 tiles=1, overlap=0):
    """
    Read a GDAL file

    Input:
    sdb = Connection to the SciDB host
    rasterArrayName = Name of the destination array
    rasterPath = Input file path for the raster
    tempOutDirectory = Temporary out path
    tempSciDBLoad = Temporary SciDB load path
    attribute = Name of the destination array
    chunk = Chunk size for the destination array
    tiles = Size in rows of the read window
    overlap = Chunk overlap size

    Output:
    None
    """

    from osgeo import gdal
    from gdalconst import GA_ReadOnly
    import timeit

    raster = gdal.Open(rasterPath, GA_ReadOnly)
    width = raster.RasterXSize
    height = raster.RasterYSize
    RasterMetadata = ArrayMetadata(width, height, chunk, tiles)
    print("Width: %s, Height: %s, TotalPixels: %s" % (width, height, width * height))

    for scidbVersion, k in enumerate(RasterMetadata.keys()):
        totalstart = timeit.default_timer()
        tempRastName = 'temprast_%s' % (scidbVersion)
        csvPath = '%s/%s.sdbbin' % (tempOutDirectory, tempRastName)
        rArray = raster.ReadAsArray(xoff=RasterMetadata[k]["xOffSet"], yoff=RasterMetadata[k]["yOffSet"],
                                    xsize=RasterMetadata[k]["xWindow"], ysize=RasterMetadata[k]["yWindow"])

        # Create final destination array
        rasterValueDataType = rArray.dtype
        if scidbVersion == 0: CreateDestinationArray(sdb, rasterArrayName, attribute, rasterValueDataType, height,
                                                     width, chunk)

        # Write the array to a csv
        start = timeit.default_timer()
        WriteArray(rArray, csvPath)
        stop = timeit.default_timer()
        writeBinaryTime = stop - start
        os.chmod(csvPath, 0o755)

        # Create the array, which will hold the read in data. Y/Column and X/Row coordinates are different on purpose
        CreateLoadArray(sdb, tempRastName, attribute, rasterValueDataType)

        # Time the loading of binary file
        start = timeit.default_timer()
        binaryLoadPath = '%s/%s.sdbbin' % (tempSciDBLoad, tempRastName)
        sdb.query("load(%s,'%s', -2, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, rasterValueDataType))
        stop = timeit.default_timer()
        loadBinaryTime = stop - start

        # Time the redimension
        start = timeit.default_timer()
        sdb.query("insert(redimension(apply( {A}, y, y1+{yOffSet}, x, x1+{xOffSet} ), {B} ), {B})", A=tempRastName,
                  B=rasterArrayName, yOffSet=RasterMetadata[k]["yOffSet"], xOffSet=RasterMetadata[k]["xOffSet"])
        stop = timeit.default_timer()
        redimensionArrayTime = stop - start

        # Clean up the temporary files
        if scidbVersion >= 1: CleanUpTemp(sdb, rasterArrayName, scidbVersion, csvPath, tempRastName)

        totalstop = timeit.default_timer()

        print('Completed %s of %s' % (scidbVersion + 1, len(RasterMetadata)))

        if scidbVersion == 0:
            NumberOfIterations = len(RasterMetadata)
            totalTime = totalstop - totalstart
            print('Took %s seconds to complete' % (totalTime))
            print("Writing time: %s, Loading time: %s, Redimension time: %s " % (
                writeBinaryTime, loadBinaryTime, redimensionArrayTime))
            print('Estimated time to load (%s) = time %s * loop %s' % (
                totalTime * NumberOfIterations, totalTime, NumberOfIterations))
            print('Estimated time in hours: %s ' % (totalTime * NumberOfIterations / 60 / 60))
            CleanUpTemp(sdb, rasterArrayName, scidbVersion, csvPath, tempRastName)


def WriteArray(theArray, csvPath):
    """
    Write an array to the given path

    Input:
    theArray = the array to write
    csvPath = the path where the array will be written

    Output:
    None
    """
    col, row = theArray.shape
    with open(csvPath, 'wb') as fileout:
        thecolumns = [y for y in range(col)]
        column_index = np.array(np.repeat(thecolumns, row), dtype=np.dtype('int64'))

        therows = [x for x in range(row)]
        allrows = [therows for i in range(col)]
        row_index = np.array(np.concatenate(allrows), dtype=np.dtype('int64'))

        values = theArray.ravel()

        dataset = np.core.records.fromarrays([column_index, row_index, values], names='y,x,value',
                                             dtype='int64, int64, uint8')

        fileout.write(dataset.ravel().tobytes())


def WriteMultiDimensionalArray(rArray, csvPath):
    '''
    Write the multidimensional array as binary

    Input:
    rArray = the multidimensional array to write
    csvPath = the path where the array will be written

    Output:
    The width and height of the written array
    '''
    with open(csvPath, 'wb') as fileout:
        arrayHeight, arrayWidth = rArray.shape
        it = np.nditer(rArray, flags=['multi_index'], op_flags=['readonly'])
        byteValues = []
        for counter, pixel in enumerate(it):
            col, row = it.multi_index
            indexvalue = np.array([col, row], dtype=np.dtype('int64'))
            byteValues.append(indexvalue.tobytes())
            byteValues.append(it.value.tobytes())
        bytesTile = b"".join(byteValues)
        fileout.write(bytesTile)
    return arrayHeight, arrayWidth


def CleanUpTemp(sdb, rasterArrayName, version_num, csvPath, tempRastName):
    """
    Remove all temporary files

    Input:
    sdb = connection to the SciDB host
    rasterArrayName = Name of the destination array
    version_num = version of SciDB
    csvPath = the path where the array will be written
    tempRastName = Name of the temporary raster

    Output:
    None
    """
    if version_num > 0:
        sdb.query("remove_versions(%s, %s)" % (rasterArrayName, version_num))
    sdb.query("remove(%s)" % tempRastName)
    os.remove(csvPath)


def argument_parser():
    """
    Parse arguments and return the argument parser

    Input:
    None

    Output:
    Parser for arguments to program
    """
    import argparse

    parser = argparse.ArgumentParser(description="Load GDAL dataset into SciDB")
    parser.add_argument("-Host", required=False, help="SciDB host for connection", dest="host",
                        default="http://localhost:8080")
    parser.add_argument("-RasterPath", required=True, help="Input file path for the raster", dest="rasterPath")
    parser.add_argument("-SciDBArray", required=True, help="Name of the destination array", dest="SciArray")
    parser.add_argument("-AttributeNames", required=True, help="Name of the destination array", dest="attributes",
                        default="value")
    parser.add_argument("-Tiles", required=False, type=int, help="Size in rows of the read window, default: 1",
                        dest="tiles", default=1)
    parser.add_argument("-Chunk", required=False, type=int, help="Chunk size for the destination array, default: 1,000",
                        dest="chunk", default=1000)
    parser.add_argument("-Overlap", required=False, type=int,
                        help="Chunk overlap size. Adding overlap increases data loading time. defalt: 0",
                        dest="overlap", default=0)
    parser.add_argument("-TempOut", required=False, default='/home/scidb/scidb_data/0/0', dest='OutPath', )
    parser.add_argument("-SciDBLoadPath", required=False, default='/home/scidb/scidb_data/0/0', dest='SciDBLoadPath')
    parser.add_argument("-CSV", required=False, help="Create CSV file", dest="csv", default="None")

    return parser


if __name__ == '__main__':
    args = argument_parser().parse_args()
    if os.path.exists(args.rasterPath):
        sdb = connect(args.host)
        if sdb:
            ReadGDALFile(sdb, args.SciArray, args.rasterPath, args.OutPath, args.SciDBLoadPath, args.attributes,
                         args.chunk, args.tiles, args.overlap)
        else:
            print('Not Valid connection: %s' % args.Host)
    else:
        print("Not a valid Raster Path")
