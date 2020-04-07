import timeit, itertools, math
import multiprocessing as mp
import numpy as np
from collections import OrderedDict
from osgeo import gdal
from gdalconst import GA_ReadOnly


class RasterLoader(object):

    def __init__(self, RasterPath, scidbArray, attribute, chunksize, dataStorePath, tiles=None, maxPixels=10000000,
                 yOffSet=0, overlap=0):
        """
        Initialize the class RasterLoader

        Input:
            RasterPath = Path to the raster
            scidbArray = Array from SciDB
            attribute = The attribute to process
            chunksize = The size of the chunk
            dataStorePath = The path of the data store
            tiles = The tiles to write over
            maxPixels = The maximum number of pixels
            yOffSet = The offset for y
            overlap = The overlap of the rasters

        Output:
            A RasterLoader object
        """

        self.width, self.height, self.datatype, self.numbands = self.GetRasterDimensions(RasterPath)
        self.AttributeNames = attribute
        self.GetSciDBInstances()
        self.AttributeString, self.RasterArrayShape = self.RasterShapeLogic(attribute)
        self.chunksize = chunksize
        self.overlap = overlap
        self.dataStorePath = dataStorePath
        self.rasterPath = RasterPath
        hdataset = np.arange(self.height)

        # Build the metadata and reading data from parameters
        self.RasterMetadata = {node: {"node": node, "y_min": min(heightRange), "y_max": max(heightRange), "height":
            len(heightRange), "width": self.width, "datastore": self.dataStorePath, "filepath": self.rasterPath,
                                      "attribute": self.AttributeString, "array_shape": self.RasterArrayShape,
                                      "destination_array": scidbArray} for node, heightRange in
                               enumerate(np.array_split(hdataset, self.SciDB_Instances))}

        self.RasterReadingData = self.CreateArrayMetadata(scidbArray, widthMax=self.width, heightMax=self.height,
                                                          widthMin=0, heightMin=yOffSet, chunk=self.chunksize,
                                                          tiles=tiles, maxPixels=maxPixels,
                                                          attribute=self.AttributeString, band=self.numbands)

        self.ParalleReadingData = self.ConfigureParallelReads()

    def RasterShapeLogic(self, attributeNames):
        """
        This function will provide the logic for determining the shape of the raster
        The attributeNames variable is a list of attributes

        Input:
            attributeNames = The name of the attributes to process over

        Output:
            A tuple in the following format:
                (attribute string, array type)
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

    def GetSciDBInstances(self):
        """
        Setup the SciDB instances for the class

        Input:
            None

        Output:
            None
        """
        from scidb import iquery
        sdb = iquery()
        query = sdb.queryAFL("list('instances')")
        self.SciDB_Instances = len(query.splitlines()) - 1

    def GetMetadata(self, scidbInstances, rasterFilePath, outPath, loadPath, band):
        """
        Generator for the class, iteratively yielding

        Input: 
            scidbInstance = SciDB Instance IDs
            rasterFilePath = Absolute Path to the GeoTiff
            outPath = Path to write to
            loadPath = Path to load from
            band = Band to use

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
            thePath = The path to the file

        Output:
            A tuple in the following format:
                (width of the raster, height of the raster, date type of the raster, number of bands in the raster)
        """
        if isinstance(thePath, str):
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

            del raster

        elif type(thePath).__module__ == 'numpy':
            rasterValueDataType = thePath.dtype
            height, width = thePath.shape
            numbands = 1

        return width, height, rasterValueDataType, numbands

    def CreateDestinationArray(self, rasterArrayName, height, width, chunk, overlap):
        """
        Function creates the final destination array.
        Updated to handle 3D arrays.

        Input:
            rasterArrayName = The name of the array to create
            height = The height of the array to create
            width = The width of the array to create
            chunk = The chunk to create with
            overlap = The overlap of the array

        Output:
            None
        """

        import scidb
        sdb = scidb.iquery()

        if self.RasterArrayShape <= 2:
            myQuery = "create array %s <%s> [y=0:%s,%s,%s; x=0:%s,%s,%s]" % (rasterArrayName, self.AttributeString,
                                                                             height - 1, chunk, overlap, width - 1,
                                                                             chunk,
                                                                             overlap)
        else:
            # Add in bands to the query
            myQuery = "create array %s <%s> [band=0:%s,1,%s; y=0:%s,%s,0; x=0:%s,%s,%s]" % (rasterArrayName,
                                                                                            self.AttributeString,
                                                                                            self.numbands - 1,
                                                                                            height - 1, chunk, overlap,
                                                                                            width - 1, chunk, overlap)

        try:
            sdb.query(myQuery)
        except:
            # Remove the array if it already exists then rerun the query
            print("*****  Array %s already exists. Removing ****" % rasterArrayName)
            sdb.query("remove(%s)" % rasterArrayName)
            sdb.query(myQuery)

        del sdb

    def CreateLoadArray(self, tempRastName, attribute_name, rasterArrayType):
        """
        Create the loading array

        Input:
            tempRastName = The names of the raster
            attribute_name = The attribute to create on
            rasterArrayType = The type of the raster

        Output:
            None
        """

        import scidb
        sdb = scidb.iquery()

        if rasterArrayType <= 2:
            theQuery = "create array %s <y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)
        elif rasterArrayType == 3:
            theQuery = "create array %s <z1:int64, y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName,
                                                                                            attribute_name)

        try:
            sdb.query(theQuery)
        except:
            # Silently deleting temp arrays
            sdb.query("remove(%s)" % tempRastName)
            sdb.query(theQuery)

    def CreateArrayMetadata(self, theArray, widthMax, heightMax, widthMin=0, heightMin=0, chunk=1000, tiles=1,
                            maxPixels=10000000, attribute='value', band=1):
        """
        This function gathers all the metadata necessary

        {node: {"node": node, "y_min": min(heightRange), "y_max": max(heightRange),"height": len(heightRange), \
        "width": self.width ,"datastore": dataStorePath, "filepath": RasterPath, "attribute": self.AttributeString, \
        "array_shape": self.RasterArrayShape, "destination_array": scidbArray}

        Input:
            theArray = The array to process on
            widthMax = The maximum width
            heightMax = The maximum height
            widthMin = The minimum width
            heightMin = The minimum height
            chunk = The chunk to process on
            tiles = The tile to process on
            maxPixels = Maximum pixels to process on
            attribute = Attribute to process on
            band = The band to process on

        Output:
            The raster reads
        """
        if tiles == 1:
            tiles = int(round(maxPixels / (chunk * chunk)))
            if tiles < 1:
                tiles = 1

        print("Number of tiles to be loaded %s .... %s x %s <= %s" % (tiles, chunk, chunk, maxPixels))
        RasterReads = OrderedDict()

        for y_version, yOffSet in enumerate(np.arange(heightMin, heightMax, chunk)):

            rowsRemaining = heightMax - yOffSet

            # If this is not a short read, then read the correct size.
            if rowsRemaining > chunk:
                rowsRemaining = chunk

            for x_version, xOffSet in enumerate(np.arange(widthMin, widthMax, chunk * tiles)):

                columnsRemaining = widthMax - xOffSet

                # If this is not a short read, then read the correct size.
                if columnsRemaining > chunk * tiles:
                    columnsRemaining = chunk * tiles

                RasterReads[(y_version, x_version)] = OrderedDict([("xoff", xOffSet), ("yoff", yOffSet),
                                                                   ("height", heightMax), ("width", widthMax),
                                                                   ("xsize", columnsRemaining),
                                                                   ("ysize", rowsRemaining),
                                                                   ("array_shape", self.RasterArrayShape),
                                                                   ("attribute", attribute),
                                                                   ("destination_array", theArray),
                                                                   ("chunk", chunk), ("bands", band),
                                                                   ("datastore", self.dataStorePath),
                                                                   ("filepath", self.rasterPath)])

        return RasterReads

    def ConfigureParallelReads(self):
        """
        The Raster Reads dictionary returns a number of reads. But SciDB Parallel load needs all instances to have
        something to load
        This function fixes that

        Input:
            None

        Output:
            The parallel dictionary, an ordered dictionary with the (row, column) tuple as a key, which itself has a
            value of an ordered dictonary with the following keys:
                xoff = x offset for reads
                yoff = y offset for reads
                width = width for reads
                xsize = size of x for reads
                array_shape = shape of array for reads
                attribute = attribute for reads
                destination_array = destination_array for reads
                chunk = chunk for reads
                datastore = datastore for reads
                filepath = filepath for reads
        """

        numOfReads = len(self.RasterReadingData)
        numOfSciDB = self.SciDB_Instances

        # This identifies all of the reads where there is enough tiles for each instance
        numParallelReads = math.floor(numOfReads / numOfSciDB)
        print(numOfReads, numParallelReads, numOfSciDB)
        # Backing out the number of remaining tiles
        numUnevenReads = int(numOfReads - (numParallelReads * numOfSciDB))

        if numOfReads == numParallelReads * numOfSciDB:
            return self.RasterReadingData
        else:

            raggedKeys = list(self.RasterReadingData.keys())[-numUnevenReads:]

            numPartitions = numOfSciDB - (len(raggedKeys) - 1)

            # Generating an array shape for splitting
            theTile = self.RasterReadingData[raggedKeys[-1]]
            array = np.ones((theTile['ysize'], theTile['xsize']))

            parallelDict = OrderedDict((k, self.RasterReadingData[k]) for k in list(self.RasterReadingData.keys())[:-1])
            row, col = list(parallelDict.keys())[-1:][0]
            newYOff = theTile['yoff']
            for splitArray in np.array_split(array, numPartitions):
                y, x = splitArray.shape
                # provide a new key
                col += 1

                parallelDict[(row, col)] = OrderedDict([('xoff', theTile['xoff']), \
                                                        ('yoff', newYOff), ('height', theTile['height']),
                                                        ('width', theTile['width']), \
                                                        ('xsize', x), ('ysize', y),
                                                        ('array_shape', theTile['array_shape']), \
                                                        ('attribute', theTile['attribute']),
                                                        ('destination_array', theTile['destination_array']), \
                                                        ('chunk', theTile['chunk']), ('bands', theTile['bands']),
                                                        ('datastore', theTile['datastore']), \
                                                        ('filepath', theTile['filepath'])])

                newYOff += y

            return parallelDict


class ZonalStats(object):

    def __init__(self, boundaryPath, rasterPath, SciDBArray):
        """
        Initialization of a ZonalStats object

        Input:
            boundaryPath = Path of the boundary
            rasterPath = Path of the raster
            SciDBArray = SciBD array

        Output:
            An instance of the ZonalStats class
        """
        import scidb

        self.sdb = scidb.iquery()
        self.__SciDBInstances()

        self.vectorPath = boundaryPath
        self.geoTiffPath = rasterPath
        self.SciDBArrayName = SciDBArray

    def __SciDBInstances(self):
        """
        Determine SciDB Instances available

        Input:
            None

        Output:
            None
        """

        query = self.sdb.queryAFL("list('instances')")
        self.SciDBInstances = len(query.splitlines()) - 1

    def SerialRasterization(self, boundaryPath, rasterPath, SciDBArray, storagePath):
        """
        Serial rasterization

        Input:
             boundaryPath = The path of the boundary
             rasterPath = The path of the raster
             SciDBArray = The array to process on
             storagePath = The path to store the array

        Output:
            None
        """
        self.tempRastName = 'p_zones'
        self.binaryPath = r'%s' % storagePath 
        self.RasterArray = self.RasterizePolygon(rasterPath, boundaryPath)

    def RasterizePolygon(self, inRasterPath, vectorPath):
        """
        This function will Rasterize the Polygon based off the inRasterPath provided. 
        This only creates a memory raster
        The rasterization process uses the shapefile attribute ID

        Input:
            inRasterPath = The path of the raster
            vectorPath = The path of the vector

        Output:
            The array of the band
        """
        from osgeo import ogr, gdal

        # The array size, sets the raster size
        inRaster = gdal.Open(inRasterPath)
        rasterTransform = inRaster.GetGeoTransform()
        pixel_size = rasterTransform[1]

        # Open the vector dataset
        vector_dataset = ogr.Open(vectorPath)
        theLayer = vector_dataset.GetLayer()
        geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()

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

    def NumpyToGDAL(self, arrayType):
        """
        Description

        Input:
            arrayType = The type of the array to convert

        Output:
            The converted type
        """
        gdalTypes = {
            "uint8": 1,
            "int8": 1,
            "uint16": 2,
            "int16": 3,
            "uint32": 4,
            "int32": 5,
            "float32": 6,
            "float64": 7,
            "complex64": 10,
            "complex128": 11,
        }

        return gdalTypes[arrayType]

    def SciDBDataTypes(self):
        """
        List of all SciDB Data Types

        Input:
            None

        Output:
            None

        """
        SciDBTypes = {
            "bool": "",
            "char": "",
            "datetime": "",
            "double": "float64",
            "float": "float32",
            "int8": "int8",
            "int16": "int16",
            "int32": "int32",
            "int64": "int64",
            "string": "",
            "uint8": "uint8",
            "uint16": "uint16",
            "uint32": "uint32",
            "uint64": ""
        }

    def WriteRaster(self, inArray, outPath, noDataValue=-999):
        """
        Write a raster to a given path

        Input:
            inArray = The array to write
            outPath = The path to write to
            noDataValue = What to replace missing data with in the outfile

        Output:
            None
        """
        from osgeo import ogr, gdal, gdal_array
        driver = gdal.GetDriverByName('GTiff')

        height, width = inArray.shape

        if driver:
            geoTiff = driver.Create(outPath, width, height, 1, 6)
            geoTiff.SetGeoTransform((self.RasterX, self.pixel_size, 0, self.RasterY, 0, -self.pixel_size))
            geoTiff.SetProjection(self.rasterProjection)
            band = geoTiff.GetRasterBand(1)
            band.SetNoDataValue(noDataValue)
            # Writing Array
            band.WriteArray(inArray)
            geoTiff.FlushCache()

        del geoTiff

    def RasterMetadata(self, inRasterPath, vectorPath, instances, dataStorePath):
        """
        Get the metadata for a raster

        Input:
            inRasterPath = The path of the raster
            vectorPath = The vector path
            instances = The instances to process on
            dataStorePath = Where to store the data

        Output:
            None
        """
        from osgeo import ogr, gdal
        from SciDBGDAL import world2Pixel, Pixel2world
        # The array size, sets the raster size
        inRaster = gdal.Open(inRasterPath)

        rasterTransform = inRaster.GetGeoTransform()
        self.rasterProjection = inRaster.GetProjection()
        self.pixel_size = rasterTransform[1]
        vector_dataset = ogr.Open(vectorPath)
        theLayer = vector_dataset.GetLayer()
        geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()

        self.width = int((geomMax_X - geomMin_X) / self.pixel_size)

        # TopLeft & lowerRight
        self.tlX, self.tlY = world2Pixel(rasterTransform, geomMin_X, geomMax_Y)
        self.RasterX, self.RasterY = Pixel2world(rasterTransform, self.tlX, self.tlY)
        self.lrX, self.lrY = world2Pixel(rasterTransform, geomMax_X, geomMin_Y)
        print("Height %s = %s - %s" % (self.lrY - self.tlY, self.lrY, self.tlY))
        self.VectorHeight = abs(self.lrY - self.tlY)
        self.VectorWidth = abs(self.width)

        step = int(abs(self.tlY - self.lrY) / instances)
        print("Each partition array dimensions are approximately %s lines, %s columns = %s pixels" %
              (step, self.width, step * self.width))
        self.arrayMetaData = []

        # Set the top pixel, then use it to add to the metadata
        top = self.tlY
        for i in range(instances):
            if top + step <= self.lrY and i < instances - 1:
                topPixel = top
                print("top pixel: %s" % topPixel)
                self.height = step
            elif top + step <= self.lrY and i == instances - 1:
                topPixel = top
                self.height = self.lrY - topPixel
            else:
                topPixel = i
                self.height = abs(self.lrY - topPixel)

            top += step

            offset = abs(self.tlY - topPixel)
            self.x, self.y = Pixel2world(rasterTransform, self.tlX, topPixel)
            self.arrayMetaData.append((geomMin_X, self.y, self.height, self.width,
                                       self.pixel_size, rasterTransform[5], self.rasterProjection,
                                       vectorPath, i, offset, dataStorePath))

    def CreateMask(self, rasterValueDataType, tempArray='mask'):
        """
        Create an empty raster "Mask "that matches the SciDBArray

        Input:
            rasterValueDataType = The data type of the array
            tempArray = The name of the array to create

        Output:
            The dimensions of the mask
        """
        import re

        results = self.sdb.queryAFL("show(%s)" % self.SciDBArrayName)
        results = results.decode("utf-8")

        R = re.compile(r'\<(?P<attributes>[\S\s]*?)\>(\s*)\[(?P<dim_1>\S+)(;\s|,\s)(?P<dim_2>[^\]]+)')
        results = results.lstrip('results').strip()
        match = R.search(results)

        # This code creates 2d planar mask
        dim = r'([a-zA-Z0-9]+(=[0-9]:[0-9]+:[0-9]+:[0-9]+))'
        alldimensions = re.findall(dim, results)
        thedimensions = [d[0] for d in alldimensions]
        if len(thedimensions) > 2:
            dimensions = "; ".join(thedimensions[1:])
        else:
            dimensions = "; ".join(thedimensions)

        try:
            A = match.groupdict()
        except:
            print(results)
            raise

        try:
            sdbquery = r"create array %s <id:%s> [%s]" % (tempArray, rasterValueDataType, dimensions)
            self.sdb.query(sdbquery)
        except:
            self.sdb.query("remove(%s)" % tempArray)
            sdbquery = r"create array %s <id:%s> [%s]" % (tempArray, rasterValueDataType, dimensions)
            self.sdb.query(sdbquery)

        return thedimensions

    def InsertRedimension(self, tempRastName, destArray, minY, minX):
        """
        First part inserts the boundary array into larger global mask array

        Input:
            tempRastName = The name of the array to insert
            destArray = The destination array
            minY = The minimum Y to process on
            minX = The minimum X to process on

        Output:
            Time it takes to insert
        """
        start = timeit.default_timer()
        sdbquery = "insert(redimension(apply({A}, x, x1+{xOffSet}, y, y1+{yOffSet}, value, id), {B} ), {B})".format(
            A=tempRastName, B=destArray, yOffSet=minY, xOffSet=minX)
        self.sdb.query(sdbquery)
        stop = timeit.default_timer()
        insertTime = stop - start

        return insertTime

    def GlobalJoin_SummaryStats(self, SciDBArray, tempRastName, tempArray, minY, minX, maxY, maxX, thedimensions,
                                theband=0, csvPath=None):
        """
        This is the SciDB Global Join
        Joins the array and conducts the grouped_aggregate

        Input:
            SciDBArray = The SciDB array to process on
            tempRastName = The name of the temporary array (not currently used)
            tempArray = The array to join on
            minY = The minimum Y to join on
            minX = The minimum X to join on
            maxY = The maximum Y to join on
            maxX = The maximum X to join on
            thedimensions = The dimensions of the array
            theband = The band to reclass over
            csvPath = The CSV to write to

        Output:
            The time it takes to run the query
        """

        dimName = thedimensions[0].split("=")[0]
        start = timeit.default_timer()
        if len(thedimensions) > 2:
            sdbquery = "grouped_aggregate(join(between(slice(%s,%s,%s), %s, %s, %s, %s), " \
                       "between(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), id)" \
                       % (SciDBArray, dimName, theband, minY, minX, maxY, maxX, tempArray, minY, minX, maxY, maxX)
        else:
            sdbquery = "grouped_aggregate(join(between(%s, %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), " \
                       "min(value), max(value), avg(value), count(value), id)" \
                       % (SciDBArray, minY, minX, maxY, maxX, tempArray, minY, minX, maxY, maxX)

        # Time the query, writing if able
        self.sdb.query(sdbquery)
        stop = timeit.default_timer()
        queryTime = stop - start
        if csvPath:
            self.sdb.queryResults(sdbquery, r"%s" % csvPath)

        return queryTime

    def JoinReclass(self, SciDBArray, tempRastName, tempArray, minY, minX, maxY, maxX, thedimensions, reclassText,
                    theband=0, outcsv=None):
        """
        Join and save a csv

        Input:
            SciDBArray = The SciDB array to process on
            tempRastName = The name of the temporary array
            tempArray = The array to join on
            minY = The minimum Y to join on
            minX = The minimum X to join on
            maxY = The maximum Y to join on
            maxX = The maximum X to join on
            thedimensions = The dimensions of the array
            reclassText = The text for reclassing
            theband = The band to reclass over
            outcsv = The CSV to write to

        Output:
            None
        """
        self.InsertRedimension(tempRastName, tempArray, minY, minX)

        dimName = thedimensions[0].split("=")[0]
        start = timeit.default_timer()
        if len(thedimensions) > 2:
            sdbquery = "apply(join(between(slice(%s, %s, %s), %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), " \
                       "newvalue, %s)" % (SciDBArray, dimName, theband, minY, minX, maxY, maxX, tempArray, minY, minX,
                                          maxY, maxX, reclassText)
        else:
            sdbquery = "apply(join(between(%s, %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), newvalue, %s)" % \
                       (SciDBArray, minY, minX, maxY, maxX, tempArray, minY, minX, maxY, maxX, reclassText)

        # Save the CSV
        if outcsv:
            self.sdb.query("save(sort(%s,y,y,x,x),y,x),'%s', 0, 'csv') " % (sdbquery[:-1], outcsv))
        else:
            self.sdb.query(sdbquery)


def ParamSeperator(inParams):
    """
    Seperate paramaters into a tuple

    Input:
        inParams = List of parameters including the following, in order:
            x = x dimension
            y = y dimension
            height = Height of the raster
            width = Width of the raster
            pixel_1 = Pixel size
            pixel_2 = Result from raster transform
            projection = Projection of the raster
            vectorPath = Path of the vector
            counter = Current counter
            offset = Offset to start at
            dataStorePath = Where to store the data

    Output:
        The parameters as a tuple:
            (x, y, height, width, pixel_1, pixel_2, projection, vectorPath, counter, offset, dataStorePath)
    """

    x = inParams[0]
    y = inParams[1]
    height = inParams[2]
    width = inParams[3]
    pixel_1 = inParams[4]
    pixel_2 = inParams[5]
    projection = inParams[6]
    vectorPath = inParams[7]
    counter = inParams[8]
    offset = inParams[9]
    dataStorePath = inParams[10]

    return x, y, height, width, pixel_1, pixel_2, projection, vectorPath, counter, offset, dataStorePath


def BigRasterization(inParams):
    """
    Function for rasterizing in parallel

    Input:
        inParams = List of parameters including the following
            x = The x dimension to use
            y = The y dimension to use
            height = The height of a raster dataset to process on
            width = The width of a raster dataset to process on
            pixel_1 = First pixel to use for transformation
            pixel_2 = Second pixel to use for transformation
            projection = The projection to use
            vectorPath = Path to the vector to use
            counter = Counter to use for writing
            offset = Offset to use for processing
            dataStorePath = Where to write the data files to

    Output:
        Tuple containing counter, offset, bandArray, binaryPartitionPath
    """
    from SciDBGDAL import world2Pixel, Pixel2world
    from osgeo import ogr, gdal
    import numpy as np
    import os

    x, y, height, width, pixel_1, pixel_2, projection, vectorPath, counter, offset, dataStorePath = \
        ParamSeperator(inParams)
    outTransform = [x, pixel_1, 0, y, 0, pixel_2]
    memDriver = gdal.GetDriverByName('MEM')

    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()

    folder = "%s/%s" % (dataStorePath, counter)
    if not os.path.exists(folder):
        os.mkdir(folder)

    binaryPartitionPath = "%s/p_zones.scidb" % folder

    if os.path.exists(binaryPartitionPath):
        print("****Removing file****")
        os.remove(binaryPartitionPath)

    # Generate an array of elements the length of raster height
    hdataset = np.arange(height)
    if height * width > 50000000:
        # This is the very big rasterization process
        for p, h in enumerate(np.array_split(hdataset, 100)):
            # h min is the minimum offset value
            colX, colY = world2Pixel(outTransform, x, y)
            memX, memY = Pixel2world(outTransform, colX, colY + h.min())
            # Out Transform will change with the step
            memTransform = [memX, pixel_1, 0, memY, 0, pixel_2]
            # Height changes
            theRast = memDriver.Create('', width, len(h), 1, gdal.GDT_Int32)  # gdal.GDT_Int16

            theRast.SetProjection(projection)
            theRast.SetGeoTransform(memTransform)

            band = theRast.GetRasterBand(1)
            band.SetNoDataValue(-999)

            # If you want to use another shapefile field you need to change this line
            gdal.RasterizeLayer(theRast, [1], theLayer, options=["ATTRIBUTE=ID"])

            bandArray = band.ReadAsArray()
            print("Node: %s, partition number: %s y: %s, x: %s, height: %s, arrayshape: %s " %
                  (counter, p, memY, memX, height, bandArray.shape))
            del theRast

            folder = "%s/%s" % (dataStorePath, counter)
            if not os.path.exists(folder):
                os.mkdir(folder)

            binaryPartitionPath = "%s/p_zones.scidb" % folder

            ArrayToBinary(bandArray, binaryPartitionPath, 'mask', offset + h.min())
    else:

        theRast = memDriver.Create('', width, height, 1, gdal.GDT_Int32)  # gdal.GDT_Int16

        theRast.SetProjection(projection)
        theRast.SetGeoTransform(outTransform)

        band = theRast.GetRasterBand(1)
        band.SetNoDataValue(-999)

        # If you want to use another shapefile field you need to change this line
        gdal.RasterizeLayer(theRast, [1], theLayer, options=["ATTRIBUTE=ID"])

        bandArray = band.ReadAsArray()
        del theRast

        folder = "%s/%s" % (dataStorePath, counter)
        if not os.path.exists(folder):
            os.mkdir(folder)

        binaryPartitionPath = "%s/p_zones.scidb" % folder

        ArrayToBinary(bandArray, binaryPartitionPath, 'mask', offset)

    return counter, offset, bandArray, binaryPartitionPath


def ArrayToBinary(theArray, binaryFilePath, attributeName='value', yOffSet=0, xOffSet=0):
    """
    Use Numpy tricks to write a numpy array in binary format with indices 

    input:
        theArray = Numpy 2D array
        binaryFilePath = File to write to
        attributeName = Name of the attribute to use
        yOffset = Offset for the y dimension
        xOffset = Offset for the x dimension
    output:
        Numpy 2D array in binary format
    """
    import numpy as np

    col, row = theArray.shape
    with open(binaryFilePath, 'ab') as fileout:
        # Oneliner that creates the column index. Pull out [y for y in range(col)] to see how it works
        column_index = np.array(np.repeat([y for y in np.arange(0 + yOffSet, col + yOffSet)], row),
                                dtype=np.dtype('int64'))

        # Oneliner that creates the row index. Pull out the nested loop first: [x for x in range(row)]
        # Then pull the full list comprehension: [[x for x in range(row)] for i in range(col)]
        row_index = np.array(np.concatenate([[x for x in range(0 + xOffSet, row + xOffSet)] for i in range(col)]),
                             dtype=np.dtype('int64'))

        # Oneliner for writing out the file
        # Add this to make it a csv tofile(binaryFilePath), "," and modify the open statement to 'w'
        np.core.records.fromarrays(
            [column_index, row_index, theArray.ravel()],
            dtype=[('y', 'int64'), ('x', 'int64'), (attributeName, theArray.dtype)]).ravel().tofile(fileout)

    del column_index, row_index, theArray


def ParallelRasterization(coordinateData, theRasterClass=None):
    """
    Rasterize data in parallel

    Input:
        coordinateData = Data to use for rasterization
        theRasterClass = Raster class to utilize

    Output:
        Data types of the array on success
    """

    pool = mp.Pool(len(coordinateData))

    # Attempt to run the rasterization
    try:
        arrayData = pool.imap(BigRasterization, (c for c in coordinateData))
        pool.close()
        pool.join()
    except Exception as e:
        print(e)
        print("Error")
    else:
        arraydatatypes = []
        for datastore, offset, array, resultPath in arrayData:
            arraydatatypes.append(array.dtype)
        return arraydatatypes


def RemoveArrayVersions(sdb, theArrayName):
    """
    Remove array versions from the database

    Input:
        sdb = The connection to the SciDB array
        theArrayName = Name of the array to remove from

    Output:
        None
    """

    versions = sdb.versions(theArrayName)
    if len(versions) > 1:
        for v in versions[:-1]:
            try:
                sdb.query("remove_versions(%s, %s)" % (theArrayName, v))
            except:
                # If we don't remove the version we don't care
                pass


def ParallelLoadByChunk(rasterReadingData):
    """
    This function will do parallel loading that supports fast redimensioning

    Input:
        rasterReadingData = The raster data

    Output:
        None
    """

    from scidb import iquery, Statements
    from itertools import cycle, chain
    from collections import Counter
    import timeit

    sdb = iquery()
    sdb_statements = Statements(sdb)
    query = sdb.queryAFL("list('instances')")
    scidbInstances = len(query.splitlines()) - 1

    # Cycle through the instances with the given data
    for r, node in zip(rasterReadingData, cycle(range(scidbInstances))):
        rasterReadingData[r]["node"] = node

    # Counter dictionary which reports back how many times node x occured.
    # We are just interested in node 0
    numberofNodeLoops = Counter(rasterReadingData[k]["node"] for k in rasterReadingData)
    loadLoops = numberofNodeLoops[0]
    aKey = list(rasterReadingData.keys())[0]
    loadAttribute = "%s_1:%s" % (rasterReadingData[aKey]['attribute'].split(":")[0], rasterReadingData[aKey]
    ['attribute'].split(":")[1])

    try:
        start = timeit.default_timer()
        for l, nodeLoopIteration in enumerate(np.array_split(list(rasterReadingData.items()), loadLoops)):
            # Create load arrsy
            pool = mp.Pool(scidbInstances)
            print("Loading %s of %s" % (l, loadLoops - 1))
            sdb_statements.CreateLoadArray("LoadArray", loadAttribute, int(nodeLoopIteration[0][1]['array_shape']))
            pool.imap(Read_Write_Raster, (n for n in nodeLoopIteration))
            pool.close()
            pool.join()

            # Load the one dimension array and insert redimension
            startLoad = timeit.default_timer()
            sdb_statements.LoadOneDimensionalArray(-1, "LoadArray", loadAttribute, 1, 'pdataset.scidb')

            startRedimension = timeit.default_timer()
            sdb_statements.InsertRedimension("LoadArray", nodeLoopIteration[0][1]["destination_array"],
                                             oldvalue=loadAttribute.split(":")[0], newvalue='value')

            sdb.query("remove(LoadArray)")
            RemoveArrayVersions(sdb, nodeLoopIteration[0][1]["destination_array"])

            stop = timeit.default_timer()
            if l == 0:
                print("Estimated time for loading the dataset in minutes %s: LoadTime: %s seconds, RedimensionTime: "
                      "%s seconds" % ((stop - start) * loadLoops / 60, startRedimension - startLoad, stop -
                                      startRedimension))

    except:
        print("Something went wrong")


def ParallelLoad(rasterReadingMetadata):
    """
    This function is designed to load all sizes of arrays
    We are using a couple of custom functions to break the dataset into smaller pieces for repetive parallel writing /
    loading and then a single redimension store
    You can improve the performance by setting a high maxPixel threshold value. 

    maxPixel = Number of pixels to read/write/load per loop.
    Make sure to consider the number of SciDB processes when setting maxPixel 

    Input:
        rasterReadingMetadata = The raster data

    Output:
        None
    """
    from scidb import iquery, Statements
    import timeit
    numProcesses = len(rasterReadingMetadata)

    sdb = iquery()
    sdb_statements = Statements(sdb)

    try:
        loadLoops = ArraySplicerLogic(rasterReadingMetadata[0]['width'], rasterReadingMetadata[0]['height'], 5000000)
        loadAttribute = "%s_1:%s" % (rasterReadingMetadata[0]['attribute'].split(":")[0], rasterReadingMetadata[0]
        ['attribute'].split(":")[1])
        nodeLoopData = AdjustMetaData(loadLoops, rasterReadingMetadata)

        start = timeit.default_timer()
        for l, nodeLoopIteration in enumerate(np.array_split(list(nodeLoopData.items()), loadLoops)):
            # Have to initiate the pool for each loop
            pool = mp.Pool(numProcesses)
            print("Loading %s of %s" % (l + 1, loadLoops))
            # Create the load array
            sdb_statements.CreateLoadArray("LoadArray", loadAttribute, rasterReadingMetadata[0]['array_shape'])
            pool.imap(Read_Write_Raster, (n for n in nodeLoopIteration))
            pool.close()
            pool.join()

            # Load the one dimension array and insert redimension
            startLoad = timeit.default_timer()
            sdb_statements.LoadOneDimensionalArray(-1, "LoadArray", loadAttribute, 1, 'pdataset.scidb')

            startRedimension = timeit.default_timer()
            sdb_statements.InsertRedimension("LoadArray", rasterReadingMetadata[1]["destination_array"], oldvalue=
            loadAttribute.split(":")[0], newvalue='value')

            sdb.query("remove(LoadArray)")
            RemoveArrayVersions(sdb, rasterReadingMetadata[1]["destination_array"])

            stop = timeit.default_timer()
            if l == 0:
                print("Estimated time for loading the dataset in minutes %s: WriteTime: %s seconds, LoadTime: %s "
                      "seconds, RedimensionTime: %s seconds" % ((stop - start) * loadLoops / 60, startLoad - start,
                                                                startRedimension - startLoad, stop - startRedimension))

    except Exception as e:
        print(e)
        print("Error")


def AdjustMetaData(loops, theRMD):
    """
    Function to adjust the metadata for proper loading
    This function returns the adjust load dimensions for each iteration

    Input:
        loops = The number of subarrays to run through
        theRMD = The raster metadata

    Output:
        A dictionary with the adjusted metadata, same format as the input
    """
    adjustedData = {(l, r): {"node": theRMD[r]["node"], "y_min": theRMD[r]["y_min"], "y_max": theRMD[r]["y_max"],
                             "height": theRMD[r]["height"], "width": theRMD[r]["width"],
                             "datastore": theRMD[r]["datastore"],
                             "filepath": theRMD[r]["filepath"], "array_shape": theRMD[r]["array_shape"],
                             "destination_array":
                                 theRMD[r]["destination_array"],
                             "attribute": theRMD[r]["attribute"], "xoff": 0, "yoff": int(theRMD[r]["y_min"] + min(h)),
                             "ysize": len(h),
                             "xsize": theRMD[r]["width"]}
                    for r in theRMD for l, h in enumerate(np.array_split(np.arange(theRMD[r]["height"]), loops))}

    sortedDict = OrderedDict([(r, adjustedData[r]) for r in sorted(adjustedData.keys())])

    return sortedDict


def Read_Write_Raster(rDict):
    """
    This function takes as input an OrderedDict, which is uses for reading and writing

    Input:
        rDict = An ordered Dictionary (key, {key, value} ) with the following keys:
            node = The node to use
            ysize = The size of the y dimension
            xsize = The size of the x dimension
            yoff = Offsey for y
            xoff = Offset for x
            datastore = Where the data is stored
            filepath = Path to the file to

    Output:
        None
    """

    import os
    from osgeo import gdal
    from gdalconst import GA_ReadOnly

    print("Node %s, array size h:%s, w:%s ,totalpixles: %s " % (rDict[1]["node"], rDict[1]["ysize"], rDict[1]["xsize"],
                                                                rDict[1]["ysize"] * rDict[1]["xsize"]))
    print("xoff: %s, yoff: %s " % (rDict[1]["xoff"], rDict[1]["yoff"]))
    raster = gdal.OpenShared(rDict[1]["filepath"], GA_ReadOnly)

    binaryPartitionPath = r"%s/%s/pdataset.scidb" % (rDict[1]["datastore"], rDict[1]["node"])
    if os.path.exists(binaryPartitionPath):
        # If the file already exists, remove it
        print("****Removing file****  %s" % binaryPartitionPath)
        os.remove(binaryPartitionPath)

    rArray = raster.ReadAsArray(xoff=int(rDict[1]["xoff"]), yoff=int(rDict[1]["yoff"]), xsize=int(rDict[1]["xsize"]),
                                ysize=int(rDict[1]["ysize"]))
    ArrayToBinary(rArray, binaryPartitionPath, 'value_1', int(rDict[1]["yoff"]), int(rDict[1]["xoff"]))

    del raster


def ArraySplicerLogic(width, height, maxPixels=20000000):
    """
    This function will determine how many loops are necessary to efficiently load a large geoTiff
    maxPixels should be determined based on memory constraints of your machine

    Input:
        width = The width of the array
        height = The height of the array
        maxPixels = The maximum number of pixels per loop

    Output:
        The number of iterations
    """
    if height * width < maxPixels:
        return 1
    elif width > maxPixels:
        return height
    else:
        # Split the array
        possibles = OrderedDict([(h, h * width) for h in range(height)])
        solutions = {k: v for k, v in possibles.items() if v <= maxPixels and k > 0}
        numIterations = height / max({k: v for k, v in solutions.items()})
        return round(numIterations)
