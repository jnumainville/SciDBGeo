import timeit, itertools
import multiprocessing as mp
import csv

class ZonalStats(object):

    def __init__(self,boundaryPath, rasterPath, SciDBArray):
        import scidb
        import numpy as np
        
        self.sdb = scidb.iquery()
        self.__SciDBInstances()
        
        #self.pool = mp.Pool(2) #self.SciDBInstances)
        self.np = np

        self.vectorPath = boundaryPath
        self.geoTiffPath = rasterPath
        self.SciDBArrayName = SciDBArray

    

    def __SciDBInstances(self,):
        """
        Determine SciDB Instances available
        """
        
        query = self.sdb.queryAFL("list('instances')")
        self.SciDBInstances = len(query.splitlines())-1


    def SerialRasterization(self,boundaryPath, rasterPath, SciDBArray, storagePath):
        """

        """
        self.tempRastName = 'p_zones'
        self.binaryPath = r'%s' % (storagePath) #'/storage' #'/home/scidb/scidb_data/0'
        self.RasterArray = self.RasterizePolygon(rasterPath, boundaryPath)
        #self.ParallelRasterization(rasterPath, boundaryPath, 2)

    def RasterizePolygon(self, inRasterPath, vectorPath):
        """
        This function will Rasterize the Polygon based off the inRasterPath provided. 
        This only creates a memory raster
        The rasterization process uses the shapfile attribute ID
        """
        from osgeo import ogr, gdal
        #RasterizePolygon(r'c:\scidb\glc2000.tif', r'c:\scidb\final_boundaries\states.shp')
        #ParallelRasterization(r'c:\scidb\glc2000.tif', r'c:\scidb\final_boundaries\states.shp', 6)

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

    def NumpyToGDAL(self, arrayType):
        """

        """
        gdalTypes= {
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

    def WriteRaster(self, inArray, outPath, noDataValue=-999):
        """

        """
        from osgeo import ogr, gdal, gdal_array
        driver = gdal.GetDriverByName('GTiff')

        height, width = inArray.shape
        #pixelType = self.NumpyToGDAL(inArray.dtype)
        #pixelType = gdal_array.NumericTypeCodeToGDALTypeCode(inArray.dtype)
        #https://gist.github.com/CMCDragonkai/ac6289fa84bcc8888035744d7e00e2e6
        if driver:
            geoTiff = driver.Create(outPath, width, height, 1, 6)
            geoTiff.SetGeoTransform( (self.RasterX, self.pixel_size, 0, self.RasterY, 0, -self.pixel_size)  )
            geoTiff.SetProjection(self.rasterProjection)
            band = geoTiff.GetRasterBand(1)
            band.SetNoDataValue(noDataValue)

            band.WriteArray(inArray)
            geoTiff.FlushCache()

        del geoTiff

    # def OutputToArray(self, filePath, columnReader, ycolumn=1):
    #     """
    #     The CSV array, must output the y and x coordinate values.
    #     """

    #     with open(filePath, 'r') as filein:
    #         thedoc = csv.reader(filein)
    #         doclines = [line for line in thedoc]
    #         dataset = self.np.array(doclines)

    #     del doclines

    #     ycoordinates = dataset[:,1]

    #     unique_y, number_of_y = self.np.unique(ycoordinates, return_counts=True)
    #     height = len(unique_y)
    #     width = number_of_y[0]
    #     valuearray = dataset[:,columnReader]

    #     #array = self.np.array([row.split(',')[columnReader] for row in csv[:-1] ]).reshape((1960, width))

    #     return valuearray.reshape( (height,width) )


    def RasterMetadata(self, inRasterPath, vectorPath, instances, dataStorePath):
        """
        
        """
        from osgeo import ogr, gdal
        from SciDBGDAL import world2Pixel, Pixel2world
        #The array size, sets the raster size 
        inRaster = gdal.Open(inRasterPath)
        # rArray = inRaster.ReadAsArray(xoff=0, yoff=0, xsize=1, ysize=1)
        # self.rasterValueDataType = rArray[0][0].dtype
        # print(self.rasterValueDataType)

        rasterTransform = inRaster.GetGeoTransform()
        self.rasterProjection = inRaster.GetProjection()
        self.pixel_size = rasterTransform[1]
        # print(rasterTransform)
        vector_dataset = ogr.Open(vectorPath)
        theLayer = vector_dataset.GetLayer()
        geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()
        
        self.width = int((geomMax_X - geomMin_X) / self.pixel_size)
        # print(self.width)
        
        #TopLeft & lowerRight
        self.tlX, self.tlY = world2Pixel(rasterTransform, geomMin_X, geomMax_Y)
        self.RasterX, self.RasterY = Pixel2world(rasterTransform, self.tlX, self.tlY)
        self.lrX, self.lrY = world2Pixel(rasterTransform, geomMax_X, geomMin_Y)
        # print(self.tlY, self.lrY, geomMin_Y, geomMax_Y)
        print("Height %s = %s - %s" % (self.lrY-self.tlY, self.lrY, self.tlY))
        #print("Rows between %s" % (tlY-lrY))
        
        step = int(abs(self.tlY-self.lrY)/instances)
        print("Each partition will have approximately %s lines" % (step))
        self.arrayMetaData = []

        top = self.tlY
        for i in range(instances):
            if top+step <= self.lrY and i < instances-1:
                topPixel = top
                # print("top pixel: %s" % (topPixel) )                
                self.height = step
            elif top+step <= self.lrY and i == instances-1:
                topPixel = top
                # print("long read, top pixel: %s" % (topPixel) )                
                self.height = self.lrY - topPixel
            else:
                topPixel = i
                # print("short read, top pixel: %s" % (topPixel) )
                self.height = abs(self.lrY-topPixel)

            top += step
            
            offset = abs(self.tlY - topPixel)
            self.x, self.y = Pixel2world(rasterTransform, self.tlX, topPixel)
            self.arrayMetaData.append((geomMin_X, self.y, self.height, self.width, 
                self.pixel_size, rasterTransform[5], self.rasterProjection, 
                vectorPath, i, offset, dataStorePath))


    def SciDBZonalStats(self,rasterizedArray):
        """
        This is the full parallel mode of zonal statistics
        """
        # print("Parallel Version of Zonal Stats")
        

        # print("Partitioning Array")
        start = timeit.default_timer()
        chunkedArrays = self.np.array_split(rasterizedArray, 2, axis=0)
        stop = timeit.default_timer()
        # print("Took: %s" % (stop-start))

        #This is super ugly, but I can't think of the one liner!
        allColumns = [c.shape[0] for c in chunkedArrays]
        yOffSet = [0]
        z = 0
        for c in allColumns:
            z += c
            yOffSet.append(z)
        #Remove the last item 
        yOffSet.pop()

        the_binaryPath = '%s' % (self.binaryPath)
        print("Converting to Binary and Writing Files in Parallel")
        start = timeit.default_timer()
        try:
            results = self.pool.imap(self.ParallelProcessing, zip( itertools.repeat(the_binaryPath), itertools.cycle(yOffSet), ((p, chunk) for p, chunk in enumerate(chunkedArrays))  )    )
        #results = pool.imap(ParallelProcessing,  (chunk for  chunk in chunkedArrays)     )
        except Exception as e:
            print(e)
        self.pool.close()
        self.pool.join()
        stop = timeit.default_timer()
        print("Took: %s" % (stop-start))            
        
        # print("Loading...")
        # start = timeit.default_timer()
        # binaryLoadPath = "p_zones.scidb" #'/data/projects/services/scidb/scidbtrunk/stage/DB-mydb/0'  #binaryPartitionPath.split("/")[-1]
        # LoadArraytoSciDB(sdb, tempRastName, binaryLoadPath, rasterValueDataType, "y1", "x1", verbose, -1)
        # stop = timeit.default_timer()
        # print("Took: %s" % (stop-start))
        
        # transferTime, queryTime = GlobalJoin_SummaryStats(sdb, SciDBArray, rasterValueDataType, '', tempRastName, ulY, ulX, lrY, lrX, verbose)


    def CreateMask(self, rasterValueDataType, tempArray='mask'):
        """
        Create an empty raster "Mask "that matches the SciDBArray
        """
        import re    
        tempArray = "mask"

        results = self.sdb.queryAFL("show(%s)" % (self.SciDBArrayName))
        results = results.decode("utf-8")

        #R = re.compile(r'\<(?P<attributes>[\S\s]*?)\>(\s*)\[(?P<dim_1>\S+)(;\s|,\s)(?P<dim_2>\S+)(\])')
        R = re.compile(r'\<(?P<attributes>[\S\s]*?)\>(\s*)\[(?P<dim_1>\S+)(;\s|,\s)(?P<dim_2>[^\]]+)')
        results = results.lstrip('results').strip()
        match = R.search(results)

        #This code creates 2d planar mask
        dim = r'([a-zA-Z0-9]+(=[0-9]:[0-9]+:[0-9]+:[0-9]+))'
        alldimensions = re.findall(dim, results)
        thedimensions = [d[0] for d in alldimensions]
        if len(thedimensions) > 2:
            dimensions = "; ".join(thedimensions[1:])
        else:
            dimensions = "; ".join(thedimensions)
        
        try:
          A = match.groupdict()
          schema = A['attributes']
          #dimensions = "[%s; %s]" % (A['dim_1'], A['dim_2'])
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

    def InsertRedimension(self, tempRastName, tempArray, minY, minX):
        """
        First part inserts the boundary array into larger global mask array
        """
        start = timeit.default_timer()
        sdbquery ="insert(redimension(apply({A}, x, x1+{xOffSet}, y, y1+{yOffSet}, value, id), {B} ), {B})".format( A=tempRastName, B=tempArray, yOffSet=minY, xOffSet=minX)
        self.sdb.query(sdbquery)
        stop = timeit.default_timer()
        insertTime = stop-start

        return insertTime

    def GlobalJoin_SummaryStats(self, SciDBArray, tempRastName, tempArray, minY, minX, maxY, maxX, thedimensions, theband=0, csvPath=None):
        """
        This is the SciDB Global Join
        Joins the array and conducts the grouped_aggregate
        """
        insertTime = self.InsertRedimension(tempRastName, tempArray, minY, minX)

        dimName = thedimensions[0].split("=")[0]
        start = timeit.default_timer()
        if len(thedimensions) > 2:
            sdbquery = "grouped_aggregate(join(between(slice(%s,%s,%s), %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), id)" % (SciDBArray, dimName,theband, minY, minX, maxY, maxX, tempArray, minY, minX, maxY, maxX)
        else:
            sdbquery = "grouped_aggregate(join(between(%s, %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), id)" % (SciDBArray, minY, minX, maxY, maxX, tempArray, minY, minX, maxY, maxX)
        
        self.sdb.query(sdbquery)
        stop = timeit.default_timer()
        queryTime = stop-start
        if csvPath:
            self.sdb.queryResults(sdbquery, r"%s" % (csvPath) ) # r"/home/04489/dhaynes/%s_states2.csv"
        #self.sdb.query("remove(%s)" % tempArray)
        #self.sdb.query("remove(%s)" % tempRastName)

        return insertTime, queryTime

    def JoinReclass(self, SciDBArray, tempRastName, tempArray,  minY, minX, maxY, maxX, thedimensions, reclassText, theband=0, outcsv=None):
        """

        """
        self.InsertRedimension(tempRastName, tempArray, minY, minX)

        dimName = thedimensions[0].split("=")[0]
        start = timeit.default_timer()
        if len(thedimensions) > 2:
            sdbquery = "apply(join(between(slice(%s, %s, %s), %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), newvalue, %s)" % (SciDBArray, dimName,theband, minY, minX, maxY, maxX, tempArray, minY, minX, maxY, maxX, reclassText)
        else:
            sdbquery = "apply(join(between(%s, %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), newvalue, %s)" % (SciDBArray, minY, minX, maxY, maxX, tempArray, minY, minX, maxY, maxX, reclassText)
        
        if outcsv:
            #print(sdbquery)
            self.sdb.query("save(sort(%s,y,y,x,x),y,x),'%s', 0, 'csv') "  % (sdbquery[:-1], outcsv))
        else:
            self.sdb.query(sdbquery)

def ParamSeperator(inParams):

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

def Rasterization(inParams):
    """
    Function for rasterizing in parallel
    """
    from osgeo import ogr, gdal
    import numpy as np
    # print("Rasterizing Vector in Parallel")

    x, y, height, width, pixel_1, pixel_2, projection, vectorPath, counter, offset, dataStorePath = ParamSeperator(inParams)
    # print(offset, x, y)
    outTransform= [x, pixel_1, 0, y, 0, pixel_2 ]
    
    memDriver = gdal.GetDriverByName('MEM')
    theRast = memDriver.Create('', width, height, 1, gdal.GDT_Int32) #gdal.GDT_Int16
      
    theRast.SetProjection(projection)
    theRast.SetGeoTransform(outTransform)
    
    band = theRast.GetRasterBand(1)
    band.SetNoDataValue(-999)
    
    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()

    #If you want to use another shapefile field you need to change this line
    gdal.RasterizeLayer(theRast, [1], theLayer, options=["ATTRIBUTE=ID"])
    
    bandArray = band.ReadAsArray()
    del theRast

    binaryPartitionPath = "%s/%s/p_zones.scidb" % (dataStorePath, counter)
    #ArrayToBinary(bandArray, binaryPartitionPath, 'mask', offset)    
    c, r = bandArray.shape
    if c*r > 50000000:
        print("Number of pixels: %s" % (c*r))
        with open(binaryPartitionPath, 'wb') as fileout:
            for partitionBandArray in np.array_split(bandArray, 10, axis=0):
                ArrayToBinary(partitionBandArray, binaryPartitionPath, 'mask', offset)
                yOffSet += partitionBandArray.shape[0] + offset
    else:
        ArrayToBinary(bandArray, binaryPartitionPath, 'mask', offset)  
    
    return counter, offset, bandArray, binaryPartitionPath

def ArrayToBinary(theArray, binaryFilePath, attributeName='value', yOffSet=0):
    """
    Use Numpy tricks to write a numpy array in binary format with indices 

    input: Numpy 2D array
    output: Numpy 2D array in binary format
    """
    import numpy as np
    # print("Writing out file: %s" % (binaryFilePath))
    col, row = theArray.shape
    with open(binaryFilePath, 'wb') as fileout:
        #Oneliner that creates the column index. Pull out [y for y in range(col)] to see how it works
        column_index = np.array(np.repeat([y for y in np.arange(0+yOffSet, col+yOffSet) ], row), dtype=np.dtype('int64'))
        
        #Oneliner that creates the row index. Pull out the nested loop first: [x for x in range(row)]
        #Then pull the full list comprehension: [[x for x in range(row)] for i in range(col)]
        row_index = np.array(np.concatenate([[x for x in range(row)] for i in range(col)]), dtype=np.dtype('int64'))

        #Oneliner for writing out the file
        #Add this to make it a csv tofile(binaryFilePath), "," and modify the open statement to 'w'
        np.core.records.fromarrays([column_index, row_index, theArray.ravel()], dtype=[('x','int64'),('y','int64'),(attributeName,theArray.dtype)]).ravel().tofile(binaryFilePath) 

    
    del column_index, row_index, theArray


def ParallelRasterization(coordinateData):
    """

    """

    pool = mp.Pool(len(coordinateData))

    try:
        arrayData = pool.imap(Rasterization, (c for c in coordinateData)  )
        pool.close()
        pool.join()
    except Exception as e:
        print(e)
        print("Error")
    else:
        arraydatatypes = []
        for datastore, offset, array, resultPath in arrayData:
            # print(datastore, offset, array.shape, resultPath, array.dtype)
            arraydatatypes.append(array.dtype)
        return arraydatatypes


