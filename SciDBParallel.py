import timeit, itertools
import multiprocessing as mp

class ZonalStats(object):

    def __init__(self,boundaryPath, rasterPath, SciDBArray):
        import scidb
        import numpy as np
        
        #self.__SciDBInstances()

        self.scidb = scidb.iquery()
        #self.pool = mp.Pool(2) #self.SciDBInstances)
        self.np = np

        self.vectorPath = boundaryPath
        self.geoTiffPath = rasterPath
        self.SciDBArrayName = SciDBArray

        

        #self.main(boundaryPath, rasterPath, SciDBArray)

    def __SciDBInstances(self,):
        """
        Determine SciDB Instances available
        """
        
        query = self.sdb.queryAFL("list('instances')")
        self.SciDBInstances = len(query.splitlines())-1


    def main(self,boundaryPath, rasterPath, SciDBArray):
        """

        """
        self.tempRastName = 'p_zones'
        self.binaryPath = r'c:\work' #'/storage' #'/home/scidb/scidb_data/0'
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



    # def ParallelProcessing(self, params):
    #     """
    #     This function wraps around the ArrayToBinary and WriteBinaryFile and 
    #     """

    #     binaryPath = params[0]
    #     yOffSet = params[1]
    #     print(yOffSet)
    #     datastore, arrayChunk = params[2]

    #     binaryPartitionPath = "%s/%s/p_zones.scidb" % (binaryPath, datastore)
    #     #print(binaryPartitionPath)
      
    #     with open(binaryPartitionPath, 'wb') as fileout:
    #           fileout.write(ArrayToBinary(arrayChunk, yOffSet).ravel().tobytes())
    #           print(binaryPartitionPath)


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



    def RasterMetadata(self, inRasterPath, vectorPath, instances, dataStorePath):
        """
        
        """
        from osgeo import ogr, gdal
        from SciDBGDAL import world2Pixel, Pixel2world
        #The array size, sets the raster size 
        inRaster = gdal.Open(inRasterPath)
        rasterTransform = inRaster.GetGeoTransform()
        rasterProjection = inRaster.GetProjection()
        pixel_size = rasterTransform[1]
        
        vector_dataset = ogr.Open(vectorPath)
        theLayer = vector_dataset.GetLayer()
        geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()
        
        rasterWidth = int((geomMax_X - geomMin_X) / pixel_size)
        
        #TopLeft & lowerRight
        tlX, tlY = world2Pixel(rasterTransform, geomMin_X, geomMax_Y)
        lrX, lrY = world2Pixel(rasterTransform, geomMax_X, geomMin_Y)
        #print(tlY, lrY, geomMin_Y, geomMax_Y)
        #print("Rows between %s" % (tlY-lrY))
        
        step = int(abs(tlY-lrY)/instances)
        
        self.arrayMetaData = []
        for c, i in enumerate(range(tlY,lrY,step)):
            if i+step <= lrY:
                print("top pixel: %s" % (i+c) )
                topPixel = i+c
                height = step

            else:
                print("top pixel: %s" % (i+c) )
                topPixel = i+c
                height = abs(lrY-topPixel)
            
            offset = abs(tlY - topPixel)
            x,y = Pixel2world(rasterTransform, tlX, topPixel)
            self.arrayMetaData.append((geomMin_X, y, height, rasterWidth, 
                pixel_size, rasterTransform[5], rasterProjection, 
                vectorPath, c, offset, dataStorePath))

        #print(pixelCoordinates)
        #return pixelCoordinates


    def SciDBZonalStats(self,rasterizedArray,  ):
        #This is the full parallel mode
        print("Parallel Version of Zonal Stats")
        

        print("Partitioning Array")
        start = timeit.default_timer()
        chunkedArrays = self.np.array_split(rasterizedArray, 2, axis=0)
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

    def CreateMask(self):
        """
        SciDB Summary Stats
        1. Make an empty raster "Mask "that matches the SciDBArray
        """

        import re    
        tempArray = "mask"

        #afl = sdb.afl
        #theArray = afl.show(SciDBArray)
        #results = theArray.contents()

        results = self.sdb.queryAFL("show(%s)" % (SciDBArray))
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
          self.sdb.query(sdbquery)
        except:
          print(sdbquery)
          self.sdb.query("remove(%s)" % tempArray)
          sdbquery = r"create array %s <id:%s> %s" % (tempArray, rasterValueDataType, dimensions)
          self.sdb.query(sdbquery)

    def GlobalJoin_SummaryStats(self, SciDBArray, rasterValueDataType, tempSciDBLoad, tempRastName, minY, minX, maxY, maxX, verbose=False):
        """
        """
        #Write the array in the correct location
        start = timeit.default_timer()
        sdbquery ="insert(faster_redimension(apply({A}, x, x1+{xOffSet}, y, y1+{yOffSet}, value, id), {B} ), {B})".format( A=tempRastName, B=tempArray, yOffSet=minY, xOffSet=minX)
        self.sdb.query(sdbquery)
        stop = timeit.default_timer()
        insertTime = stop-start
        if verbose: print(sdbquery , insertTime)

      
        start = timeit.default_timer()
        sdbquery = "grouped_aggregate(join(between(%s, %s, %s, %s, %s), between(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), id)" % (SciDBArray, minY, minX, maxY, maxX, tempArray, minY, minX, maxY, maxX)
        self.sdb.query(sdbquery)
        stop = timeit.default_timer()
        queryTime = stop-start
        self.sdb.queryResults(sdbquery, r"/home/04489/dhaynes/%s_states2.csv" % (SciDBArray) )
        if verbose: print(sdbquery, queryTime)
        self.sdb.query("remove(%s)" % tempArray)
        self.sdb.query("remove(%s)" % tempRastName)

        return insertTime, queryTime

    # def world2Pixel(self, geoMatrix, x, y):
    #     """
    #     Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    #     the pixel location of a geospatial 
    #     """
    #     ulX = geoMatrix[0]
    #     ulY = geoMatrix[3]
    #     xDist = geoMatrix[1]
    #     yDist = geoMatrix[5]
    #     rtnX = geoMatrix[2]
    #     rtnY = geoMatrix[4]
    #     pixel = int((x - ulX) / xDist)
    #     line = int((ulY - y) / xDist)
        
    #     return (pixel, line)
    
    # def Pixel2world(self, geoMatrix, row, col):
    #     """
    #     Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    #     the x,y location of a pixel location
    #     """

    #     ulX = geoMatrix[0]
    #     ulY = geoMatrix[3]
    #     xDist = geoMatrix[1]
    #     yDist = geoMatrix[5]
    #     rtnX = geoMatrix[2]
    #     rtnY = geoMatrix[4]
    #     x_coord = (ulX + (row * xDist))
    #     y_coord = (ulY - (col * xDist))

    #     return (x_coord, y_coord)
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
    print("Rasterizing Vector in Parallel")

    x, y, height, width, pixel_1, pixel_2, projection, vectorPath, counter, offset, dataStorePath = ParamSeperator(inParams)

    outTransform= [x, pixel_1, 0, y, 0, pixel_2 ]
    
    memDriver = gdal.GetDriverByName('MEM')
    theRast = memDriver.Create('', width, height, 1, gdal.GDT_Int16)
      
    theRast.SetProjection(projection)
    theRast.SetGeoTransform(outTransform)
    
    band = theRast.GetRasterBand(1)
    band.SetNoDataValue(-99)
    
    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()

    #If you want to use another shapefile field you need to change this line
    gdal.RasterizeLayer(theRast, [1], theLayer, options=["ATTRIBUTE=ID"])
    
    bandArray = band.ReadAsArray()
    del theRast

    binaryPartitionPath = "%s\%s\p_zones.scidb" % (dataStorePath, counter)
    ArrayToBinary(bandArray, binaryPartitionPath, 'mask', offset)    
    
    return counter, offset, bandArray, binaryPartitionPath

def ArrayToBinary(theArray, binaryFilePath, attributeName='value', yOffSet=0):
    """
    Use Numpy tricks to write a numpy array in binary format with indices 

    input: Numpy 2D array
    output: Numpy 2D array in binary format
    """
    import numpy as np
    print("Writing out file: %s" % (binaryFilePath))
    col, row = theArray.shape
    with open(binaryFilePath, 'wb') as fileout:
        #Oneliner that creates the column index. Pull out [y for y in range(col)] to see how it works
        column_index = np.array(np.repeat([y for y in np.arange(0+yOffSet, col+yOffSet) ], row), dtype=np.dtype('int64'))
        
        #Oneliner that creates the row index. Pull out the nested loop first: [x for x in range(row)]
        #Then pull the full list comprehension: [[x for x in range(row)] for i in range(col)]
        row_index = np.array(np.concatenate([[x for x in range(row)] for i in range(col)]), dtype=np.dtype('int64'))

        #Oneliner for writing out the file
        fileout.write( np.core.records.fromarrays([column_index, row_index, theArray.ravel()], dtype=[('x','int64'),('y','int64'),(attributeName,theArray.dtype)]).ravel().tobytes() )
    
    del column_index, row_index, theArray


def ParallelRasterization(coordinateData):
    pool = mp.Pool(len(coordinateData))
    try:
        arrayData = pool.imap(Rasterization, (c for c in coordinateData)  )
        pool.close()
        pool.join()
    except Exception as e:
        print(e)
        print("Error")
    else:
        for datastore, offset, array, resultPath in arrayData:
            print(datastore, offset, array.shape, resultPath)
            #binaryPartitionPath = "%s\%s\p_zones.scidb" % (binaryPath, datastore)
        #return arrayData

