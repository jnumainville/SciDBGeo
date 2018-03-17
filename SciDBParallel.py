import timeit, itertools, csv, math
import multiprocessing as mp
import numpy as np
from collections import OrderedDict

class RasterReader(object):

    def __init__(self, RasterPath, scidbArray, attribute, chunksize, tiles, yOffSet):
        """
        Initialize the class RasterReader
        """
        
        self.width, self.height, self.datatype, self.numbands = self.GetRasterDimensions(RasterPath)
        self.AttributeNames = attribute
        self.AttributeString, self.RasterArrayShape = self.RasterShapeLogic(attribute)
        self.RasterReadingData = self.CreateArrayMetadata(scidbArray, widthMax=self.width, heightMax=self.height, widthMin=0, heightMin=yOffSet, chunk=chunksize, tiles=tiles, attribute=self.AttributeString, band=self.numbands )
        
    def RasterShapeLogic(self, attributeNames):
        """
        This function will provide the logic for determining the shape of the raster
        """
        if len(attributeNames) >= 1 and self.numbands > 1:
            #Each pixel value will be a new attribute
            attributes = ["%s:%s" % (i, self.datatype) for i in attributeNames ]
            if len(attributeNames) == 1:
                attString = " ".join(attributes)
                arrayType = 3
    
            else:
                attString = ", ".join(attributes)
                arrayType = 2
        else:
            #Each pixel value will be a new band and we must loop.
            #Not checking for 2 attribute names that will crash
            attString = "%s:%s" % (attributeNames[0], self.datatype)
            arrayType = 1
            
        return (attString, arrayType)
            
            
    def GetMetadata(self, scidbInstances, rasterFilePath, outPath, loadPath,  band):
        """
        Generator for the class
        Input: 
        scidbInstance = SciDB Instance IDs
        rasterFilePath = Absolute Path to the GeoTiff
        """
        
        for key, process, filepath, outDirectory, loadDirectory, band in zip(self.RasterMetadata.keys(), itertools.cycle(scidbInstances), itertools.repeat(rasterFilePath), itertools.repeat(outPath), itertools.repeat(loadPath), itertools.repeat(band)):
            yield self.RasterMetadata[key], process, filepath, outDirectory, loadDirectory,  band
            
    def GetRasterDimensions(self, thePath):
        """
        Function gets the dimensions of the raster file 
        """
        if isinstance(thePath,str):
            raster = gdal.Open(thePath, GA_ReadOnly)
    
            if raster.RasterCount > 1:
                rArray = raster.ReadAsArray(xoff=0, yoff=0, xsize=1, ysize=1)
                #print(rArray)
                rasterValueDataType = rArray[0][0].dtype
                #print("RasterValue Type", rasterValueDataType)

            elif raster.RasterCount == 1:
                rasterValueDataType = raster.ReadAsArray(xoff=0, yoff=0, xsize=1, ysize=1)[0].dtype
                #rasterValueDataType= [rArray]

            if rasterValueDataType == 'float32': rasterValueDataType = 'float'
            numbands = raster.RasterCount
            width = raster.RasterXSize 
            height  = raster.RasterYSize
            print(rasterValueDataType)
            
            del raster
        
        elif type(thePath).__module__ == 'numpy':
            rasterValueDataType = thePath.dtype
            height, width = thePath.shape
            numbands = 1

        return (width, height, rasterValueDataType, numbands)
    

    def CreateArrayMetadata(self, theArray, widthMax, heightMax, widthMin=0, heightMin=0, chunk=1000, tiles=1, attribute='value', band=1):
        """
        This function gathers all the metadata necessary
        The loops are 
        """
        
        RasterReads = OrderedDict()
        rowMax = 0
        
        for y_version, yOffSet in enumerate(range(heightMin, heightMax, chunk)):
            #Adding +y_version will stagger the offset
            rowsRemaining = heightMax - (y_version*chunk + y_version)

            #If this is not a short read, then read the correct size.
            if rowsRemaining > chunk: rowsRemaining = chunk
            
            for x_version, xOffSet in enumerate(range(widthMin, widthMax, chunk*tiles)):
                version_num = rowMax+x_version
                #Adding +x_version will stagger the offset
                columnsRemaining = widthMax - (x_version*chunk*tiles + x_version)
                
                #If this is not a short read, then read the correct size.
                if columnsRemaining > chunk*tiles : columnsRemaining = chunk*tiles

                #print(rowsRemaining, columnsRemaining, version_num, x_version,y_version,)
                RasterReads[str(version_num)] = OrderedDict([ ("xOffSet",xOffSet), ("yOffSet",yOffSet), \
                    ("xWindow", columnsRemaining), ("yWindow", rowsRemaining), ("version", version_num), ("array_type", self.RasterArrayShape), \
                    ("attribute", attribute), ("scidbArray", theArray), ("y_version", y_version), ("chunk", chunk), ("bands", band) ])

            
            rowMax += math.ceil(widthMax/(chunk*tiles))

        for r in RasterReads.keys():
            RasterReads[r]["loops"] = len(RasterReads)
            
            if len(RasterReads[r]["attribute"].split(" ")) < RasterReads[r]["bands"]:
                RasterReads[r]["iterate"] = RasterReads[r]["bands"]
            else:
                RasterReads[r]["iterate"] = 0
            
        return RasterReads

class ZonalStats(object):

    def __init__(self,boundaryPath, rasterPath, SciDBArray):
        import scidb
        import numpy as np
        
        self.sdb = scidb.iquery()
        self.__SciDBInstances()

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

    def SciDBDataTypes(self):
        """
        List of all SciDB Data Types
        """
        SciDBTypes ={
        "bool" :"",
        "char": "",
        "datetime" :"",
        "double" : "float64",
        "float" : "float32",
        "int8" : "int8",
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
            #Writing Array
            band.WriteArray(inArray)
            geoTiff.FlushCache()

        del geoTiff

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
        self.VectorHeight = abs(self.lrY-self.tlY)
        self.VectorWidth = abs(self.width)

        #print("Rows between %s" % (tlY-lrY))
        
        step = int(abs(self.tlY-self.lrY)/instances)
        print("Each partition array dimensionsa are approximately %s lines, %s columns = %s pixels" % (step,self.width, step*self.width))
        self.arrayMetaData = []

        top = self.tlY
        for i in range(instances):
            if top+step <= self.lrY and i < instances-1:
                topPixel = top
                print("top pixel: %s" % (topPixel) )                
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
        chunkedArrays = np.array_split(rasterizedArray, 2, axis=0)
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
        #insertTime = self.InsertRedimension(tempRastName, tempArray, minY, minX)

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

        return queryTime

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

def BigRasterization(inParams):
    """
    Function for rasterizing in parallel
    """
    from SciDBGDAL import world2Pixel, Pixel2world
    from osgeo import ogr, gdal
    import numpy as np
    import os
    # print("Rasterizing Vector in Parallel")

    x, y, height, width, pixel_1, pixel_2, projection, vectorPath, counter, offset, dataStorePath = ParamSeperator(inParams)
    # print(offset, x, y)
    outTransform= [x, pixel_1, 0, y, 0, pixel_2 ]
    memDriver = gdal.GetDriverByName('MEM')

    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()

    #Generate an array of elements the length of raster height
    hdataset = np.arange(height)
    if height * width > 50000000:
        #This is the very big rasterization process
        for p, h in enumerate(np.array_split(hdataset,20)):
            #h min is the minimum offset value
            colX, colY = world2Pixel(outTransform, x, y)
            memX, memY = Pixel2world(outTransform, colX, colY + h.min())
            # print("Node: %s, partition number: %s y: %s, x: %s, height: %s " % (counter, p, memY, memX, height))
            #Out Transform will change with the step
            memTransform = [memX, pixel_1, 0, memY, 0, pixel_2 ]
            #Height changes
            theRast = memDriver.Create('', width, len(h), 1, gdal.GDT_Int32) #gdal.GDT_Int16
              
            theRast.SetProjection(projection)
            theRast.SetGeoTransform(memTransform)
            
            band = theRast.GetRasterBand(1)
            band.SetNoDataValue(-999)

            #If you want to use another shapefile field you need to change this line
            gdal.RasterizeLayer(theRast, [1], theLayer, options=["ATTRIBUTE=ID"])
            
            bandArray = band.ReadAsArray()
            print("Node: %s, partition number: %s y: %s, x: %s, height: %s, arrayshape: %s " % (counter, p, memY, memX, height, bandArray.shape ))
            del theRast
           

            binaryPartitionPath = "%s/%s/p_zones.scidb" % (dataStorePath, counter)
            ArrayToBinary(bandArray, binaryPartitionPath, 'mask', offset + h.min())
    else:

        theRast = memDriver.Create('', width, height, 1, gdal.GDT_Int32) #gdal.GDT_Int16
          
        theRast.SetProjection(projection)
        theRast.SetGeoTransform(outTransform)
        
        band = theRast.GetRasterBand(1)
        band.SetNoDataValue(-999)
    
        #If you want to use another shapefile field you need to change this line
        gdal.RasterizeLayer(theRast, [1], theLayer, options=["ATTRIBUTE=ID"])
        
        bandArray = band.ReadAsArray()
        del theRast

        binaryPartitionPath = "%s/%s/p_zones.scidb" % (dataStorePath, counter)
        ArrayToBinary(bandArray, binaryPartitionPath, 'mask', offset) 
   
    return counter, offset, bandArray, binaryPartitionPath
    
def Rasterization(inParams):
    """
    Function for rasterizing in parallel
    """
    from osgeo import ogr, gdal
    import numpy as np
    import os
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

    if c*r > 5000000:
        #from SciDBParallel import RasterReader
        from scidb import Statements, iquery
        print("SciDB Instance: %s and initial offset %s with array size: %s" % (counter, offset, bandArray.shape))
        #bigRaster = RasterReader(bandArray, 'mask', 'id', 500, 8, offset)
        #for x in bigRaster.RasterReadingData:
        #    print(counter, bigRaster.RasterReadingData[x])

        sdbConnection = iquery()
        sdbStatements = Statements(sdbConnection)
                
        yOffSet = offset
        for partitions, partitionBandArray in enumerate(np.array_split(bandArray, 10, axis=0)):
             outName = "junk_%s_%s" % (counter, partitions)
             outFileDir = "/".join(binaryPartitionPath.split('/')[:-1])
             sdbStatements.CreateLoadArray(outName, 'id:int32', 2)
             outFilePath = "/%s/%s.sdb" % ('mnt', outName)
             #print(outFilePath)

             ArrayToBinary(partitionBandArray, outFilePath, 'mask', yOffSet)
           
             loadPath = "/data/04489/dhaynes/%s.sdb" % (outName)
             sdbStatements.LoadOneDimensionalArray( counter, outName, 'id:int32', 1, loadPath)
             print("Redimension node: %s, Array Partition: %s, offset: %s" %(counter,partitions, yOffSet))
             query = "insert(redimension(apply({A}, x, x1, y, y1, value, id), {B} ), {B})".format( A=outName, B='mask')

             sdbConnection.query(query)
             os.remove(outFilePath)
             sdbConnection.queryAFL("remove(%s)" % (outName))
             yOffSet += partitionBandArray.shape[0]

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
    with open(binaryFilePath, 'ab') as fileout:
        #Oneliner that creates the column index. Pull out [y for y in range(col)] to see how it works
        column_index = np.array(np.repeat([y for y in np.arange(0+yOffSet, col+yOffSet) ], row), dtype=np.dtype('int64'))
        
        #Oneliner that creates the row index. Pull out the nested loop first: [x for x in range(row)]
        #Then pull the full list comprehension: [[x for x in range(row)] for i in range(col)]
        row_index = np.array(np.concatenate([[x for x in range(row)] for i in range(col)]), dtype=np.dtype('int64'))

        #Oneliner for writing out the file
        #Add this to make it a csv tofile(binaryFilePath), "," and modify the open statement to 'w'
        np.core.records.fromarrays([column_index, row_index, theArray.ravel()], dtype=[('x','int64'),('y','int64'),(attributeName,theArray.dtype)]).ravel().tofile(binaryFilePath) 

    
    del column_index, row_index, theArray

def RasterizationDecider(theRasterizationMetaData, theRasterClass):
    """
    This function will determine if the rasterization can use the parallel load or another strategy
    """
    rasterTotalPixels = 0
    maxRasterizationPixels = []
    for c in theRasterizationMetaData:
        x, y, height, width, pixel_1, pixel_2, projection, vectorPath, counter, offset, dataStorePath = ParamSeperator(c)
        totalPixels = height*width
        rasterTotalPixels += totalPixels
        maxRasterizationPixels.append(totalPixels)

    largestRasterization = max(maxRasterizationPixels)

    if largestRasterization > 5000000:
        
        #Create mask array
        numDimensions = theRasterClass.CreateMask('int32', 'mask') #This is hardcoded for int32
        return 1
    else:
        return 0



def ParallelRasterization(coordinateData, theRasterClass=None):
    """

    """

    bigRaster = 0 #RasterizationDecider(coordinateData, theRasterClass)
  

    pool = mp.Pool(len(coordinateData))

    try:
        arrayData = pool.imap(BigRasterization, (c for c in coordinateData)  )
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
        return (arraydatatypes)


