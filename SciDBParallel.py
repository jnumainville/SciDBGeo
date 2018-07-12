import timeit, itertools, csv, math
import multiprocessing as mp
import numpy as np
from collections import OrderedDict
from osgeo import gdal
from gdalconst import GA_ReadOnly

class RasterLoader(object):

    def __init__(self, RasterPath, scidbArray, attribute, chunksize, dataStorePath, tiles=None, yOffSet=None, destinationArray=None):
        """
        Initialize the class RasterReader
        """
        
        self.width, self.height, self.datatype, self.numbands = self.GetRasterDimensions(RasterPath)
        self.AttributeNames = attribute
        self.GetSciDBInstances()
        self.AttributeString, self.RasterArrayShape = self.RasterShapeLogic(attribute)
        self.chunksize = chunksize
        hdataset = np.arange(self.height)

        self.RasterMetadata = {node: {"node": node, "y_min": min(heightRange), "y_max": max(heightRange),"height": len(heightRange), \
        "width": self.width ,"datastore": dataStorePath, "filepath": RasterPath, "attribute": self.AttributeString, \
        "array_shape": self.RasterArrayShape, "destination_array": destinationArray} for node, heightRange in enumerate(np.array_split(hdataset,self.SciDB_Instances)) }
        #self.RasterReadingData = self.CreateArrayMetadata(scidbArray, widthMax=self.width, heightMax=self.height, widthMin=0, heightMin=yOffSet, chunk=chunksize, tiles=tiles, attribute=self.AttributeString, band=self.numbands )
        
    def RasterShapeLogic(self, attributeNames):
        """
        This function will provide the logic for determining the shape of the raster
        The attributeNames variable is a list of attributes
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

    def GetSciDBInstances(self):
        """

        """
        from scidb import iquery
        sdb = iquery()
        query = sdb.queryAFL("list('instances')")
        self.SciDB_Instances = len(query.splitlines())-1
            
            
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
            #print(rasterValueDataType)
            
            del raster
        
        elif type(thePath).__module__ == 'numpy':
            rasterValueDataType = thePath.dtype
            height, width = thePath.shape
            numbands = 1

        print(width, height, rasterValueDataType, numbands)

        return (width, height, rasterValueDataType, numbands)

    def CreateDestinationArray(self, rasterArrayName, height, width, chunk):
        """
        Function creates the final destination array.
        Updated to handle 3D arrays.
        """
        
        import scidb
        sdb = scidb.iquery()
        
        
        if self.RasterArrayShape <= 2:
            #sdb.query("create array %s <%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, self.AttributeString , height-1, chunk, width-1, chunk) )
            myQuery = "create array %s <%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, self.AttributeString , height-1, chunk, width-1, chunk)
        else:
            #sdb.query("create array %s <%s> [band=0:%s,1; y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, self.AttributeString , len(self.datatype)-1, height-1, chunk, width-1, chunk) )
            myQuery = "create array %s <%s> [band=0:%s,1,0; y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, self.AttributeString , self.numbands-1, height-1, chunk, width-1, chunk)
        
        try:
            #print(myQuery)
            sdb.query(myQuery)
            #print("Created array %s" % (rasterArrayName))
        except:
            print("*****  Array %s already exists. Removing ****" % (rasterArrayName))
            sdb.query("remove(%s)" % (rasterArrayName))
            sdb.query(myQuery)
            #sdb.query("create array %s <%s:%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, self.AttributeString, height-1, chunk, width-1, chunk) )

        del sdb 
    
    def CreateLoadArray(self, tempRastName, attribute_name, rasterArrayType):
        """
        Create the loading array
        """
        
        import scidb
        sdb = scidb.iquery()

        if rasterArrayType <= 2:
            theQuery = "create array %s <y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)
        elif rasterArrayType == 3:
            theQuery = "create array %s <z1:int64, y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)
        
        try:
            #print(theQuery)
            sdb.query(theQuery)
            #sdb.query("create array %s <y1:int64, x1:int64, %s:%s> [xy=0:*,?,?]" % (tempRastName, attribute_name, rasterValueDataType) )
        except:
            #Silently deleting temp arrays
            sdb.query("remove(%s)" % (tempRastName))
            sdb.query(theQuery)

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

    def InsertRedimension(self, tempRastName, destArray, minY, minX):
        """
        First part inserts the boundary array into larger global mask array
        """
        start = timeit.default_timer()
        sdbquery ="insert(redimension(apply({A}, x, x1+{xOffSet}, y, y1+{yOffSet}, value, id), {B} ), {B})".format( A=tempRastName, B=destArray, yOffSet=minY, xOffSet=minX)
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
    #print("Rasterizing Vector in Parallel")

    x, y, height, width, pixel_1, pixel_2, projection, vectorPath, counter, offset, dataStorePath = ParamSeperator(inParams)
    # print(offset, x, y)
    outTransform= [x, pixel_1, 0, y, 0, pixel_2 ]
    memDriver = gdal.GetDriverByName('MEM')

    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()

    binaryPartitionPath = "%s/%s/p_zones.scidb" % (dataStorePath, counter)
    if os.path.exists(binaryPartitionPath):
         print("****Removing file****")
         os.remove(binaryPartitionPath)

    #Generate an array of elements the length of raster height
    hdataset = np.arange(height)
    if height * width > 50000000:
        #This is the very big rasterization process
        for p, h in enumerate(np.array_split(hdataset,100)):
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
        np.core.records.fromarrays([column_index, row_index, theArray.ravel()], dtype=[('y','int64'),('x','int64'),(attributeName,theArray.dtype)]).ravel().tofile(fileout) 

    
    del column_index, row_index, theArray



def ParallelRasterization(coordinateData, theRasterClass=None):
    """

    """

    #bigRaster = 0 #RasterizationDecider(coordinateData, theRasterClass)
  

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


def ParallelLoad(rasterReadingMetadata):
    """

    """
    pool = mp.Pool(len(rasterReadingMetadata))

    #ArraySplicerLogic()    
    try:
        loadLoops = ArraySplicerLogic(rasterReadingMetadata[0]['width'], rasterReadingMetadata[0]['height'], 3000000)
        print("Number of loops for loading: %s" % (loadLoops))
        nodeLoopData = AdjustMetaData(loadLoops, rasterReadingMetadata)
        #{r:{"loop": l, "height": h} for r in rasterReadingMetadata.keys() for l, h in enumerate(np.array_split(rasterReadingMetadata[r]["height"],loops))}
            #arrayHeight, arrayWidth  = rArray.shape
        #print(nodeLoopData)
        # for n in nodeLoopData:
        #     print(nodeLoopData[n])
        
        quit()
        pool.imap(Read_Write_Raster, (nodeLoopData[n] for n in nodeLoopData)  ) #
        pool.close()
        pool.join()
    except Exception as e:
        print(e)
        print("Error")

def AdjustMetaData(loops, theRMD):
    """
    Function to adjust the metadata for proper loading
    This function returns the adjust load dimensions for each iteration
    """
    adjustedData = {(l,r): {"node": theRMD[r]["node"], "y_min": theRMD[r]["y_min"], "y_max": theRMD[r]["y_max"], 
    "height": theRMD[r]["height"] , "width": theRMD[r]["width"], "datastore": theRMD[r]["datastore"], 
    "filepath": theRMD[r]["filepath"], "array_shape": theRMD[r]["array_shape"], "destination_array": theRMD[r]["destination_array"], 
    "attribute": theRMD[r]["attribute"], "xoff": 0, "yoff": int(theRMD[r]["y_min"] + min(h)), "ysize":len(h), "xsize": theRMD[r]["width"] } 
    for r in theRMD for l, h in enumerate(np.array_split(np.arange(theRMD[r]["height"]),loops))}
    #rArray = raster.ReadAsArray(xoff=0, yoff=int(yOffSet+min(h) ), xsize=rDict["width"], ysize=len(h))

    #OrderedDict()
    sortedDict = OrderedDict( [ (r,adjustedData[r]) for r in sorted(adjustedData.keys())] )
    #print(sortedDict)
        
        # for l, h in enumerate(np.array_split(np.arange(theRMD[r]["height"]),loops)):
        #     print(r,len(h))
            # print(theRMD[r])
            

    return sortedDict

    


def Read_Write_Raster(rDict):
    """

    """
    
    import os
    import numpy as np
    from osgeo import gdal
    from gdalconst import GA_ReadOnly

    print("Node %s, array size h:%s, w:%s ,totalpixles: %s " % (rDict["node"],rDict["height"], rDict["width"], rDict["height"] * rDict["width"]))

    raster = gdal.OpenShared(rDict["filepath"], GA_ReadOnly)

    
    binaryPartitionPath = r"%s/%s/pdataset.scidb" % (rDict["datastore"], rDict["node"])
    if os.path.exists(binaryPartitionPath):
         print("****Removing file****")
         os.remove(binaryPartitionPath)

    if rDict["height"] * rDict["width"] < 5000000:
        
        rArray = raster.ReadAsArray(xoff=0, yoff=int(rDict["y_min"]), xsize=rDict["width"], ysize=rDict["height"])
        ArrayToBinary(rArray, binaryPartitionPath, 'data_array', rDict["y_min"])
    
    else:
        #Generate an array of elements the length of raster height
        hdataset = np.arange(rDict["height"])
        yOffSet = int(rDict["y_min"])
        
        
        binaryPartitionPath = r"%s/%s/pdataset.scidb" % (rDict["datastore"], rDict["node"])
        if os.path.exists("/data/projects/services/scidb/scidbtrunk/stage/DB-mydb/0/%s/pdataset.scidb" % rDict["node"]): 
            print("****Removing file****")
            os.remove("/data/projects/services/scidb/scidbtrunk/stage/DB-mydb/0/%s/pdataset.scidb" % rDict["node"])
        
        for l, h in enumerate(np.array_split(hdataset,300)):
             
            print("Node: %s Writing: %s of 300, height: %s , OffSet: %s" % (rDict["node"], l+1, len(h), yOffSet + min(h)  ))
            rArray = raster.ReadAsArray(xoff=0, yoff=int(yOffSet+min(h) ), xsize=rDict["width"], ysize=len(h))
            arrayHeight, arrayWidth  = rArray.shape
                
            #print("%s,%s,%s,%s,%s,%s" % (rDict["node"], l, arrayHeight, arrayWidth, len(h), yOffSet ))
            ArrayToBinary(rArray, binaryPartitionPath, 'data_array', yOffSet+min(h) )

    # elif rDict["height"] * rDict["width"] > 50000000 :
    #     from scidb import iquery
    #     from scidb import Statements
    #     print("Massive Array")

    #     if rDict["destination_array"]:
                            
    #         sdb = iquery()
    #         sdb_statements = Statements(sdb)
            
    #         loadAttribute = "%s_1:%s" % (rDict["attribute"].split(":")[0], rDict["attribute"].split(":")[1])
    #         hdataset = np.arange(rDict["height"])
    #         yOffSet = int(rDict["y_min"])

    #         for l, h in enumerate(np.array_split(hdataset,800)):

    #             binaryPartitionPath = r"%s/%s/pdataset.scidb" % (rDict["datastore"], rDict["node"])
    #             if os.path.exists("/data/projects/services/scidb/scidbtrunk/stage/DB-mydb/0/%s/pdataset.scidb" % rDict["node"]): 
    #                 print("****Removing file****")
    #                 os.remove("/data/projects/services/scidb/scidbtrunk/stage/DB-mydb/0/%s/pdataset.scidb" % rDict["node"])
                 
    #             print("Node: %s Writing: %s of 800, height: %s, OffSet: %s" % (rDict["node"], l+1, len(h), yOffSet + min(h)  ))
    #             rArray = raster.ReadAsArray(xoff=0, yoff=int(yOffSet+min(h) ), xsize=rDict["width"], ysize=len(h))
    #             arrayHeight, arrayWidth  = rArray.shape
    #             print("Writing Array")

    #             #print("%s,%s,%s,%s,%s,%s" % (rDict["node"], l, arrayHeight, arrayWidth, len(h), yOffSet ))
    #             ArrayToBinary(rArray, binaryPartitionPath, 'data_array', yOffSet+min(h) )
                
    #             arrayName = "Load_%s" % (rDict["node"])
    #             print("Loading array: %s" % (arrayName))
                 
    #             sdb_statements.CreateLoadArray(arrayName, loadAttribute, rDict["array_shape"]) 
    #             sdb_statements.LoadOneDimensionalArray(rDict["node"], arrayName, loadAttribute, rDict["array_shape"], 'pdataset.scidb')
    #             print("Loaded %s" % (arrayName))

    #             print("Redimensioning")
    #             sdb_statements.InsertRedimension( arrayName, rDict["destination_array"], oldvalue=loadAttribute.split(":")[0], newvalue='value')
    #             print("Inserted %s into Destination Array" % (arrayName)  )
    #             sdb.query("remove(%s)" % (arrayName))

              
    del raster

def ArraySplicerLogic(width, height, maxPixels=20000000):
    """
    This function will determine how many loops are necessary to efficiently load a large geoTiff
    maxPixels should be determined based on memory constraints of your machine
    """
    
    if height * width < maxPixels:
        return 1
    elif width > maxPixels:
        return height
    else:
        possibles = OrderedDict([(h, h * width) for h in range(height)])
        solutions = {k: v for k, v in possibles.items() if v <= maxPixels and k > 0}
        #print(solutions)
        numIterations = height / max({k: v for k, v in solutions.items()})
        return round(numIterations)

            #OrderedDict([  ] )