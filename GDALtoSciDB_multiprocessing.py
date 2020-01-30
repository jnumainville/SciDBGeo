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
        """
        
        self.width, self.height, self.datatype, self.numbands = self.GetRasterDimensions(RasterPath)
        self.AttributeNames = attribute
        self.AttributeString, self.RasterArrayShape = self.RasterShapeLogic(attribute)
        self.RasterMetadata = self.CreateArrayMetadata(scidbArray, self.width, self.height, chunksize, tiles, self.AttributeString, self.numbands )
        self.CreateDestinationArray(scidbArray, self.height, self.width, chunksize)

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

        raster = gdal.Open(thePath, GA_ReadOnly)
        
        if raster.RasterCount > 1:
            rArray = raster.ReadAsArray(xoff=0, yoff=0, xsize=1, ysize=1)
            rasterValueDataType = rArray[0][0].dtype

        elif raster.RasterCount == 1:
            rasterValueDataType = raster.ReadAsArray(xoff=0, yoff=0, xsize=1, ysize=1)[0].dtype

        if rasterValueDataType == 'float32': rasterValueDataType = 'float'
        numbands = raster.RasterCount
        width = raster.RasterXSize 
        height  = raster.RasterYSize
        print(rasterValueDataType)
        
        del raster

        return (width, height, rasterValueDataType, numbands)
    
    def CreateDestinationArray(self, rasterArrayName, height, width, chunk):
        """
        Function creates the final destination array.
        Updated to handle 3D arrays.
        """
        
        import scidb
        sdb = scidb.iquery()
        
        
        if self.RasterArrayShape <= 2:
            myQuery = "create array %s <%s> [y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, self.AttributeString , height-1, chunk, width-1, chunk)
        else:
            myQuery = "create array %s <%s> [band=0:%s,1,0; y=0:%s,%s,0; x=0:%s,%s,0]" %  (rasterArrayName, self.AttributeString , self.numbands-1, height-1, chunk, width-1, chunk)
        
        try:
            sdb.query(myQuery)
        except:
            print("*****  Array %s already exists. Removing ****" % (rasterArrayName))
            sdb.query("remove(%s)" % (rasterArrayName))
            sdb.query(myQuery)

        del sdb 


    def CreateArrayMetadata(self, theArray, width, height, chunk=1000, tiles=1, attribute='value', band=1):
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

                RasterReads[str(version_num)] = OrderedDict([ ("xOffSet",xOffSet), ("yOffSet",yOffSet), \
                    ("xWindow", columnsRemaining), ("yWindow", rowsRemaining), ("version", version_num), ("array_type", self.RasterArrayShape), \
                    ("attribute", attribute), ("scidbArray", theArray), ("y_version", y_version), ("chunk", chunk), ("bands", band) ])

            
            rowMax += math.ceil(width/(chunk*tiles))

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
    I think redimensioning affects the performance of the load if they run concurrently
    """

    theData = theMetadata.RasterMetadata
    for theKey in theMetadata.RasterMetadata.keys():
        start = timeit.default_timer()
        tempArray = "temprast_%s" % (theData[theKey]['version'])
        RedimensionAndInsertArray(sdb, tempArray, theData[theKey]['scidbArray'], theData[theKey]['array_type'], theData[theKey]['xOffSet'], theData[theKey]['yOffSet'])    
        stop = timeit.default_timer()
        redimensionTime = stop-start
        
        RemoveTempArray(sdb, tempArray)
        print("Inserted version %s of %s" % (theData[theKey]['version'], theData[theKey]["Loops"] ))
        dataLoadingTime = (redimensionTime * theData[theKey]["Loops"]) / 60 
        if theData[theKey]['version'] == 0: print("Estimated time for redimensioning the dataset in minutes %s: RedimensionTime: %s seconds" % ( dataLoadingTime, redimensionTime))
        if theData[theKey]['version'] > 1: 
            versions = sdb.versions(theData[theKey]['scidbArray'])
            if len(versions) > 1: 
                print("Versions you could remove: %s" % (versions))
                for v in versions[:-1]:
                    try:
                        sdb.query("remove_versions(%s, %s)" % (theData[theKey]['scidbArray'], v) )
                    except:
                        print("Couldn't remove version %s from array %s" % (v, theData[theKey]['scidbArray']) )
        
        #Add the redimension time to the TimeDictionary
        theDict[ str(theData[theKey]['version']) ]['redimensionTime'] = redimensionTime

    return theDict
# @profile
def GDALReader(inParams):
    """
    This is the main worker function.
    Split up Loading and Redimensioning. Only Loading is multiprocessing
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

    print("xoffset: %s, yOffSet: %s, xWindow: %s, yWindow: %s " % (theMetadata['xOffSet'], theMetadata['yOffSet'],theMetadata['xWindow'],theMetadata['yWindow']))

    raster = gdal.Open(theRasterPath, GA_ReadOnly)
    if bandIndex:
        #This code is for multibanded arrays, with z (band) dimension.
        print("**** Reading band %s" % (bandIndex))
        band = raster.GetRasterBand(bandIndex)
        array = band.ReadAsArray(xoff=theMetadata['xOffSet'], yoff=theMetadata['yOffSet'], win_xsize=theMetadata['xWindow'], win_ysize=theMetadata['yWindow'])
        rasterBinaryFilePath = "%s/band%s_%s.sdbbin" % (theSciDBOutPath, bandIndex, tempArray)
        rasterBinaryLoadPath = "%s/band%s_%s.sdbbin" % (theSciDBLoadPath,bandIndex, tempArray)
        tempArray = "temprast_band%s_%s" % (bandIndex, theMetadata['version'])
    else:
        array = raster.ReadAsArray(xoff=theMetadata['xOffSet'], yoff=theMetadata['yOffSet'], xsize=theMetadata['xWindow'], ysize=theMetadata['yWindow'])
    
    start = timeit.default_timer()
    WriteArray(array, rasterBinaryFilePath, theMetadata['array_type'], theMetadata['attribute'], bandIndex)
    stop = timeit.default_timer()
    writeTime = stop-start

    if theMetadata['array_type'] == 2:                
        items = ["%s:%s" % (attribute.split(":")[0].strip()+"1", attribute.split(":")[1].strip()) for attribute in theMetadata['attribute'].split(",")  ]
        pseudoAttributes = ", ".join(items)
    else:
        pseudoAttributes = "%s:%s" % (theMetadata['attribute'].split(":")[0].strip()+"1", theMetadata['attribute'].split(":")[1].strip())
        
    os.chmod(rasterBinaryFilePath, 0o755)
    #Support multiple attributes or 2D and 3D arrays
    sdb_statements.CreateLoadArray(tempArray, theMetadata['attribute'], theMetadata['array_type'])
    start = timeit.default_timer()

        
    if sdb_statements.LoadOneDimensionalArray(theInstance, tempArray, pseudoAttributes, theMetadata['array_type'], rasterBinaryLoadPath):
        stop = timeit.default_timer()
        loadTime = stop-start

        dataLoadingTime = ((writeTime + loadTime) * theMetadata["loops"]) / 60 
        if theMetadata['version'] == 0: print("Estimated time for loading in minutes %s: WriteTime: %s, LoadTime: %s" % ( dataLoadingTime, writeTime, loadTime))

        #Clean up
        gc.collect()

        RedimensionAndInsertArray(sdb, tempArray, theMetadata['scidbArray'], theMetadata['array_type'], theMetadata['xOffSet'], theMetadata['yOffSet'])
        
        return (theMetadata['version'], writeTime, loadTime)

    else:
        print("Error Loading")
        return (theMetadata['version'], -999, -999)
    
    
def ArrayDimension(anyArray):
    """
    Return the number of rows and columns for 2D or 3D array
    """
    if anyArray.ndim == 2:
        return anyArray.shape
    else:
        return anyArray.shape[1:]
    
def WriteArray(theArray, binaryFilePath, arrayType, attributeName='value', bandID=0):
    """
    This function uses numpy tricks to write a numpy array in binary format with indices 
    """
    print("Writing %s" % (binaryFilePath))
    col, row = ArrayDimension(theArray)
    with open(binaryFilePath, 'wb') as fileout:

        #Oneliner that creates the column index. Pull out [y for y in range(col)] to see how it works
        column_index = np.array(np.repeat([y for y in range(col)], row), dtype=np.dtype('int64'))
        
        #Oneliner that creates the row index. Pull out the nested loop first: [x for x in range(row)]
        #Then pull the full list comprehension: [[x for x in range(row)] for i in range(col)]
        row_index = np.array(np.concatenate([[x for x in range(row)] for i in range(col)]), dtype=np.dtype('int64'))
        
        if arrayType == 3:
            #Raster has multiple attributes, with a z (band) dimension
            #Raster band reader for GDAL begins as 1, but the dimension range is 0:numbands-1
            band_index = np.array([bandID-1 for c in column_index])

            np.core.records.fromarrays([band_index, column_index, row_index, theArray.ravel()], \
                dtype=[('band','int64'),('x','int64'),('y','int64'),(attributeName.split(":")[0],theArray.dtype)]).ravel().tofile(binaryFilePath) 

        elif arrayType == 2:
            #Raster has multiple attributes, but it is 2D
            #Making a list of attribute names with data types
            #numbands > 1 and attributeName.find(",") == -1:
            
            attributesList = [('x','int64'),('y','int64')]
            for dim, name in enumerate(attributeName.split(",")):
                attName, attType = name.split(":")
                attributesList.append( (attName.strip(), attType.strip()) )
            
            #Making a list of numpy arrays
            arrayList = [column_index, row_index]
            for attArray in np.array_split(theArray, dim+1, axis=0):
                arrayList.append(attArray.reshape(attArray.shape[1:]).ravel())
            
            np.core.records.fromarrays(arrayList, dtype=attributesList ).ravel().tofile(binaryFilePath)
            
        elif arrayType == 1:
            #A single band GeoTiff
            np.core.records.fromarrays([column_index, row_index, theArray.ravel()], dtype=[('x','int64'),('y','int64'),(attributeName,theArray.dtype)]).ravel().tofile(binaryFilePath) 
             
        else:
            print("ERROR")

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

def CreateLoadArray(sdb, tempRastName, attribute_name, rasterArrayType):
    """
    Create the loading array
    """
    
    if rasterArrayType <= 2:
        theQuery = "create array %s <y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)
    elif rasterArrayType == 3:
        theQuery = "create array %s <z1:int64, y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)
    
    try:
        sdb.query(theQuery)
    except:
        #Silently deleting temp arrays
        sdb.query("remove(%s)" % (tempRastName))
        sdb.query(theQuery)

def RedimensionAndInsertArray(sdb, tempArray, SciDBArray, RasterArrayShape, xOffSet, yOffSet):
    """
    Function for redimension and inserting data from the temporary array into the destination array
    """
    
    if RasterArrayShape <= 2:
        #Raster has one or more attributes but the array is 2D
        query = "insert(redimension(apply( %s, y, y1+%s, x, x1+%s ), %s ), %s)" % (tempArray, yOffSet, xOffSet, SciDBArray, SciDBArray)
        
    elif RasterArrayShape == 3:
        #Raster has multiple attributes, with a z (band) dimension in 3D
        query = "insert(redimension(apply(%s, band, z1, y, y1+%s, x, x1+%s ), %s ), %s)" % (tempArray, yOffSet, xOffSet, SciDBArray, SciDBArray)

    try:    
        sdb.query(query)
    except:
        print("Failing on inserting data into array")
        print(query)
        
def RemoveTempArray(sdb, tempRastName):
    """
    Remove the temporary array
    """
    sdb.query("remove(%s)" % (tempRastName))


def GetNumberofSciDBInstances():
    import scidb
    sdb = scidb.iquery()

    query = sdb.queryAFL("list('instances')")
    #There is a header?
    numInstances = len(query.splitlines())-1
    numInstances = list( range(numInstances ) )
    return numInstances


def MultiProcessLoading(Rasters, rasterFilePath, SciDBOutPath, SciDBLoadPath):
    """
    This function creates the pool based upon the number of SciDB instances and the generates the parameters for each Python instance
    """
    SciDBInstances = GetNumberofSciDBInstances()
    pool = mp.Pool(len(SciDBInstances), maxtasksperchild=1)    

    aKey = list(Rasters.RasterMetadata.keys())[0]
    try:
        if not Rasters.RasterMetadata[aKey]['iterate']:
            results = pool.imap(GDALReader, (r for r in Rasters.GetMetadata(SciDBInstances, rasterFilePath, SciDBOutPath, SciDBLoadPath, 0)  ))
        else:
            for bandNum in range(1, Rasters.RasterMetadata[aKey]['iterate']+1):
                #This works fine, check your EverNote notes for oddities. Loading Data into SciDB
                results = pool.imap(GDALReader, (r for r in Rasters.GetMetadata(SciDBInstances, rasterFilePath, SciDBOutPath, SciDBLoadPath, bandNum)  ))
    except:
        print(mp.get_logger())

    timeDictionary  = {str(i[0]):{"version": i[0], "writeTime": i[1], "loadTime": i[2] } for i in results}
    return timeDictionary
        
    pool.close()
    pool.join()


def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "multiprocessing module for loading GDAL read data into SciDB with multiple instances")
    #If host = NoSHIM, then use the cmd iquery   
    parser.add_argument("-r", required=True, help="Input file path for the raster", dest="rasterPath")    
    parser.add_argument("-a", required=True, help="Name of the destination array", dest="arrayName")
    parser.add_argument("-n", required=True, nargs='*', help="Name of the attribute(s) for the destination array", dest="attributes", default="value")
    parser.add_argument("-t", required=False, type=int, help="Size in rows of the read window, default: 8", dest="tiles", default=8)
    parser.add_argument("-c", required=False, type=int, help="Chunk size for the destination array, default: 1,000", dest="chunk", default=1000)
    parser.add_argument("-o", required=False, type=int, help="Chunk overlap size. Adding overlap increases data loading time. default: 0", dest="overlap", default=0)
    parser.add_argument("-TempOut", required=False, default='/home/scidb/scidb_data/0/0', dest='OutPath')
    parser.add_argument("-SciDBLoadPath", required=False, default='/home/scidb/scidb_data/0/0', dest='SciDBLoadPath')
    parser.add_argument("-csv", required =False, help="Create CSV file", dest="csv", default="None")
    parser.add_argument("-p", required =False, help="Parallel Redimensioning", dest="parallel", default="None")

    return parser


if __name__ == '__main__':
    #Main running function
    args = argument_parser().parse_args()
    start = timeit.default_timer()
    RasterInformation = RasterReader(args.rasterPath, args.arrayName, args.attributes, args.chunk, args.tiles)
    ## Debugger to see the metadata for SciDB loading.

    timeDictionary = MultiProcessLoading(RasterInformation, args.rasterPath, args.OutPath, args.SciDBLoadPath)
    if not args.parallel:
        allTimesDictionary = GlobalRasterLoading(args.host, RasterInformation, timeDictionary)


   
