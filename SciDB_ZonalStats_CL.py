# -*- coding: utf-8 -*-
"""
Created on Fri Dec 02 15:02:14 2016
A command line tool for conducting Zonal Statistics in SciDB

@author: dahaynes
"""


from osgeo import ogr, gdal
import scidbpy, timeit, csv, argparse, os, re
from collections import OrderedDict

def world2Pixel(geoMatrix, x, y):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the pixel location of a geospatial coordinate
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

def OptimalZonalStats(sdb, SciDBArray, polygonSciDBArray, minY, minX):
    afl = sdb.afl
    theArray = afl.show(SciDBArray)
    theArray.contents()

    tempArray = "junkArray"
    
    #"SciDBArray()\n[('GLC2000<value:uint8> [y=0:40319:0:1000; x=0:16352:0:1000]')]\n"
    #Get regular expressions match
    ydimension = r"\[y=[0-9]*:[0-9]*:[0-9]*:[0-9]*"
    xdimension = r"x=[0-9]*:[0-9]*:[0-9]*:[0-9]*\]"
    pattern= r"(SciDBArray\(\)\n\[\(')(%s)(<value:)([a-z0-9]*>)(\s)*(%s)(;\s)(%s)" % (SciDBArray, ydimension, xdimension)
    matches = re.match(pattern, theArray.contents())
    
    start = timeit.default_timer()   
    #create empty array
    sciquery = ("create array %s %s%s %s; %s" % (tempArray, matches.groups()[2], "int16>", matches.groups()[5], matches.groups()[7]) )
    print(sciquery)
    sdb.query(sciquery)

    #write array in the correct spot
    sciquery ="insert(redimension(apply({A}, x, i0+{yOffSet}, y, i1+{xOffset}, value, f0), {B} ), {B})".format( A=polygonSciDBArray.name, B=tempArray, yOffSet=minY, xOffset=minX)
    print(sciquery)
    sdb.query(sciquery)
    stop = timeit.default_timer()

    insertTime = stop-start
    print(insertTime)
    #limit(insert(redimension(apply(py1496072375503176173_00001, x, i0+4548, y, i1+6187, value, f0), junkArray ), junkArray), 100);

    #py1496072375503176173_00001,
    #py1496075988591369127_00001
    # AFL% show(py1496075988591369127_00001);
    # {i} schema
    # {0} 'py1496075988591369127_00001<f0:int16> [i0=0:2783:0:1000; i1=0:6474:0:1000]'

    # AFL% show(GLC2000);
    # {i} schema
    # {0} 'GLC2000<value:uint8> [y=0:40319:0:1000; x=0:16352:0:1000]'


    equi_join(between(GLC2000, 4548, 6187, 4548, 6197), between(py1496075988591369127_00001, 0, 0, 10, 10), 'left_ids=~0,~1', 'right_ids=~0,~1');

    limit(equi_join(between(GLC2000, 4548, 6187, 7331, 12661), apply(py1496075988591369127_00001, x, i0+4548, y, i1+6187, value, f0), 'left_ids=~0,~1', 'right_ids=~0,~1'), 20);
    limit(equi_join(between(GLC2000, 4548, 6187, 7331, 12661), apply(py1496075988591369127_00001, x, i0+4548, y, i1+6187, value, f0), 'left_ids=~0,~1', 'right_names=x,y'), 20);

    equi_join(between(GLC2000, 4548, 6187, 7331, 12661), apply(py1496075988591369127_00001, x, i0+4548, y, i1+6187, value, f0), 'left_names=x,y', 'right_names=x,y');

    grouped_aggregate(equi_join(between(GLC2000, 4548, 6187, 7331, 12661), apply(py1496075988591369127_00001, x, i0+4548, y, i1+6187, id, f0), 'left_ids=~0,~1', 'right_ids=~0,~1'), min(value), max(value), avg(value), count(value), id);

    grouped_aggregate(equi_join(between(GLC2000, 4548, 6187, 7331, 12661), apply(py1496075988591369127_00001, x, i0+4548, y, i1+6187, id, f0), 'left_names=x,y', 'right_names=x,y'), min(value), max(value), avg(value), count(value), id);
    #apply(py1496075988591369127_00001, x, i0+4548, y, i1+6187, id, f0)
    equi_join(between(GLC2000, 0, 0, 10, 10), between(py1496075988591369127_00001, 0, 0, 10, 10), 'left_ids=~0,~1', 'right_names=~0,~1');

    #equi_join(between(GLC2000, 4548, 6187, 4548, 6197), between(py1496075988591369127_00001, 0, 0, 10, 10), 'left_names=value', 'right_names=f0');

    #'left_ids=~0,0', 'right_ids=1,0'

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
            #if counter < 100: print("y/column: %s, x/row: %s" % (col + yOffset, row + xOffset))
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
    "Function to perform the Zonal Analysis can get back the results"
    afl = sdb.afl
    result = afl.grouped_aggregate(afl.join(polygonSciDBArray.name, afl.subarray(SciDBArray, ulY, ulX, lrY, lrX)), max("value"), "f0")
    #query = "grouped_aggregate(join(%s,subarray(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), f0)" % (polygonSciDBArray.name, SciDBArray, ulY, ulX, lrY, lrX)

def LoadPolygonArray(sdb, tempRastName, rasterValueDataType, tempSciDBLoad, ulY, lrY, ulX, lrX, chunk_size=1000):
    """ 
    This function will be used instead of the from array function in scidbpy
    """
    rasterArrayName = "mask"

    try:
        sdbquery = "create array %s <id:%s> [y=%s:%s,%s,0; x=%s:%s,%s,0]" %  (rasterArrayName, rasterValueDataType, ulY, lrY, chunk_size, ulX, lrX, chunk_size)
        print(sdbquery)
        sdb.query(sdbquery)
        binaryLoadPath = '%s/%s.scidb' % (tempSciDBLoad,tempRastName )
        sdbquery = "create array %s <x1:int64, y1:int64, id:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType)
        print(sdbquery)
        sdb.query(sdbquery)
        sdbquery = "load(%s,'%s', -2, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, rasterValueDataType)
        print(sdbquery)
        sdb.query(sdbquery)
        sdbquery = "insert(redimension(apply( {A}, x, x1+{yOffset}, y, y1+{xOffset} ), {B} ), {B})".format(A=tempRastName, B=rasterArrayName, yOffset=ulY, xOffset=ulX)
        print(sdbquery)
        sdb.query(sdbquery)
    except:
        sdb.query("remove(%s)" % (rasterArrayName))
        sdb.query("remove(%s)" % (tempRastName))
        #LoadPolygonArray(sdb, tempRastName, rasterValueDataType, tempSciDBLoad, ulY, lrY, ulX, lrX, 1000)

def EquiJoin_SummaryStats(sdb, SciDBArray, tempRastName, rasterValueDataType, tempSciDBLoad, ulY, lrY, ulX, lrX, verbose=False):
    """
    Load the polygon array in as a 1D array, shifted over

    Example (equi_join(between(GLC2000, 4548, 6187, 7331, 12661)
    grouped_aggregate(equi_join(between(GLC2000, 4548, 6187, 7332, 12662), polygon), 'left_names=x,y', 'right_names=x,y'), min(value), max(value), avg(value), count(value), id)

    """

    binaryLoadPath = '%s/%s.scidb' % (tempSciDBLoad,tempRastName )
    try:
        sdbquery = "create array %s <x:int64, y:int64, id:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType)
        sdb.query(sdbquery)
    except:
        sdb.query("remove(%s)" % (tempRastName))
        sdbquery = "create array %s <x:int64, y:int64, id:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType)
        sdb.query(sdbquery)
    
    if verbose: print(sdbquery)

    start = timeit.default_timer()
    sdbquery = "load(%s,'%s', -2, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, rasterValueDataType)
    sdb.query(sdbquery)
    stop = timeit.default_timer()
    loadTime = stop-start
    if verbose: print(sdbquery , loadTime)
    
    start = timeit.default_timer()
    sdbquery = "grouped_aggregate(equi_join(between(%s, %s, %s, %s, %s), %s, 'left_names=x,y', 'right_names=x,y'), min(value), max(value), avg(value), count(value), id)" % (SciDBArray, ulY, ulX, lrY, lrX, tempRastName) 
    if verbose: print(sdbquery)
    sdb.query(sdbquery) 
    stop = timeit.default_timer()
    queryTime = stop-start

    return loadTime, queryTime


def ZonalStats(NumberofTests, boundaryPath, rasterPath, SciDBArray, statsMode=1, filePath=None, verbose=False):
    "This function conducts zonal stats in SciDB"
    
    outDictionary = OrderedDict()
    sdb = scidbpy.connect()

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
            print("ulX:%s, ulY:%s, lrX:%s, lrY:%s" % ( ulX, ulY, lrX, lrY))

        if statsMode == 1:
            #Transfering Raster Array to SciDB
            start = timeit.default_timer()
            polygonSciDBArray = sdb.from_array(rasterizedArray, instance_id=0, persistent=False, chunk_size=1000) 

            #polygonSciDBArray = sdb.from_array(rasterizedArray, dim_low=(4000,5000), dim_high=(5000,7000), instance_id=0, chunk_size=1000) 
            #name="states"

            stop = timeit.default_timer()
            transferTime = stop-start
            print(transferTime)

            #Raster Summary Stats
            query = "grouped_aggregate(join(%s,subarray(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), f0)" % (polygonSciDBArray.name, SciDBArray, ulY, ulX, lrY, lrX)
            start = timeit.default_timer()
            if verbose: print(query)
            results = sdb.query(query)
            stop = timeit.default_timer()
            queryTime = stop-start
    

        elif statsMode == 2:

            csvPath = '/home/scidb/scidb_data/0/0/polygon.scidb'
            WriteMultiDimensionalArray(rasterizedArray, csvPath, ulX, ulY )    
            tempRastName = csvPath.split('/')[-1].split('.')[0]
            tempSciDBLoad = '/'.join(csvPath.split('/')[:-1])
            #LoadPolygonArray(sdb, tempRastName, rasterValueDataType, tempSciDBLoad, ulY, lrY, ulX, lrX, 1000)
            transferTime, queryTime = EquiJoin_SummaryStats(sdb, SciDBArray, tempRastName, rasterValueDataType, tempSciDBLoad, ulY, lrY, ulX, lrX, verbose)
            #OptimalZonalStats(sdb, SciDBArray, polygonSciDBArray, ulY, ulX,)
        else:
            pass
        
        print("Zonal Analyis time %s, for file %s, Query run %s " % (queryTime, boundaryPath, t+1 ))
        if verbose: print("TransferTime: %s" % (transferTime)  )
        outDictionary[theTest] = OrderedDict( [ ("test",theTest), ("SciDBArrayName",SciDBArray), ("BoundaryFilePath",boundaryPath), ("transfer_time",transferTime), ("rasterization_time",rasterizeTime), ("query_time",queryTime), ("total_time",transferTime+rasterizeTime+queryTime) ] )
    

    sdb.reap()
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
    parser.add_argument('-v', required=False, action="store_true", dest='verbose')
    
    return parser

if __name__ == '__main__':
    args = argument_parser().parse_args()
    if CheckFiles(args.Shapefile, args.Raster):
        ZonalStats(args.Runs, args.Shapefile, args.Raster, args.SciArray, args.mode, args.CSV, args.verbose)
    # else:
    #     print(args)

