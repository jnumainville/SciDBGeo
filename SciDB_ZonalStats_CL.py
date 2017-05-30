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
def WriteFile(filePath, theDictionary):
    "This function writes out the dictionary as csv"
    
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

def ZonalStats(NumberofTests, boundaryPath, rasterPath, SciDBArray, optimized=False, filePath=None):
    "This function conducts zonal stats in SciDB"
    
    outDictionary = OrderedDict()
    
    for t in range(NumberofTests):
        theTest = "test_%s" % (t+1)
        #outDictionary[theTest]

        vectorFile = ogr.Open(boundaryPath)
        theLayer = vectorFile.GetLayer()
        geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()

        inRaster = gdal.Open(rasterPath)
        rasterTransform = inRaster.GetGeoTransform()

        start = timeit.default_timer()
        rasterizedArray = RasterizePolygon(rasterPath, r'/home/scidb/data/nothing.tiff', boundaryPath)
        stop = timeit.default_timer()
        rasterizeTime = stop-start
        print("Rasterization time %s for file %s" % (rasterizeTime, boundaryPath ))
        
        #Transfering Raster Array to SciDB
        sdb = scidbpy.connect()
        start = timeit.default_timer()
        polygonSciDBArray = sdb.from_array(rasterizedArray, instance_id=0, persistent=False)
        stop = timeit.default_timer()
        transferTime = stop-start
        

        ulX, ulY = world2Pixel(rasterTransform, geomMin_X, geomMax_Y)
        lrX, lrY = world2Pixel(rasterTransform, geomMax_X, geomMin_Y)
        if optimized:
            OptimalZonalStats(sdb, SciDBArray, polygonSciDBArray, ulY, ulX,)
        else:
            #afl = sdb.afl
            #afl.subarray()
            query = "grouped_aggregate(join(%s,subarray(%s, %s, %s, %s, %s)), min(value), max(value), avg(value), count(value), f0)" % (polygonSciDBArray.name, SciDBArray, ulY, ulX, lrY, lrX)
            start = timeit.default_timer()
            #print(query)
            results = sdb.query(query)
            stop = timeit.default_timer()
            queryTime = stop-start
    
            print("Zonal Analyis time %s, for file %s, Query run %s " % (queryTime, boundaryPath, t+1 ))

            outDictionary[theTest] = OrderedDict( [ ("test",theTest), ("SciDBArrayName",SciDBArray), ("BoundaryFilePath",boundaryPath), ("transfer_time",transferTime), ("rasterization_time",rasterizeTime), ("query_time",queryTime), ("total_time",transferTime+rasterizeTime+queryTime) ] )
            #outDictionary[theTest] = OrderedDict( [ ("test"), (theTest), "SciDBArrayName" : SciDBArray, "BoundaryFilePath": boundaryPath, "transfer_time": transferTime, "rasterization_time": rasterizeTime, "query_time": queryTime, "total_time": transferTime+rasterizeTime+queryTime ] )

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
    parser.add_argument('-CSV', required=False, dest='CSV')
    return parser



if __name__ == '__main__':
    args = argument_parser().parse_args()
    if CheckFiles(args.Shapefile, args.Raster):
        ZonalStats(args.Runs, args.Shapefile, args.Raster, args.SciArray, False, args.CSV)
    # else:
    #     print(args)

