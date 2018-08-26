from scidb import iquery, Statements
from SciDBParallel import *
import os, timeit, csv
from collections import OrderedDict

def ZonalStatistics(sdbConn, dataset, theRun, summaryStatsCSV=None):
    """
    This is the functions for zonal statistics
    Each function should be completely self contained
    """
    
    start = timeit.default_timer()
    raster = ZonalStats(dataset["shape_path"], dataset["raster_path"], dataset["array_table"])
    raster.RasterMetadata(dataset["raster_path"], dataset["shape_path"], raster.SciDBInstances, '/storage' ) 
    stopPrep = timeit.default_timer()
    print(raster.geoTiffPath, raster.SciDBArrayName)
    
    if theRun == 1:
        datapackage = ParallelRasterization(raster.arrayMetaData, raster)
        stopRasterization = timeit.default_timer()
        SciDBInstances = raster.SciDBInstances
    
    sdb_statements = Statements(sdbConn)
    
    theAttribute = 'id:%s' % (datapackage[0]) #
    sdb_statements.CreateLoadArray('boundary', theAttribute , 2)
    sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, 'p_zones.scidb')
    #SciDB Load operator -1 loads in parallel
    numDimensions = raster.CreateMask(datapackage[0], 'mask')
    redimension_time = raster.InsertRedimension( 'boundary', 'mask', raster.tlY, raster.tlX )

    summaryStatTime = raster.GlobalJoin_SummaryStats(raster.SciDBArrayName, 'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, 1, summaryStatsCSV)
    stopSummaryStats = timeit.default_timer()            
    
    timed = OrderedDict( [("connectionInfo", "XSEDE"), ("run", r), \
        ("array_table", d["array_table"]), ("boundary_table", d["shape_path"]), ("full_time", stopSummaryStats-start), \
        ("join_time", summaryStatTime), ("redimension_time", redimension_time), ("rasterize_time", stopRasterization-stopPrep) ])
    sdbConn.query("remove(mask)")
    sdbConn.query("remove(boundary)")
    del raster

    return timed

def FocalAnalysis(sdbConn, arrayTable):
    """
    This function perform a focal analysis on a SciDB Array
    """
    
    start = timeit.default_timer()
    #https://paradigm4.atlassian.net/wiki/spaces/scidb/pages/242745395/window
    query = "window(%s, 1,1,1,1, avg(value))" % (arrayTable)
    
    results = sdbConn.queryAFL(query)
    stop = timeit.default_timer()
    
    timed = OrderedDict( [("connectionInfo", "XSEDE"), ("run", r), ("analytic", "count"), ("time", stop-start), ("array_table", arrayTable), ("dataset", arrayTable.split("_")[:-1]), ("chunk", arrayTable.split("_")[0])])

    return timed

def CountPixels(sdbConn, arrayTable, pixelValue):
    """
    This function will return the sum of the pixels in a SciDB Array
    """
    
    start = timeit.default_timer()
    query = "SELECT count(value) from %s WHERE value = %s" % (arrayTable, pixelValue)
    
    results = sdbConn.aql_query(query)
    stop = timeit.default_timer()
    pixelCount = str(results.splitlines()[-1])
    print("Sum of pixel values %s for array: %s" % (pixelCount.split(" ")[-1],arrayTable) )
    
    timed = OrderedDict( [("connectionInfo", "XSEDE"), ("run", r), ("analytic", "count"), ("time", stop-start), ("array_table", arrayTable) ])

    return timed


def Reclassify(sdbConn, arrayTable, oldValue, newValue, run=1):
    """
    This function will return the sum of the pixels in a SciDB Array
    """
    
    start = timeit.default_timer()
    query = "aggregate(apply(%s, value2, iif(value = %s, %s, 0)), sum(value2))" % (arrayTable, oldValue, newValue)
    #query = "apply(%s, value2, iif(value = %s, %s, -999))" % (arrayTable, oldValue, newValue)
    
    results = sdbConn.query(query)
    stop = timeit.default_timer()
    Statement = Statements(sdbConn)
    if run == 1:
        Statement.CreateMask(arrayTable, "reclassedTable", "newvalue", "int64") 
        sdbConn.query("""insert(redimension(apply(%s, newvalue, iif(value = %s, %s, -99)), "reclassedTable"), "reclassedTable") """ %  (arrayTable, oldValue, newValue ) )
        stopInsert = timeit.default_timer() 
        insertTime =  stopInsert - stop
    else:
        insertTime = 0

    timed = OrderedDict( [("connectionInfo", "XSEDE"), ("run", r), ("analytic", "reclassify"), ("time", stop-start), ("array_table", arrayTable),  ("redimensionInsertTime",  insertTime) ])

    return timed

def localDatasetPrep():
    """

    """
    chunk_sizes = [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
    raster_tables = ["glc_2000_clipped", "meris_2010_clipped", "nlcd_2006"] #glc_2010_clipped_400 nlcd_2006_clipped_2500
    
    

    rasterTables =  [ "%s_%s" % (raster, chunk) for raster in raster_tables for chunk in chunk_sizes ]
    datasetRuns = []
    for r in rasterTables:
        if "glc_2000_clipped" in r: 
            pixelValue = 16
            reclassValues = {"oldPixel" : 16, "newPixel" : 1 }
        elif "meris_2010_clipped" in r:
            pixelValue = 100
            reclassValues = {"oldPixel" : 100, "newPixel" : 1 }
        elif "nlcd_2006" in r:
            pixelValue = 21
            reclassValues = {"oldPixel" : 21, "newPixel" : 1 }
        datasetRuns.append(  OrderedDict([ ("array_table", r), ("pixelValue", pixelValue), ("newPixel", 1) ]) )

    return datasetRuns

def zonalDatasetPrep():
    """
    Function will return all the possible combinations of dastes for analysis
    rasterTables = (raster dataset * tile size) 
    boundaryNames = All boundaries to test against 
    """
    

    chunk_sizes = [500] #, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
    array_names = ["glc2000_clipped","meris2015_clipped", "nlcd_2006_clipped"]
    raster_paths = ["/home/04489/dhaynes/glc2000_clipped.tif","/home/04489/dhaynes/meris_2010_clipped.tif", "/home/04489/dhaynes/nlcd_2006.tif"]
    shapefiles = ["/home/04489/dhaynes/shapefiles/states.shp"]#["/home/04489/dhaynes/shapefiles/tracts2.shp","/home/04489/dhaynes/shapefiles/states.shp","/home/04489/dhaynes/shapefiles/regions.shp","/home/04489/dhaynes/shapefiles/counties.shp"]#,"/home/04489/dhaynes/shapefiles/tracts.shp"]

    arrayTables =  [ "%s_%s" % (array, chunk) for array in array_names for chunk in chunk_sizes ]
    rasterPaths =  [ raster_path for raster_path in raster_paths for chunk in chunk_sizes ]

    datasetRuns = [ OrderedDict([("shape_path", "%s/5070/%s" % ("/".join(s.split("/")[:5]), s.split("/")[-1]) ),("array_table", a), ("raster_path", r)] ) if "nlcd" in r else OrderedDict([("shape_path", s),("array_table", a), ("raster_path", r)] ) for a,r in zip(arrayTables,rasterPaths) for s in shapefiles ]

    return datasetRuns


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

def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Analysis Script for running SciDB Analytics")  
    parser.add_argument("-csv", required =False, help="Output timing results into CSV file", dest="csv", default="None")  
   
    subparser = parser.add_subparsers(dest="command")
    analytic_subparser = subparser.add_parser('zonal')
    analytic_subparser.set_defaults(func=zonalDatasetPrep)
    
    count_subparser = subparser.add_parser('count')
    count_subparser.set_defaults(func=localDatasetPrep)
    
    reclass_subparser = subparser.add_parser('reclassify')
    reclass_subparser.set_defaults(func=localDatasetPrep)

    focal_subparser = subparser.add_parser('focal')
    focal_subparser.set_defaults(func=localDatasetPrep)

    return parser

if __name__ == '__main__':
    args = argument_parser().parse_args()
    
    sdb = iquery()
    query = sdb.queryAFL("list('instances')")
    SciDBInstances = len(query.splitlines())-1

    runs = [1]#,2,3]
    analytic = 1
    filePath = '/mnt/focal_8_25_2018.csv'
    rasterStatsCSV = '/mnt/zonalstats.csv'

    datasets = args.func()
    timings = OrderedDict()
    
    for d in datasets:
        print(d)
        for r in runs:
            if args.command == "zonal":                  
                print(d["raster_path"], d["shape_path"], d["array_table"])
                rasterStatsCSV = '/mnt/zonalstats_%s_%s.csv' % (d["shape_path"].split("/")[-1].split(".")[0], d["array_table"])
                timed = ZonalStatistics(sdb, d, r, rasterStatsCSV)
                timings[(r,d["array_table"])] = timed
            elif args.command =="count":
                timed = CountPixels(sdb, d["array_table"], d["pixelValue"])
                timings[(r,d["array_table"])] = timed
            elif args.command == "reclassify":
                timed = Reclassify(sdb, d["array_table"], d["pixelValue"], d["newPixel"], 6)
                timings[(r,d["array_table"])] = timed
            elif args.command == "focal":
                timed = FocalAnalysis(sdb, d["array_table"], )
                timings[(r,d["array_table"])] = timed


        #Remove the parallel zone files after each dataset run
        if args.command == "zonal":    
            for i in range(SciDBInstances):
                os.remove("/storage/%s/p_zones.scidb" % (i))
        

    if filePath: WriteFile(filePath, timings)
    print("Finished")
