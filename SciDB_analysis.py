from scidb import iquery, Statements
from SciDBParallel import ZonalStats, ArrayToBinary, ParallelRasterization, Rasterization, ParamSeperator
import os, timeit, csv
from collections import OrderedDict


def datasetsprep():
    """
    Function will return all the possible combinations of dastes for analysis
    rasterTables = (raster dataset * tile size) 
    boundaryNames = All boundaries to test against 
    """
    

    chunk_sizes = [500, 1000] #, 1500, 2000, 2500, 3000, 3500, 4000]
    array_names = ["glc2000_clipped"] #,"meris_2010_clipped", "nlcd_2006_clipped"]
    raster_paths = ["/home/04489/dhaynes/glc2000_clipped.tif"] #,"/home/04489/dhaynes/meris_2010_clipped.tif", "/home/04489/dhaynes/nlcd_2006_clipped.tif"]
    shapefiles = ["/home/04489/dhaynes/shapefiles/states.shp"]#,"/home/04489/dhaynes/shapefiles/states.shp","/home/04489/dhaynes/shapefiles/counties.shp","/home/04489/dhaynes/shapefiles/tracts.shp"]

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


if __name__ == '__main__':

    sdb = iquery()
    runs = [1] #,2,3]
    analytic = 0
    filePath = '/mnt/zonal_stats_3_4_2018.csv'
    timings = OrderedDict()
    rasterStatsCSV = ''

    datasets = datasetsprep()
    for d in datasets:
        print(d["raster_path"], d["shape_path"], d["array_table"])
        for r in runs:
            start = timeit.default_timer()
            
            raster = ZonalStats(d["raster_path"], d["shape_path"], d["array_table"])
            print(raster.SciDBInstances)
            raster.RasterMetadata(d["raster_path"], d["shape_path"], raster.SciDBInstances, '/storage' ) 
            stopPrep = timeit.default_timer()

            datapackage = ParallelRasterization(raster.arrayMetaData)
            stopRasterization = timeit.default_timer()
            
            sdb_statements = Statements(sdb)
            
            theAttribute = 'id:%s' % (datapackage[0]) #
            sdb_statements.CreateLoadArray('boundary', theAttribute , 2)
            sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, 'p_zones.scidb')
            #Load operator -1 in parallel
            numDimensions = raster.CreateMask(datapackage[0], 'mask')
            redimension_time, summaryStatTime = raster.GlobalJoin_SummaryStats(raster.SciDBArrayName, 'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, 1, rasterStatsCSV)
            stopSummaryStats = timeit.default_timer()            

            analytic += 1

            timings[analytic] = OrderedDict( [("connectionInfo", "XSEDE"), ("run", r), ("SciDB_Executors", raster.SciDBInstances), ("array_table", d["array_table"]), ("boundary_table", d["shape_path"]), ("full_time", stopSummaryStats-start), ("join_time", summaryStatTime), ("redimension_time", redimension_time), ("rasterize_time", stopRasterization-stopPrep) ])
            sdb.query("remove(mask)")
            sdb.query("remove(boundary)")

    if filePath: WriteFile(filePath, timings)
    print("Finished")
