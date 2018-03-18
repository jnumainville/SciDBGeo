from scidb import iquery, Statements
from SciDBParallel import *
import os, timeit, csv
from collections import OrderedDict


def datasetsprep():
    """
    Function will return all the possible combinations of dastes for analysis
    rasterTables = (raster dataset * tile size) 
    boundaryNames = All boundaries to test against 
    """
    

    chunk_sizes = [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
    array_names = ["glc2000_clipped","meris2015_clipped", "nlcd_2006_clipped"]
    raster_paths = ["/home/04489/dhaynes/glc2000_clipped.tif","/home/04489/dhaynes/meris_2010_clipped.tif", "/home/04489/dhaynes/nlcd_2006.tif"]
    shapefiles = ["/home/04489/dhaynes/shapefiles/tracts2.shp","/home/04489/dhaynes/shapefiles/states.shp","/home/04489/dhaynes/shapefiles/states.shp","/home/04489/dhaynes/shapefiles/counties.shp"]#,"/home/04489/dhaynes/shapefiles/tracts.shp"]

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
    runs = [1,2,3]
    analytic = 1
    filePath = '/mnt/zonal_stats_3_18_2018_all.csv'
    timings = OrderedDict()
    rasterStatsCSV = ''

    datasets = datasetsprep()
    for d in datasets:
        print(d["raster_path"], d["shape_path"], d["array_table"])
        for r in runs:
            start = timeit.default_timer()
            
            raster = ZonalStats(d["shape_path"], d["raster_path"], d["array_table"])
            raster.RasterMetadata(d["raster_path"], d["shape_path"], raster.SciDBInstances, '/storage' ) 
            stopPrep = timeit.default_timer()
            print(raster.geoTiffPath, raster.SciDBArrayName)
            
            if r == 1:
                datapackage = ParallelRasterization(raster.arrayMetaData, raster)
                stopRasterization = timeit.default_timer()
                SciDBInstances = raster.SciDBInstances
            
            sdb_statements = Statements(sdb)
            
            theAttribute = 'id:%s' % (datapackage[0]) #
            sdb_statements.CreateLoadArray('boundary', theAttribute , 2)
            sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, 'p_zones.scidb')
            #SciDB Load operator -1 loads in parallel
            numDimensions = raster.CreateMask(datapackage[0], 'mask')
            redimension_time = raster.InsertRedimension( 'boundary', 'mask', raster.tlY, raster.tlX )

            summaryStatTime = raster.GlobalJoin_SummaryStats(raster.SciDBArrayName, 'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, 1, rasterStatsCSV)
            stopSummaryStats = timeit.default_timer()            
            
            timings[analytic] = OrderedDict( [("connectionInfo", "XSEDE"), ("run", r), ("SciDB_Executors", raster.SciDBInstances), ("array_table", d["array_table"]), ("boundary_table", d["shape_path"]), ("full_time", stopSummaryStats-start), ("join_time", summaryStatTime), ("redimension_time", redimension_time), ("rasterize_time", stopRasterization-stopPrep) ])
            sdb.query("remove(mask)")
            sdb.query("remove(boundary)")
            del raster
            analytic += 1

        for i in range(SciDBInstances):
            os.remove("/storage/%s/p_zones.scidb" % (i))

    if filePath: WriteFile(filePath, timings)
    print("Finished")
