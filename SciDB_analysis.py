from scidb import iquery, Statements
from SciDBParallel import ZonalStats, ArrayToBinary, ParallelRasterization, Rasterization, ParamSeperator
import os, timeit
from collection import OrderedDict


def datasetsprep():
    """
    Function will return all the possible combinations of dastes for analysis
    rasterTables = (raster dataset * tile size) 
    boundaryNames = All boundaries to test against 
    """
    

    chunk_sizes = [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
    array_names = ["glc_clipped","meris_2010_clipped", "nlcd_2006_clipped"]
    raster_paths = ["glc2000_clipped.tif","meris_2010_clipped.tif", "nlcd_2006_clipped.tif"]
    shapefiles = ["regions.shp","states.shp","counties.shp","tracts.shp"]

    arrayTables =  [ "%s_%s" % (array, chunk) for array in array_names for chunk in chunk_sizes ]
    rasterPaths =  [ raster_path for raster_path in raster_path for chunk in chunk_sizes ]

    datasetRuns = [ OrderedDict([("shapepath", s),("array_table", a), ("raster_path", r)] ) if "nlcd" in r else OrderedDict([("shape_path", "%s_proj.shp" % s[:-4]),("array_table", a), ("raster_path", r)] ) for a,r in zip(arrayTables,rasterPaths) for s in shapefiles ]

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
    analytic = 0
    filePath = ''
    timings = OrderedDict()

    datasets = datasetprep()
    for d in datasets:
        for r in runs:
            start = timeit.default_timer()
            
            raster = ZonalStats(dataset[d]["raster_path"], datasets[d]["shape_path"], datasets[d]["array_table"])
            raster.RasterMetadata(datasets[d]["raster_path"], datasets[d]["shape_path"] raster.SciDBInstances, '/home/scidb/scidb_data/0' ) 
            stopPrep = timeit.default_timer()

            datapackage = ParallelRasterization(raster.arrayMetaData)
            stopRasterization = timeit.default_timer()
            
            sdb_statements = Statements(sdb)
            theAttribute = 'id:%s' % (datapackage[0]) #

            sdb_statements.CreateLoadArray('boundary', theAttribute , 2)
            sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, 'p_zones.scidb')
            #Load operator -1 in parallel
            numDimensions = raster.CreateMask(datapackage[0], 'mask')
            stopLoad = timeit.default_timer()
            raster.GlobalJoin_SummaryStats(raster.SciDBArrayName, 'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, args.band, args.csv)
            stopSummaryStats = timeit.default_timer()

            analytic += 1

            timings[analytic] = OrderedDict( [("connectionInfo", "XSEDE"), ("run", r), ("SciDB_Executors", raster.SciDBInstances) ), ("array_table", datasets[d]["array_table"]), ("boundary_table", datasets[d]["shape_path"]), \
                                ("full_time", stopSummaryStats-start), ("join_time", stopSummaryStats-stopLoad), ("load_time", stopLoad - stopRasterization), ("rasterize_time", stopRasterization-stopPrep), ])

    if filePath: WriteFile(filePath, timings)
    print("Finished")