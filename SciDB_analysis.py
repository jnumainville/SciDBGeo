from scidb import iquery, Statements
from SciDBParallel import *
import os, timeit, csv
from collections import OrderedDict
import configparser
import json


def ZonalStatistics(sdbConn, dataset, theRun, summaryStatsCSV=None):
    """
    This is the functions for zonal statistics
    Each function should be completely self contained

    Input:
        sdbConn = SciDB instance to run statistics on
        dataset = The dataset to run the statistics on
        theRun = The run the program is currently on
        summaryStatsCSV = The CSV path to write to

    Output:
        An ordered dictionary with timing information
    """

    start = timeit.default_timer()
    raster = ZonalStats(dataset["shape_path"], dataset["raster_path"], dataset["array_table"])
    raster.RasterMetadata(dataset["raster_path"], dataset["shape_path"], raster.SciDBInstances, '/home/research/storage')
    stopPrep = timeit.default_timer()
    print(raster.geoTiffPath, raster.SciDBArrayName)

    if theRun == 1:
        datapackage = ParallelRasterization(raster.arrayMetaData, raster)
        stopRasterization = timeit.default_timer()

        sdb_statements = Statements(sdbConn)

        theAttribute = 'id:%s' % (datapackage[0])  #
        startLoadTime = timeit.default_timer()
        sdb_statements.CreateLoadArray('boundary', theAttribute, 2)
        sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, 'p_zones.scidb')
        stopLoadTime = timeit.default_timer()

        loadTime = stopLoadTime - startLoadTime
        # SciDB Load operator -1 loads in parallel
        numDimensions = raster.CreateMask(datapackage[0], 'mask')
        redimension_time = raster.InsertRedimension('boundary', 'mask', raster.tlY, raster.tlX)

    summaryStatTime = raster.GlobalJoin_SummaryStats(raster.SciDBArrayName, 'boundary', 'mask', raster.tlY, raster.tlX,
                                                     raster.lrY, raster.lrX, numDimensions, 1, summaryStatsCSV)
    stopSummaryStats = timeit.default_timer()

    timed = OrderedDict([("connectionInfo", "Connection"), ("run", r),
                         ("array_table", d["array_table"]), ("boundary_table", d["shape_path"]),
                         ("full_time", stopSummaryStats - start),
                         ("join_time", summaryStatTime), ("redimension_time", redimension_time),
                         ("rasterize_time", stopRasterization - stopPrep),
                         ("dataset", "_".join(d["array_table"].split("_")[:-1])),
                         ("chunk", d["array_table"].split("_")[-1]), ("load_time", loadTime)])

    return timed


def FocalAnalysis(sdbConn, arrayTable):
    """
    This function perform a focal analysis on a SciDB Array

    Input:
        sdbConn = Connection to a SciDB instance
        arrayTable = Array to run analysis on

    Output:
        An ordered dictionary with timing information
    """

    start = timeit.default_timer()
    query = "aggregate(window(%s, 1,1,1,1, avg(value)), sum(value_avg))" % (arrayTable)

    results = sdbConn.query(query)
    stop = timeit.default_timer()

    timed = OrderedDict([("run", r), ("analytic", "count"), ("time", stop - start), ("array_table", arrayTable),
                         ("dataset", "_".join(arrayTable.split("_")[:-1])), ("chunk", arrayTable.split("_")[-1])])

    return timed


def TwoRasterAdd(sdbConn, arrayTable):
    """
    This function will add an array to itself

    Input:
        sdbConn = The SCiDB connection to use
       	arrayTable = The table to use

    Output:
        An ordered dictionary containing timing information
    """

    start = timeit.default_timer()
    query = "apply( join( apply(%s, x1, value), apply(%s, y1, value)), result, x1+y1)" % (arrayTable, arrayTable) 
    results = sdbConn.query(query)

    stop = timeit.default_timer()

    timed = OrderedDict([("run", r), ("analytic", "raster_add"), ("time", stop - start), ("array_table", arrayTable),
                         ("dataset", "_".join(arrayTable.split("_")[:-1])), ("chunk", arrayTable.split("_")[-1])])

    return timed


def CountPixels(sdbConn, arrayTable, pixelValue):
    """
    This function will return the sum of the pixels in a SciDB Array

    Input:
        sdbConn = The SCiDB connection to use
        arrayTable = The table to use
        pixelValue = The type of the pixel

    Output:
        An ordered dictionary containing timing information
    """

    start = timeit.default_timer()
    #query = "SELECT count(value) from %s WHERE value = %s" % (arrayTable, pixelValue)
    #results = sdbConn.aql_query(query)

    query = "aggregate(filter(%s, value = %s), sum(value))" % (arrayTable, pixelValue)
    results = sdbConn.query(query)

    stop = timeit.default_timer()
    pixelCount = str(results.splitlines()[-1])
    print("Sum of pixel values %s for array: %s" % (pixelCount.split(" ")[-1], arrayTable))

    timed = OrderedDict([("run", r), ("analytic", "count"), ("time", stop - start), ("array_table", arrayTable),
                         ("dataset", "_".join(arrayTable.split("_")[:-1])), ("chunk", arrayTable.split("_")[-1])])

    return timed


def Reclassify(sdbConn, arrayTable, oldValue, newValue, run=1):
    """
    Reclassify the table

    Input:
        sdbConn = The SCiDB connection to use
        arrayTable = The table to use
        oldValue = The old value that the table uses
        newValue = The new value to use with the table
        run = The run that is in progress

    Output:
        An ordered dictionary containing timing information
    """

    start = timeit.default_timer()
    query = "aggregate(apply(%s, value2, iif(value = %s, %s, 0)), sum(value2))" % (arrayTable, oldValue, newValue)

    results = sdbConn.query(query)
    stop = timeit.default_timer()
    Statement = Statements(sdbConn)
    if run == 1:
        Statement.CreateMask(arrayTable, "reclassedTable", "newvalue", "int64")
        sdbConn.query("""insert(redimension(apply(%s, newvalue, iif(value = %s, %s, -99)), "reclassedTable"), 
        "reclassedTable") """ % (arrayTable, oldValue, newValue))
        stopInsert = timeit.default_timer()
        insertTime = stopInsert - stop
    else:
        insertTime = 0

    timed = OrderedDict([("run", r), ("analytic", "reclassify"), ("time", stop - start), ("array_table", arrayTable),
                         ("redimensionInsertTime", insertTime), ("dataset", "_".join(arrayTable.split("_")[:-1])),
                         ("chunk", arrayTable.split("_")[-1])])

    return timed


def localDatasetPrep(config, tableName=''):
    """
    This function preps the datasets

    Input:
        config = the instance of a configuration file
        tableName = the name of the table that

    Output:
        An ordered dictionary containing the array_table, pixelValue, and newPixel as keys
    """
    def parse(s):
        return json.loads(config.get("localDatasetPrep", s))

    chunk_sizes = parse("chunk_sizes")
    raster_tables = parse("raster_tables")
    pixel_values = parse("pixel_values")

    if tableName:
        return [OrderedDict([("array_table", "%s_%s_%s" % (raster, tableName, chunk)),
                                    ("pixelValue", pixel),
                                    ("newPixel", 1)])
                       for raster, pixel in zip(raster_tables, pixel_values) for chunk in chunk_sizes]
    else:
        return [OrderedDict([("array_table", "%s_%s" % (raster, chunk)),
                                    ("pixelValue", pixel),
                                    ("newPixel", 1)])
                       for raster, pixel in zip(raster_tables, pixel_values) for chunk in chunk_sizes]


def zonalDatasetPrep(config):
    """
    Function will return all the possible combinations of dataset for analysis
    rasterTables = (raster dataset * tile size) 
    boundaryNames = All boundaries to test against

    Input:
        config = the instance of a configuration file

    Output:
        An ordered dictionary containing run information
    """
    def parse(s):
        return json.loads(config.get("zonalDatasetPrep", s))

    raster_folder = parse("raster_folder")
    shape_folder = parse("shape_folder")

    chunk_sizes = parse("chunk_sizes")
    array_names = parse("array_names")
    raster_paths = ["{}/{}".format(raster_folder, file) for file in parse("raster_files")]
    shape_files = ["{}/{}".format(shape_folder, file) for file in parse("shape_files")]

    arrayTables = ["%s_%s" % (array, chunk) for array in array_names for chunk in chunk_sizes]
    rasterPaths = [raster_path for raster_path in raster_paths for chunk in chunk_sizes]

    return [OrderedDict([("shape_path", s), ("array_table", a), ("raster_path", r)]) for a, r in
            zip(arrayTables, rasterPaths) for s in shape_files]


def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv

    Input:
        filePath = Where to write the dictionary
        theDictionary = The dictionary to write as csv

    Output:
        None
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

    Input:
        None

    Output:
        The argument parser
    """
    import argparse

    parser = argparse.ArgumentParser(description="Analysis Script for running SciDB Analytics")
    parser.add_argument("-csv", required=False, help="Output timing results into CSV file", dest="csv", default="None")

    subparser = parser.add_subparsers(dest="command")
    subparser.required = True
    analytic_subparser = subparser.add_parser('zonal')
    analytic_subparser.set_defaults(func=zonalDatasetPrep)

    count_subparser = subparser.add_parser('count')
    count_subparser.set_defaults(func=localDatasetPrep)

    reclass_subparser = subparser.add_parser('reclassify')
    reclass_subparser.set_defaults(func=localDatasetPrep)

    focal_subparser = subparser.add_parser('focal')
    focal_subparser.set_defaults(func=localDatasetPrep)

    overlap_subparser = subparser.add_parser('overlap')
    overlap_subparser.set_defaults(func=localDatasetPrep)

    add_subparser = subparser.add_parser('add')
    add_subparser.set_defaults(func=localDatasetPrep)

    return parser


if __name__ == '__main__':
    """
        Entry point for SciDB_analysis
        This file contains the functions used for performing spatial analyses in SciDB
    """
    config = configparser.ConfigParser()
    config.read("config.ini")

    def parse(s):
        return json.loads(config.get("main", s))

    args = argument_parser().parse_args()
    sdb = iquery()
    query = sdb.queryAFL("list('instances')")
    SciDBInstances = len(query.splitlines()) - 1

    runs = parse("runs")
    # analytic does not appear to be used?
    #analytic = 1
    filePath = parse("filePath")
    rasterStatsCSVBase = parse("rasterStatsCSVBase")
    if args.command == "overlap":
        datasets = args.func(config, 'overlap')
    else:
        datasets = args.func(config)
    timings = OrderedDict()

    for d in datasets:
        print(d)
        for r in runs:
            timed = None
            if args.command == "zonal":
                print(d["raster_path"], d["shape_path"], d["array_table"])
                rasterStatsCSV = '%s_%s_%s.csv' % (rasterStatsCSVBase, d["shape_path"].split("/")[-1].split(".")[0],
                                                   d["array_table"])
                timed = ZonalStatistics(sdb, d, r, rasterStatsCSV)
                timings[(r, d["array_table"], d["shape_path"])] = timed
            elif args.command == "count":
                timed = CountPixels(sdb, d["array_table"], d["pixelValue"])
                timings[(r, d["array_table"])] = timed
            elif args.command == "reclassify":
                timed = Reclassify(sdb, d["array_table"], d["pixelValue"], d["newPixel"], 6)
                timings[(r, d["array_table"])] = timed
            elif args.command == "focal" or args.command == "overlap":
                timed = FocalAnalysis(sdb, d["array_table"])
                timings[(r, d["array_table"])] = timed
                timeit.time.sleep(120)
            elif args.command == "add":
                timed = TwoRasterAdd(sdb, d["array_table"])
                timings[(r, d["array_table"])] = timed
            print(timed)

        if args.command == "overlap" or args.command == "focal":
            print("Pausing for 10 minutes between datasets")
            timeit.time.sleep(600)

        # Remove the parallel zone files after each dataset run
        if args.command == "zonal":
            for i in range(SciDBInstances):
                #TODO: Move to config?
                os.remove("/storage/%s/p_zones.scidb" % (i))

    if filePath:
        WriteFile(filePath, timings)
    print("Finished")
