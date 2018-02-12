#Python script


def CheckFiles(*argv):
    """
    This function checks files to make sure they exist
    """
    import os
    for i in argv:
        if not os.path.exists(i): 
            print("FilePath %s does not exist" % (i) )
            return False
    return True

def argument_parser():
    """

    """
    import argparse
    parser = argparse.ArgumentParser(description="Conduct SciDB Zonal Stats")   
    parser.add_argument('-a', required=True, dest='array_name')
    parser.add_argument('-r', required=True, dest='raster')
    parser.add_argument('-s', required=True, dest='shapefile')
    parser.add_argument('-p', required=True, help="This the path for the scidb_storage", dest='path')

    # parser.add_argument('-Tests', type=int, help="Number of tests you want to run", required=False, default=3, dest='Runs')
    # parser.add_argument('-Mode', help="This allows you to choose the mode of analysis you want to conduct", type=int, default=1, required=True, dest='mode')
    # parser.add_argument('-CSV', required=False, dest='CSV')
    # parser.add_argument('-v', required=False, action="store_true", default=False, dest='verbose')
    # parser.add_argument('-Host', required=False, help="SciDB host for connection", dest="host", default="http://localhost:8080")     

    # group = parser.add_mutually_exclusive_group()
    # group.add_argument("-v", "--verbose", action="store_true")
    # group.add_argument("-q", "--quiet", action="store_true")   
    return parser

if __name__ == '__main__':
    args = argument_parser().parse_args()
    if CheckFiles(args.shapefile, args.raster):
        from scidb import iquery, statements
        sdb = iquery()
        #My Object the creates array Metadata
        from SciDBParallel import ZonalStats, ArrayToBinary, ParallelRasterization, Rasterization, ParamSeperator

        raster = ZonalStats(args.raster, args.shapefile, args.array_name)
        raster.RasterMetadata(args.raster, args.shapefile, raster.SciDBInstances, args.path )
        ParallelRasterization(raster.arrayMetaData)
        sdb_statements = statements(sdb)
        theAttribute = 'id:%s' % (raster.rasterValueDataType)
        sdb_statements.CreateLoadArray('boundary', theAttribute , 2)
        #LoadOneDimensionalArray(self, sdb_instance, tempRastName, rasterAttributes, rasterType, binaryLoadPath)
        sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, args.path)
        raster.CreateMask('mask')
        # ZonalStats(args.Runs, args.Shapefile, args.Raster, args.SciArray, sdb, args.mode, args.CSV, args.verbose)
    else:
        print(args.shapefile, args.raster)
