#Python script
def ReadReclassTxt(inShapefile):
    """

    """
    reclassFile = '%s.txt' % (inShapefile.split('.')[0])
    reclassText = open(reclassFile).read().replace('\n','')

    return reclassText

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
    parser.add_argument("-p", required=False, help="This the path for the scidb_storage",default='/home/scidb/scidb_data/0', dest='path')
    parser.add_argument('-b', required=False, default=1, help="This is the array band for analysis", dest='band')    
    parser.add_argument('-csv', required=False, dest='csv')
    parser.add_argument('-o', required=False, dest='outpath')


    # group = parser.add_mutually_exclusive_group()
    # group.add_argument("-v", "--verbose", action="store_true")
    # group.add_argument("-q", "--quiet", action="store_true")   
    return parser

if __name__ == '__main__':
    args = argument_parser().parse_args() 
    if CheckFiles(args.shapefile, args.raster):
        from scidb import iquery, statements
        sdb = iquery()
        
        #My Object that creates array Metadata
        from SciDBParallel import ZonalStats, ArrayToBinary, ParallelRasterization, Rasterization, ParamSeperator

        raster = ZonalStats(args.raster, args.shapefile, args.array_name)
        raster.RasterMetadata(args.raster, args.shapefile, raster.SciDBInstances, args.path ) 
        a = raster.RasterizePolygon(args.raster, args.shapefile)
        print(a.shape)
        datapackage = ParallelRasterization(raster.arrayMetaData)
        sdb_statements = Statements(sdb)

        theAttribute = 'id:%s' % (datapackage[0])
        sdb_statements.CreateLoadArray('boundary', theAttribute , 2)
        sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, 'p_zones.scidb')
        numDimensions = raster.CreateMask(datapackage[0], 'mask')
        #raster.GlobalJoin_SummaryStats(raster.SciDBArrayName, 'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, args.band, args.csv)

        reclassText = ReadReclassTxt(args.shapefile)
        csvOut = '%s.csv' % (args.shapefile.split('.')[0])
        tiffOut = '%s3.tiff' % (args.shapefile.split('.')[0])

        raster.JoinReclass(raster.SciDBArrayName,'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, reclassText, args.band, csvOut)
        
        dataArray = raster.OutputToArray(csvOut, 3)
        raster.WriteRaster(dataArray, tiffOut, noDataValue=-999)
        # ZonalStats(args.Runs, args.Shapefile, args.Raster, args.SciArray, sdb, args.mode, args.CSV, args.verbose)
    else:
        print(args.shapefile, args.raster)
