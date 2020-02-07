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
    parser.add_argument('-p', required=False, help="This the path for the scidb_storage",default='/home/scidb/scidb_data/0', dest='path')
    parser.add_argument('-b', required=False, default=1, help="This is the array band for analysis", dest='band')    
    parser.add_argument('-csv', required=False, dest='csv')
    parser.add_argument('-o', required=False, dest='outpath')

    return parser

if __name__ == '__main__':
    args = argument_parser().parse_args() 
    if CheckFiles(args.shapefile, args.raster):
        from scidb import iquery, Statements
        sdb = iquery()
        
        #My Object that creates array Metadata
        from SciDBParallel import ZonalStats, ArrayToBinary, ParallelRasterization, Rasterization, ParamSeperator

        raster = ZonalStats(args.raster, args.shapefile, args.array_name)
        raster.RasterMetadata(args.raster, args.shapefile, raster.SciDBInstances, args.path ) 
        a = raster.RasterizePolygon(args.raster, args.shapefile)
        print(a.shape)
        datapackage = ParallelRasterization(raster.arrayMetaData)
        sdb_statements = Statements(sdb)

        theAttribute = 'id:%s' % ('int32')
        
        sdb_statements.CreateLoadArray('boundary', theAttribute , 2)
        sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, 'p_zones.scidb')
        #Load operator -1 in parallel
        numDimensions = raster.CreateMask('int32', 'mask')

        reclassText = ReadReclassTxt(args.shapefile)
        csvOut = '%s.csv' % (args.shapefile.split('.')[0])
        tiffOut = '%s.tiff' % (args.shapefile.split('.')[0])

        print("**************Reclassifiying raster*************")
        raster.JoinReclass(raster.SciDBArrayName,'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, reclassText, args.band, csvOut)        
        dataArray = sdb.OutputToArray(csvOut, valueColumn=2, yColumn= 3)
        raster.WriteRaster(dataArray, tiffOut, noDataValue=-999)


        """ iquery -aq "between(slice(population_stack, band, 1), 8551, 8935, 10515, 10572)" > /media/sf_scidb/population/census_tracts/popvalues.csv """
        maskCsvOut = '%s_mask.csv' % (args.shapefile.split('.')[0])
        maskTiffOut = '%s_mask.tiff' % (args.shapefile.split('.')[0])
        print("**************outputing mask*************")
        maskQuery = "scan(boundary)"
        raster.sdb.queryCSV(maskQuery, maskCsvOut)
        dataArray = sdb.OutputToArray(maskCsvOut, valueColumn=3, yColumn=1)
        raster.WriteRaster(dataArray, maskTiffOut, noDataValue=-999)

        dataCsvOut = '%s_data.csv' % (args.shapefile.split('.')[0])
        dataTiffOut = '%s_data.tiff' % (args.shapefile.split('.')[0])
        print("**************outputing data*************")
        
        Output2D_Image = "save(sort(apply(between(%s, 8551, 8935, 10515, 10572), y, y, x, x), y, x) ,'%s', 0, 'csv');" % (raster.SciDBArrayName, dataCsvOut)
        raster.sdb.query(Output2D_Image)
        dataArray = sdb.OutputToArray(dataCsvOut, valueColumn=0, yColumn=1)
        raster.WriteRaster(dataArray, dataTiffOut, noDataValue=-999)

    else:
        print(args.shapefile, args.raster)
