from scidb import iquery, Statements
from SciDBParallel import ZonalStats, ArrayToBinary, ParallelRasterization, Rasterization, ParamSeperator
import os


def ReadReclassTxt(inShapefile):
    """

    """
    reclassFile = '%s.txt' % (inShapefile.split('.')[0])
    reclassText = open(reclassFile).read().replace('\n','')

    return reclassText

sdb = iquery()

arrayName = 'population_2010'
rasterPath = '/media/sf_scidb/population/population_2010myc.vrt'
mypath = '/media/sf_scidb/population/census_tracts'
myshps = ['%s/%s' % (root, f) for root, dirs, files in os.walk(mypath) for f in files if 'shp' in f]

outtext = '%s/mn_census_tracts.txt' % (mypath)

with open(outtext, 'w'):
    for myshp in myshps:
        raster = ZonalStats(rasterPath, myshp, arrayName)

        raster.RasterMetadata(rasterPath, myshp, raster.SciDBInstances, '/home/scidb/scidb_data/0' ) 
        a = raster.RasterizePolygon(rasterPath, myshp)
        print(a.shape)
        datapackage = ParallelRasterization(raster.arrayMetaData)
        sdb_statements = Statements(sdb)

        theAttribute = 'id:%s' % ('int32') #datapackage[0]

        sdb_statements.CreateLoadArray('boundary', theAttribute , 2)
        sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, 'p_zones.scidb')
        #Load operator -1 in parallel
        numDimensions = raster.CreateMask('int32', 'mask') #datapackage[0]
        #raster.GlobalJoin_SummaryStats(raster.SciDBArrayName, 'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, args.band, args.csv)

        reclassText = ReadReclassTxt(myshp)
        csvOut = '%s.csv' % (myshp.split('.')[0])
        tiffOut = '%s.tiff' % (myshp.split('.')[0])

        print("**************Reclassifiying raster*************")
        raster.JoinReclass(raster.SciDBArrayName,'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, reclassText, 1, csvOut)        
        dataArray = sdb.OutputToArray(csvOut, valueColumn=2, yColumn= 3)
        raster.WriteRaster(dataArray, tiffOut, noDataValue=-999)
        outtext.write("%s\n" %(tiffOut))

        maskCsvOut = '%s_mask.csv' % (myshp.split('.')[0])
        maskTiffOut = '%s_mask.tiff' % (myshp.split('.')[0])
        print("************** outputing mask*************")
        #maskQuery = "between(mask, 8551, 8935, 10515, 10572)"
        #maskQuery = "apply(boundary, x, x1+8935, y, y1+8551, value, id)"
        maskQuery = "scan(boundary)"
        raster.sdb.queryCSV(maskQuery, maskCsvOut)
        dataArray = sdb.OutputToArray(maskCsvOut, valueColumn=3, yColumn=1)
        raster.WriteRaster(dataArray, maskTiffOut, noDataValue=-999)




