from scidb import iquery, Statements
from SciDBParallel import ZonalStats, ArrayToBinary, ParallelRasterization, ParamSeperator
import os

def CleanUp(args):
    """
    Clean up temporary files
    """

    for f in args:
        os.remove(f)
def StartUp(thePath, theRacesDict):
    """
    
    """
    for race in theRacesDict.keys():
        theRacePath = "%s/%s" % (thePath, race)
        try:
            os.mkdir(theRacePath)
        except:
            allFiles = ['%s/%s' % (root, f) for root, dirs, files in os.walk(theRacePath) for f in files]
            CleanUp(allFiles)
            os.removedirs(theRacePath)
            os.mkdir(theRacePath)

def ReadReclassTxt(reclassFile):
    """

    """
    #reclassFile = '%s.txt' % (inShapefile.split('.')[0])
    reclassText = open(reclassFile).read().replace('\n','')

    return reclassText


races = {
        'asian': {'arrayName': 'asian_2010', 'reclassFileName': 'sage.asian_2010'},
        'black': {'arrayName': 'black_2010', 'reclassFileName': 'sage.black_2010'},
        'hispanic': {'arrayName': 'hispanic_2010', 'reclassFileName': 'sage.hispanic_2010'},
        'pi': {'arrayName': 'pi_2010', 'reclassFileName': 'sage.pi_2010'},
        'sor': {'arrayName': 'sor_2010', 'reclassFileName': 'sage.sor_2010'},
        'ai': {'arrayName': 'ai_2010', 'reclassFileName': 'sage.ai_2010'},
        'white': {'arrayName': 'white_2010', 'reclassFileName': 'sage.white_2010'}
        }
        
sdb = iquery()

arrayName = 'population_2010'
rasterPath = '/media/sf_scidb/population/population_2010myc.vrt'
mypath = '/media/sf_scidb/population/census_tracts'
myshps = ['%s/%s' % (root, f) for root, dirs, files in os.walk(mypath) for f in files if 'shp' in f]


StartUp(mypath, races)

for myshp in myshps:
    raster = ZonalStats(rasterPath, myshp, arrayName)

    raster.RasterMetadata(rasterPath, myshp, raster.SciDBInstances, '/home/scidb/scidb_data/0' ) 
    #a = raster.RasterizePolygon(rasterPath, myshp)
    #print(a.shape)
    datapackage = ParallelRasterization(raster.arrayMetaData)
    sdb_statements = Statements(sdb)

    theAttribute = 'id:%s' % ('int32') #datapackage[0]

    sdb_statements.CreateLoadArray('boundary', theAttribute , 2)
    sdb_statements.LoadOneDimensionalArray(-1, 'boundary', theAttribute, 1, 'p_zones.scidb')
    #Load operator -1 in parallel
    numDimensions = raster.CreateMask('int32', 'mask') #datapackage[0]
    #raster.GlobalJoin_SummaryStats(raster.SciDBArrayName, 'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, args.band, args.csv)

    tractID = myshp.split('/')[-1].split(".")[0]
    #print(tractID)
    reclassFiles = ['%s/%s' % (root, f) for root, dirs, files in os.walk(mypath) for f in files if tractID in f and 'txt' in f]
    #print(reclassFiles)
    filePath = "/".join(myshp.split('/')[:-1])
    #print(filePath)

    for race in races.keys():
        reclassFilePath = r'%s/%s_%s.txt' % (filePath, races[race]['reclassFileName'], tractID)
        #print(reclassFilePath)
        reclassText = ReadReclassTxt(reclassFilePath)
        csvOut = '%s.csv' % (reclassFilePath[:-4])
        tiffOut = r'%s/%s/%s.tif' % (filePath, race, tractID)
        

        print("**************Reclassifiying raster*************")
        raster.JoinReclass(races[race]['arrayName'],'boundary', 'mask', raster.tlY, raster.tlX, raster.lrY, raster.lrX, numDimensions, reclassText, 1, csvOut)        
        dataArray = sdb.OutputToArray(csvOut, valueColumn=2, yColumn= 3)
        raster.WriteRaster(dataArray, tiffOut, noDataValue=-999)
        
        #Code for outputting the mask
        maskCsvOut = '%s_mask.csv' % (myshp.split('.')[0])
        maskTiffOut = '%s_mask.tif' % (myshp.split('.')[0])
        print("************** outputing mask*************")
        #maskQuery = "between(mask, 8551, 8935, 10515, 10572)"
        #maskQuery = "apply(boundary, x, x1+8935, y, y1+8551, value, id)"
        maskQuery = "sort(boundary, y1, x1)"
        raster.sdb.queryCSV(maskQuery, maskCsvOut)
        dataArray = sdb.OutputToArray(maskCsvOut, valueColumn=3, yColumn=1)
        raster.WriteRaster(dataArray, maskTiffOut, noDataValue=-999)

    break



for race in races.keys():
    theRaceDirectory = "%s/%s" % (mypath, race)
    listTiffFiles = ['%s/%s' % (root, f) for root, dirs, files in os.walk(theRaceDirectory) for f in files if race in f and '.tif' in f]
    outFiles = "%s/%s/%s_inputfiles.txt" % (mypath, race, race)
    with open(outFiles, 'w') as fout:
        fout.writelines(listTiffFiles)





