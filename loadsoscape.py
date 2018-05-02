# import multiprocessing as mp
# import itertools, timeit
# import numpy as np
# import math, csv, os, gc, sys
# from osgeo import gdal
# from gdalconst import GA_ReadOnly
# from collections import OrderedDict
# import GDALtoSciDB_multiprocessing as geoSciDB


from SciDBParallel import RasterLoader, ParallelLoad
from scidb import iquery, Statements
from collections import OrderedDict

    
    # r = gdal.Open(geoTiffPath)
    #RasterInformation = geoSciDB.RasterReader(geoTiffPath, raceDictionary[race]["array_name"], ["value"], 1000, 8,  )
    # print(r.RasterXSize)
    #timeDictionary = geoSciDB.MultiProcessLoading(RasterInformation, geoTiffPath, SciDBpath, SciDBpath)




if __name__ == '__main__':

    SciDBpath = '/home/scidb/scidb_data/0'
    tilesize = 1000
    baseDir = '/media/sf_scidb/soscape'
    raceDictionary = OrderedDict([
                        ("AsianAmerican" , {"array_name": "asian_2010", "geoTiffName" : "nhas_2010myc.vrt", "directory": "nhas"}),
                        ("BlackAmerican" , {"array_name": "black_2010", "geoTiffName" : "nhb_2010myc.vrt", "directory": "nhb"}) ,
                        ("HispanicAmerican" , {"array_name": "hispanic_2010", "geoTiffName" : "hispanic_2010myc.vrt", "directory": "hispanic"}), #
                        ("AmericanIndian" , {"array_name": "ai_2010", "geoTiffName" : "nham_2010myc.vrt", "directory": "nham"}), #
                        ("SOR" , {"array_name": "sor_2010", "geoTiffName" : "nhother_2010myc.vrt", "directory": "nhother"}), 
                        ("PacificIslander" , {"array_name": "pi_2010", "geoTiffName" : "nhpi_2010myc.vrt", "directory": "nhpi"}),
                        ("WhiteAmerican" , {"array_name": "white_2010", "geoTiffName" : "nhw_2010myc.vrt", "directory": "nhw"})
                    ])
    
    # raceDictionary = {  "AsianAmerican"  {"array_name": "asian_2010", "geoTiffName" : "nhas_2010myc.vrt", "directory": "nhas"}, #
    #                     "BlackAmerican" : {"array_name": "black_2010", "geoTiffName" : "nhb_2010myc.vrt", "directory": "nhb"},
    #                     "HispanicAmerican" : {"array_name": "hispanic_2010", "geoTiffName" : "hispanic_2010myc.vrt", "directory": "hispanic"}, #
    #                     "AmericanIndian" : {"array_name": "ai_2010", "geoTiffName" : "nham_2010myc.vrt", "directory": "nham"}, #
    #                     "SOR" : {"array_name": "sor_2010", "geoTiffName" : "nhother_2010myc.vrt", "directory": "nhother"}, 
    #                     "PacificIslander" : {"array_name": "pi_2010", "geoTiffName" : "nhpi_2010myc.vrt", "directory": "nhpi"},
    #                     "WhiteAmerican" : {"array_name": "white_2010", "geoTiffName" : "nhw_2010myc.vrt", "directory": "nhw"}
    #                }


    for race in raceDictionary:
        print("Loading Data for %s" % (raceDictionary[race]["array_name"]) )
        geoTiffPath = '%s/%s/%s' % (baseDir, raceDictionary[race]["directory"], raceDictionary[race]["geoTiffName"])
        # datasets = {
        #               #"glc": {"geoTiffPath": "/home/04489/dhaynes/glc2000_clipped.tif", "arrayName": "glc_2000_clipped", "attribute": "value", "outDirectory": "/storage"}, 
        #              #"meris": {"geoTiffPath": "/home/04489/dhaynes/meris_2010_clipped.tif", "arrayName": "meris_2010_clipped", "attribute": "value", "outDirectory": "/storage"}
        #              #"nlcd": {"geoTiffPath": "/home/04489/dhaynes/nlcd_2006.tif", "arrayName": "nlcd_2006",  "attribute": "value", "outDirectory": "/storage"}
        #             "meris3Meter" :{"geoTiffPath": "/data/projects/G-818404/meris_2010_clipped_3m.tif", "arrayName": "meris_2010_3m",  "attribute": "value", "outDirectory": "/storage" }
        #             }

        dataset = {"arrayName": raceDictionary[race]["array_name"] , "geoTiffPath": geoTiffPath, "attribute":"value", "outDirectory": SciDBpath}
        
        sdb = iquery()
        
        
        raster = RasterLoader(dataset["geoTiffPath"], dataset["arrayName"], [dataset["attribute"]], 0, dataset["outDirectory"])
        ParallelLoad(raster.RasterMetadata)
        loadAttribute = "%s_1:%s" % (raster.AttributeString.split(":")[0], raster.AttributeString.split(":")[1])
        print(loadAttribute)
        raster.CreateLoadArray("LoadArray", loadAttribute, raster.RasterArrayShape)

        sdb_statements = Statements(sdb)
        sdb_statements.LoadOneDimensionalArray(-1, "LoadArray", loadAttribute, 1, 'pdataset.scidb')


        
        raster.CreateDestinationArray(dataset["arrayName"], raster.height, raster.width, tilesize)
        sdb_statements.InsertRedimension( "LoadArray", dataset["arrayName"], oldvalue=loadAttribute.split(":")[0], newvalue='value')   