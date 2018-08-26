from SciDBParallel import RasterLoader, ParallelLoad, ParallelLoadByChunk
from scidb import iquery, Statements


if __name__ == '__main__':
        datasets = {
                    #"glc": {"geoTiffPath": "/home/david/Downloads/glc2000_clipped.tif", "arrayName": "glc_2000_clipped_chunk", "attribute": "value", "outDirectory": "/home/david/scidb_data/0", }
                    #"glc": {"geoTiffPath": "/home/david/Downloads/glc2000.tif", "arrayName": "glc_2000_chunk", "attribute": "value", "outDirectory": "/home/david/scidb_data/0", }
                    # "glc": {"geoTiffPath": "/home/04489/dhaynes/glc2000_clipped.tif", "arrayName": "glc_2000_clipped", "attribute": "value", "outDirectory": "/storage"}, 
                    #"meris": {"geoTiffPath": "/home/04489/dhaynes/meris_2010_clipped.tif", "arrayName": "meris_2010_clipped", "attribute": "value", "outDirectory": "/storage"}
                    #"nlcd": {"geoTiffPath": "/home/04489/dhaynes/nlcd_2006.tif", "arrayName": "nlcd_junk",  "attribute": "value", "outDirectory": "/storage"}
                    "meris3Meter" :{"geoTiffPath": "/group/meris_3m/meris_3m.vrt", "arrayName": "meris_2010_3m_chunk",  "attribute": "value", "outDirectory": "/storage" }
                    }
        tileSizes = [1000] #, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
        sdb = iquery()
        for d in datasets:
                #print(datasets[d])
                # raster = RasterLoader(datasets[d]["geoTiffPath"], datasets[d]["arrayName"], [datasets[d]["attribute"]], 0, datasets[d]["outDirectory"])
                raster = RasterLoader(datasets[d]["geoTiffPath"], datasets[d]["arrayName"], [datasets[d]["attribute"]], 1000, datasets[d]["outDirectory"], 1, 20000000)
                # RasterPath, scidbArray, attribute, chunksize, dataStorePath, tiles=None, maxPixels=10000000, yOffSet=None
                # print("RasterMetadata")
                # for r in raster.RasterMetadata:
                #     print(raster.RasterMetadata[r])
                # print("New Chunk Metadata")
                # for r in raster.RasterReadingData:
                #     print(raster.RasterReadingData[r])
                #rasterArrayName, height, width, chunk
                raster.CreateDestinationArray(datasets[d]["arrayName"], raster.height, raster.width, tileSizes[0])
                # ParallelLoad(raster.RasterMetadata)
                ParallelLoadByChunk(raster.RasterReadingData)

                #loadAttribute = "%s_1:%s" % (raster.AttributeString.split(":")[0], raster.AttributeString.split(":")[1])
                #print(loadAttribute)
                #raster.CreateLoadArray("LoadArray", loadAttribute, raster.RasterArrayShape)

                #sdb_statements = Statements(sdb)
                #sdb_statements.LoadOneDimensionalArray(-1, "LoadArray", loadAttribute, 1, 'pdataset.scidb')

                # for t in tileSizes:
                #         arrayName = "%s_%s" % (datasets[d]["arrayName"], t)
                #         raster.CreateDestinationArray(arrayName, raster.height, raster.width, t)        
                #         sdb_statements.InsertRedimension( "LoadArray", arrayName, oldvalue=loadAttribute.split(":")[0], newvalue='value')
