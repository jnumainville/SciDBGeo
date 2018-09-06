from SciDBParallel import RasterLoader, ParallelLoad, ParallelLoadByChunk
from scidb import iquery, Statements


if __name__ == '__main__':
        datasets = {
                    #"glc": {"geoTiffPath": "/home/david/Downloads/glc2000_clipped.tif", "arrayName": "glc_2000_clipped_chunk", "attribute": "value", "outDirectory": "/home/david/scidb_data/0", }
                    "glc": {"geoTiffPath": "/home/04489/dhaynes/glc2000_clipped.tif", "arrayName": "glc_2000_clipped_overlap", "attribute": "value", "outDirectory": "/storage", "memory": 1500000 }
                    #"meris": {"geoTiffPath": "/home/04489/dhaynes/meris_2010_clipped.tif", "arrayName": "meris_2010_clipped", "attribute": "value", "outDirectory": "/storage"}
                    #"nlcd": {"geoTiffPath": "/home/04489/dhaynes/nlcd_2006.tif", "arrayName": "nlcd_junk",  "attribute": "value", "outDirectory": "/storage"}
                    #"meris3Meter" :{"geoTiffPath": "/group/meris_3m/meris_3m.vrt", "arrayName": "meris_2010_3m_chunk",  "attribute": "value", "outDirectory": "/storage" } #20000000
                    }
        tileSizes = [500, 1000, 1500] #, 2000, 2500, 3000, 3500, 4000]
        sdb = iquery()
        for d in datasets:
                #print(datasets[d])
                for c, tile in enumerate(tileSizes):
                    print(c)
                    arrayName = "%s_%s" % (datasets[d]["arrayName"], tile)
                # raster = RasterLoader(datasets[d]["geoTiffPath"], datasets[d]["arrayName"], [datasets[d]["attribute"]], 0, datasets[d]["outDirectory"])
                                          
                    raster = RasterLoader(datasets[d]["geoTiffPath"], arrayName, [datasets[d]["attribute"]], tile, datasets[d]["outDirectory"], 1,datasets[d]["memory"] )
                    raster.CreateDestinationArray(arrayName, raster.height, raster.width, tile, 1)
                    
                    if c == 0:
                        ParallelLoadByChunk(raster.RasterReadingData)
                        
                    else:
                        #sdb.query( "create array %s <value:uint8> [y=0:%s,%s,1; x=0:%s,%s,1]" % (arrayName, raster.height, tile, raster.width, tile) )
                        sdb.query("insert(redimension(apply(LoadArray, x, x1+0, y, y1+0, value, value_1), %s ), %s);" % (arrayName, arrayName))



