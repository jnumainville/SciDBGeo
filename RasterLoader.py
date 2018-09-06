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
        tileSizes = [1000, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
        sdb = iquery()
        for d in datasets:
                #print(datasets[d])
                for tile in tileSizes:
                # raster = RasterLoader(datasets[d]["geoTiffPath"], datasets[d]["arrayName"], [datasets[d]["attribute"]], 0, datasets[d]["outDirectory"])
                    raster = RasterLoader(datasets[d]["geoTiffPath"], datasets[d]["arrayName"], [datasets[d]["attribute"]], tile, datasets[d]["outDirectory"], 1,datasets[d]["memory"] )
                
                    arrayName = "%s_%s" % (datasets[d]["arrayName"], tile)
                    raster.CreateDestinationArray(arrayName, raster.height, raster.width, tile, 1)
                    ParallelLoadByChunk(raster.RasterReadingData)


