from SciDBParallel import RasterLoader, ParallelLoadByChunk
from scidb import iquery


if __name__ == '__main__':
        datasets = {
                    "glc": {"geoTiffPath": "/home/04489/dhaynes/glc2000_clipped.tif", "arrayName": "glc_2000_clipped_2", "attribute": "value", "outDirectory": "/storage", "memory": 5000000 },
                    "meris": {"geoTiffPath": "/home/04489/dhaynes/meris_2010_clipped.tif", "arrayName": "meris_2010_clipped_2", "attribute": "value", "outDirectory": "/storage", "memory": 20000000},
                    "nlcd": {"geoTiffPath": "/home/04489/dhaynes/nlcd_2006.tif", "arrayName": "nlcd_2006_overlap_2",  "attribute": "value", "outDirectory": "/storage", "memory": 20000000}
                    }
        tileSizes = [1500] #[500, 1000, 2000, 2500, 3000, 3500, 4000] #500
        sdb = iquery()
        for d in datasets:
                for c, tile in enumerate(tileSizes):
                    
                    arrayName = "%s_%s" % (datasets[d]["arrayName"], tile)

                    raster = RasterLoader(datasets[d]["geoTiffPath"], arrayName, [datasets[d]["attribute"]], tile, datasets[d]["outDirectory"], 1,datasets[d]["memory"] )
                    raster.CreateDestinationArray(arrayName, raster.height, raster.width, tile, 0)

                    ParallelLoadByChunk(raster.ParalleReadingData)