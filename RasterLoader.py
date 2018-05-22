from SciDBParallel import RasterLoader, ParallelLoad
from scidb import iquery, Statements


if __name__ == '__main__':
        datasets = {
                      #"glc": {"geoTiffPath": "/home/04489/dhaynes/glc2000_clipped.tif", "arrayName": "glc_2000_clipped", "attribute": "value", "outDirectory": "/storage"}, 
                     #"meris": {"geoTiffPath": "/home/04489/dhaynes/meris_2010_clipped.tif", "arrayName": "meris_2010_clipped", "attribute": "value", "outDirectory": "/storage"}
                     #"nlcd": {"geoTiffPath": "/home/04489/dhaynes/nlcd_2006.tif", "arrayName": "nlcd_junk",  "attribute": "value", "outDirectory": "/storage"}
                    "meris3Meter" :{"geoTiffPath": "/group/meris_3m/meris_3m.vrt", "arrayName": "meris_2010_3m",  "attribute": "value", "outDirectory": "/storage" }
                    }
        #tileSizes = [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
        sdb = iquery()
        for d in datasets:
                print(datasets[d])
                raster = RasterLoader(datasets[d]["geoTiffPath"], datasets[d]["arrayName"], [datasets[d]["attribute"]], 0, datasets[d]["outDirectory"], destinationArray="Meris_3M_500")
                raster.CreateDestinationArray("Meris_3M_500", raster.height, raster.width, 500)
                ParallelLoad(raster.RasterMetadata)
                # loadAttribute = "%s_1:%s" % (raster.AttributeString.split(":")[0], raster.AttributeString.split(":")[1])
                # print(loadAttribute)
                # raster.CreateLoadArray("LoadArray", loadAttribute, raster.RasterArrayShape)

                # sdb_statements = Statements(sdb)
                # sdb_statements.LoadOneDimensionalArray(-1, "LoadArray", loadAttribute, 1, 'pdataset.scidb')

                # for t in tileSizes:
                #         arrayName = "%s_%s" % (datasets[d]["arrayName"], t)
                        
                #         sdb_statements.InsertRedimension( "LoadArray", arrayName, oldvalue=loadAttribute.split(":")[0], newvalue='value')
