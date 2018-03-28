from SciDBParallel import RasterLoader, ParallelLoad
from scidb import iquery, Statements


if __name__ == '__main__':
	datasets = {
				"glc": {"geoTiffPath": "/home/04489/dhaynes/glc2000_clipped.tif", "arrayName": "glc_2000_clipped": , "attribute": "value", "outDirectory": "/storage"}, 
				"meris": {"geoTiffPath": "/home/04489/dhaynes/meris_2010_clipped.tif", "arrayName": "meris_2010_clipped": , "attribute": "value", "outDirectory": "/storage"},
				"nlcd": {"geoTiffPath": "/home/04489/dhaynes/nlcd_2006.tif", "arrayName": "nlcd_2006": , "attribute": "value", "outDirectory": "/storage"}
				 }
	tileSizes = [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
	sdb = iquery()
	for d in datasets:
		raster = RasterLoader(datasets[d]["geoTiffPath"], datasets[d]["arrayName"], datasets[d]["value"], 0, datasets[d]["outDirectory"])
		ParallelLoad(rasterMetadata)
		raster.CreateLoadArray("LoadArray", raster.AttributeString, raster.RasterArrayShape)

		sdb_statements = Statements(sdb)
		sdb_statements.LoadOneDimensionalArray(-1, "LoadArray", raster.AttributeString, 1, 'pdataset.scidb')

		for t in tileSizes:
			arrayName = "%s_%s" % (datasets[d]["arrayName"], t)
			raster.CreateDestinationArray(arrayName, raster.height, raster.width, raster.chunksize)
			sdb_statements.InsertRedimension( "LoadArray", arrayName)