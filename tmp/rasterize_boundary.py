# -*- coding: utf-8 -*-
"""
This script takes the geometry and rasterizes using the reference raster resolution, clipped the vector extent
"""



from osgeo import ogr, gdal





def world2Pixel(geoMatrix, x, y):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the pixel location of a geospatial coordinate
    """
    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]

    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / xDist)
    
    return (pixel, line)
    

def RasterizePolygon(inRasterPath, outRasterPath, vectorPath):
    #The array size, sets the raster size 
    inRaster = gdal.Open(inRasterPath)
    rasterTransform = inRaster.GetGeoTransform()
    pixel_size = rasterTransform[1]
    
    #Open the vector dataset
    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()
    geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()
    
    outTransform= [geomMin_X, rasterTransform[1], 0, geomMax_Y, 0, rasterTransform[5] ]
    
    rasterWidth = int((geomMax_X - geomMin_X) / pixel_size)
    rasterHeight = int((geomMax_Y - geomMin_Y) / pixel_size)
    
    #Masked Raster of the WebCoverageService
    tiffDriver = gdal.GetDriverByName('GTiff')
    theRast = tiffDriver.Create(outRasterPath, rasterWidth, rasterHeight, 1, gdal.GDT_Int16)

    theRast.SetProjection(inRaster.GetProjection())
    theRast.SetGeoTransform(outTransform)
    
    band = theRast.GetRasterBand(1)
    band.SetNoDataValue(-999)

    #Rasterize
    gdal.RasterizeLayer(theRast, [1], theLayer, options=["ATTRIBUTE=ID"])
    
    del theRast, inRaster

    


def ClipRaster(inRasterPath,clippedPath, outType, vectorPath):
    
    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()
    geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()
    
    
    
    inRaster = gdal.Open(inRasterPath)

    rasterTransform = inRaster.GetGeoTransform()
    pixel_size = rasterTransform[1]
    
    ulY, ulX = world2Pixel(inRaster.GetGeoTransform(), geomMin_X, geomMax_Y )

    outTransform= [geomMin_X, rasterTransform[1], 0, geomMax_Y, 0, rasterTransform[5] ]
    
    rasterWidth = int((geomMax_X - geomMin_X) / pixel_size)
    rasterHeight = int((geomMax_Y - geomMin_Y) / pixel_size)
    
    tiffDriver = gdal.GetDriverByName('GTiff')
    clippedRaster = tiffDriver.Create(clippedPath, rasterWidth, rasterHeight, 1, outType)

    outputArray = inRaster.ReadAsArray(xoff=ulY, yoff=ulX, xsize=rasterWidth, ysize=rasterHeight)
    
    clippedRaster.SetProjection(inRaster.GetProjection())
    clippedRaster.SetGeoTransform(outTransform)
    
    theBandRast = clippedRaster.GetRasterBand(1)    
    theBandRast.SetNoDataValue(-999)
    theBandRast.WriteArray(outputArray)
    
    del inRaster, clippedRaster


vDatasets = {'us_pumas': {'inPath': r'c:\scidb\US_2000_pumas_4326_simplified.shp', 'outPath' : r'c:\scidb\glc2000_pumas.tif'},
             'us_states': {'inPath': r'c:\scidb\US_states.shp', 'outPath' : r'c:\scidb\glc2000_states.tif'}  }

rDatasets= {'categorical': {'inPath': r'c:\scidb\glc2000_int.tif', 'outPath': r'c:\scidb\glc2000_clipped.tif', 'type': gdal.GDT_Int16}, 
            'area_reference':{'inPath': r'c:\scidb\glc2000_area_ref.tif', 'outPath' : r'c:\scidb\glc2000_area_ref_clipped.tif', 'type': gdal.GDT_Float64} }

for r in rDatasets:
    ClipRaster(rDatasets[r]['inPath'],rDatasets[r]['outPath'], rDatasets[r]['type'], vDatasets['us_states']['inPath'])

print("Finished")
