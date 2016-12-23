'''
Author: Joshua Donato

Purpose:  This script will create a new raster with values indicating the area of each pixel in the
specified raster.  The coordinates of the pixels in the first column are used to build a polygon.
The polygon is then transformed from the SRID of the source raster to a new SRID.  The areas of the
polygons are then calculated.  Areas are written to the output raster line by line by expanding
the individual areas into a list with as many elements as there are pixels in a row.
'''

import sys, traceback
#sys.path.append(r"C:\Users\donat050\Documents\GitHub\spatial-tools\UtilityModules")

from osgeo import gdal, ogr, osr
#import gdal, ogr, osr
import numpy


#-----Parameters-----------------------------------------------------------------------------------

#  Full path to the raster you want to use to generate areas.
# src_raster = r'E:\MERIS\lccs\ESACCI-LC-L4-LCCS-Map-300m-P5Y-2000-v1.3.tif'
src_raster = r'C:\scidb\meris_2000.tif'

#  Full path for the area reference raster to be created.
# area_raster = r'E:\MERIS\lccs\MERIS_LCCS_2000_area_ref.tif'
area_raster = r'C:\scidb\meris_arearef.tif'

#  SRID of the source raster.
src_srid = 4326

#  SRID to use for calculating area.
tgt_srid = 3410

#  X offset.  If the first column of pixels straddle 180/-180, give an
#  x offset (# of pixels) that will move the column of pixels to the
#  right so it is no longer straddle 180/-180.  Otherwise, the area
#  will not be calculated correctly.
x_offset = 100


#-----Error Handling-------------------------------------------------------------------------------

errorPrinted = False


def printPyError():
    global errorPrinted
    if errorPrinted == False:
        # Get the traceback object
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
    
        # Concatenate information together concerning the error into a message string
        print "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])      
        errorPrinted = True


#-----Main Code------------------------------------------------------------------------------------

try:

    #  Open the raster.
    dataset = gdal.Open(src_raster, gdal.GA_ReadOnly)
    
    #  Get the geotransform information.
    print "Acquiring geotransform..."
    geotransform = dataset.GetGeoTransform()
    
    if geotransform is None:
        print "Acquiring geotransform: Failed!"
    else:
        #  get top left corner, pixel width, and pixel height values
        left = geotransform[0]  #  top left x
        top = geotransform[3]  #  top left y
        pixelWidth = geotransform[1]  #  W-E pixel resolution
        pixelHeight = geotransform[5]  #  N-S pixel resolution
        
        rowCount = dataset.RasterYSize
        colCount = dataset.RasterXSize
        
        #  Get the transform to be used between SRIDs.
        src_sr = osr.SpatialReference()
        src_sr.ImportFromEPSG(src_srid)
        tgt_sr = osr.SpatialReference()
        tgt_sr.ImportFromEPSG(tgt_srid)
        the_transform = osr.CoordinateTransformation(src_sr, tgt_sr)
        
        #  Generate polygons representing the first pixel of each row and get
        #  the areas for the polygons.
        print "Retrieving areas..."
        areas = []
        for y in range(1, rowCount + 1):
            tl_x = left + (x_offset * pixelWidth)
            tr_x = left + pixelWidth + (x_offset * pixelWidth)
            br_x = tr_x
            bl_x = tl_x
            
            tl_y = top + ((y - 1) * pixelHeight)
            tr_y = tl_y
            br_y = top + (y * pixelHeight)
            bl_y = br_y
            
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint(tl_x, tl_y)
            ring.AddPoint(tr_x, tr_y)
            ring.AddPoint(br_x, br_y)
            ring.AddPoint(bl_x, bl_y)
            ring.AddPoint(tl_x, tl_y)
            
            #  Create the polygon and transform.
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)
            poly.Transform(the_transform)
            
            #  Get the area and add to the list.
            area = poly.Area()
            areas.append(area)
        
        #  Create the new raster.
        print "Creating the new raster..."
        driver = gdal.GetDriverByName('GTiff')
        outRaster = driver.Create(area_raster, colCount, rowCount, 1, gdal.GDT_Float32)
        outRaster.SetGeoTransform((left, pixelWidth, 0, top, 0, pixelHeight))
        outband = outRaster.GetRasterBand(1)
        
        #  Populate the new raster with values.
        strRowCount = str(rowCount)
        for y in range(len(areas)):
            print "\tProcessing row: %d of %s" % (y + 1, strRowCount,)
            
            #  Make the single area value a list representing the area values for the row of pixels.
            #  Need a 2D array to use numpy.array() so add areaList to another list (tempList).
            #  Get a numpy.array (rasterArray) from tempList.
            #  Write rasterArray to the band.
            areaList = [areas[y]] * colCount
            tempList = [areaList]
            rasterArray = numpy.array(tempList)
            outband.WriteArray(rasterArray, yoff=y)
                
        del tempList
        del rasterArray
        
        #  Set the spatial reference information for the area raster.
        outRasterSRS = osr.SpatialReference()
        outRasterSRS.ImportFromEPSG(src_srid)
        outRaster.SetProjection(outRasterSRS.ExportToWkt())
        
        outband.FlushCache()
        
        print "\n\tGenerating statistics.  This may take a while..."
        outband.GetStatistics(0,1)
    
    print "\nDone!"
    
except:
    printPyError()
