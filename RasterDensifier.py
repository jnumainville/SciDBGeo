# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 15:34:23 2017
This script will densify a given raster dataset
@author: dahaynes
"""

from osgeo import gdal
import numpy as np


def densification(theArray, multiplier):
    """
    This function generates a numpy dense array based on the multiplier
    In:
        theArray is valid numpy array
        multiplier is an integer above 1
    Out:
        denseRaster is a new numpy array
    """
    denseRaster = np.array([np.repeat(row, multiplier).tolist() for row in theArray for i in range(multiplier)])
    
    return denseRaster


def raster_resolution(geoTransform, densification):
    """
    Adjusts the spatial resolution for the geoTransform
    """
    
    NewTransform = list(geoTransform)
    NewTransform[1] = NewTransform[1]/ densification
    NewTransform[5] = NewTransform[5]/ densification

    return NewTransform



def main(inRasterFilePath, densifier, iterator, outRasterFilePath ):
    """
    This function is a loop that reads the raster and densifies it
    """
    
    r = gdal.Open(inRasterFilePath)
    rasterType = r.GetRasterBand(1).DataType
    
    rasterTransform = r.GetGeoTransform()
    rasterProjection = r.GetProjection()
    alteredTransform = raster_resolution(rasterTransform, densifier)
    
    numCols = r.RasterXSize
    numRows = r.RasterYSize
    #print(numRows, numCols)
    
    #Create new raster file
    tiffDriver = gdal.GetDriverByName('GTiff')
    theRast = tiffDriver.Create(outRasterFilePath, numCols*densifier , numRows*densifier, 1, rasterType, options = [ 'COMPRESS=DEFLATE' ])
    if theRast:
        theRast.SetProjection(rasterProjection)
        theRast.SetGeoTransform(alteredTransform)
    
        theBand = theRast.GetRasterBand(1)
        theBand.SetNoDataValue(255)
        
        rowCounter = 0
        for row in range(0, numRows, iterator):
            rowCounter += iterator
            if rowCounter < numRows:
                array = r.ReadAsArray(yoff=row, xoff=0 , xsize=r.RasterXSize, ysize=iterator)
            else:
                rowCounter -= iterator
                #print(r.RasterYSize, rowCounter)
                array = r.ReadAsArray(yoff=row, xoff=0 , xsize=r.RasterXSize, ysize=r.RasterYSize-rowCounter)
            
            #print(row, rowCounter, array.shape)
            densifiedArray = densification(array, densifier)
            theBand.WriteArray(densifiedArray, yoff=row*densifier)
            del densifiedArray
            
            #write_raster(outRasterFilePath, densifiedArray, row)
        
        #Necessary to close and write the raster
        del theRast
        
    else:
        print("Error, Raster not created")
    
    
    


def densifiction_grid_resolution(Transform):

    #### This is a function #####
    x = Transform[0]
    y = Transform[3]
    x_res = Transform[1]
    y_res = Transform[5]
    x_start =  x + x_res
    y_start =  y + y_res

    return x_start, y_start, x_res, y_res


def argument_parser():
    """
    Parse arguments and return parser object
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Script for generating dense raster datasets")    
    
    parser.add_argument("-RasterPath", required =True, help="Input file path for the raster", dest="input")    
    parser.add_argument("-Densifier", required =True, type=int, help="Densifies the raster", dest="dense", default=2)
    parser.add_argument("-Iterator", required =True, type=int, help="Iterator value default is 100", dest="iter", default=100)
    parser.add_argument("-OutPath", required =True, help="Output file path for the raster", dest="output")    

    return parser


#inR = r"c:\scidb\glc2000_clipped.tif"
#outR = r"c:\scidb\glc2000_clipped2x.tif"
#main(inR, 2, 100, outR)

if __name__ == '__main__':
     args = argument_parser().parse_args()
     main(args.input, args.dense, args.iter, args.output)
     print("Done")
    
