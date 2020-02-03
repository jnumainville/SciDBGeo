class GDAL_functions(object):

    def __init__():
        """
        Description

        Input:

        Output:

        """

        pass


def world2Pixel(geoMatrix, x, y):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the pixel location of a geospatial

    Input:
        geoMatrix =
        x =
        y =

    Output:

    """
    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]

    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / xDist)
    
    return (pixel, line)


def Pixel2world(geoMatrix, row, col):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the x,y location of a pixel location

    Input:
        geoMatrix:
        row:
        col:

    Output:

    """

    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]

    x_coord = (ulX + (row * xDist))
    y_coord = (ulY - (col * xDist))

    return (x_coord, y_coord)