import numpy as np
from collections import defaultdict
from itertools import groupby, cycle, product

from scidbpy import connect
#from osgeo import gdal
import os


# byte format for binary scidb data
typemap = {'bool': np.dtype('<b1'),
           'int8': np.dtype('<b'),
           'uint8': np.dtype('<B'),
           'int16': np.dtype('<h'),
           'uint16': np.dtype('<H'),
           'int32': np.dtype('<i'),
           'uint32': np.dtype('<I'),
           'int64': np.dtype('<l'),
           'uint64': np.dtype('<L'),
           'float': np.dtype('<f4'),
           'double': np.dtype('<d'),
           'char': np.dtype('c'),
           'datetime': np.dtype('<M8[s]'),
           'datetimetz': np.dtype([(str('time'), '<M8[s]'), (str('tz'), '<m8[s]')]),
           'string': np.dtype('object')
           }

null_typemap = dict(((k, False), v) for k, v in typemap.items())
null_typemap.update(((k, True), [(str('mask'), np.dtype('<B')), (str('data'), v)])
                    for k, v in typemap.items())

# NULL value for each datatype
NULLS = defaultdict(lambda: np.nan)
NULLS['bool'] = np.float32(np.nan)
NULLS['string'] = None
NULLS['float'] = np.float32(np.nan)
NULLS['datetime'] = np.datetime64('NaT')
NULLS['datetimetz'] = np.zeros(1, dtype=typemap['datetimetz'])
NULLS['datetimetz']['time'] = np.datetime64('NaT')
NULLS['datetimetz']['tz'] = np.datetime64('NaT')
NULLS['datetimetz'] = NULLS['datetimetz'][0]
NULLS['char'] = '\0'

for k in typemap:
    NULLS[typemap[k]] = NULLS[k]

# numpy datatype that each sdb datatype should be promoted to
# if nullable
NULL_PROMOTION = defaultdict(lambda: np.float)
NULL_PROMOTION['float'] = np.dtype('float32')
NULL_PROMOTION['datetime'] = np.dtype('<M8[s]')
NULL_PROMOTION['datetimetz'] = np.dtype('<M8[s]')
NULL_PROMOTION['char'] = np.dtype('c')
NULL_PROMOTION['string'] = object

# numpy datatype that each sdb datatype should be converted to
# if not nullable
mapping = typemap.copy()
mapping['datetimetz'] = np.dtype('<M8[s]')
mapping['string'] = object



def GDALtoSciDB():
    chunkSize = 100000
    chunkOverlap = 0
    yWindow = 100
    rasterArrayName = 'MERIS_2010'
    rasterPath = '/home/04489/dhaynes/data/ESACCI_300m_2010.tif'
    tempFileOutPath = '/mnt'
    tempFileSciDBLoadPath = '/data/04489/dhaynes'

    sdb = connect('http://iuwrang-xfer2.uits.indiana.edu:8080')
    ReadGDALFile(sdb, rasterArrayName, rasterPath, yWindow, tempFileOutPath, tempFileSciDBLoadPath, chunkSize, chunkOverlap)


def ReadGDALFile(sdb, rasterArrayName, rasterPath, yWindow, tempOutDirectory, tempSciDBLoad, chunk=1000, overlap=0):
    from osgeo import gdal
    from gdalconst import GA_ReadOnly
    import timeit

    raster = gdal.Open(rasterPath, GA_ReadOnly)
    width = raster.RasterXSize 
    height  = raster.RasterYSize

    
    ##test to make sure global array exists

    for version_num, y in enumerate(range(0, height,yWindow)):
        tempRastName = 'temprast_%s' % (version_num)
        csvPath = '%s/%s.sdbbin' % (tempOutDirectory,tempRastName)
        rowsRemaining = height - version_num*yWindow

        #Start timing
        totalstart = timeit.default_timer()
        #If then statement to account for final short read
        if rowsRemaining >= yWindow:    
            rArray = raster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=yWindow)
        else:
            rArray = raster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=rowsRemaining)

        rasterValueDataType = rArray.dtype

        if version_num == 0:
            #Create final destination array           
            sdb.query("create array %s <value:%s> [y=0:%s,?,0; x=0:%s,?,0]" %  (rasterArrayName, rasterValueDataType, width-1, height-1) )
            #pass
        
        #Write the Array to Binary file
        start = timeit.default_timer()      
        aWidth, aHeight = WriteMultiDimensionalArray(rArray, csvPath)
        os.chmod(csvPath, 0o755)
        stop = timeit.default_timer()
        writeBinaryTime = stop-start
                    
        #Create the array, which will hold the read in data. X and Y coordinates are different on purpose 
        sdb.query("create array %s <x1:int64, y1:int64, value:%s> [xy=0:*,?,?]" % (tempRastName, rasterValueDataType) )
        
        #Time the loading of binary file
        start = timeit.default_timer()
        binaryLoadPath = '%s/%s.sdbbin' % (tempSciDBLoad,tempRastName )
        sdb.query("load(%s,'%s', -2, '(int64, int64, %s)' )" % (tempRastName, binaryLoadPath, rasterValueDataType))
        stop = timeit.default_timer() 
        loadBinaryTime = stop-start

        #This code is for inputing a multidimensional array with overlaps
        #if version_num == 0:
            #tempArrayName = 'rasterload'
            #Create new 1D array with new attribute value
            #sdb.query("create array %s <x:int64, y:int64, value:%s> [xy=0:%s,?,0]" % (tempArrayName, rasterValueDataType, aWidth*aHeight-1) )
            #sdb.query("store(redimension(apply({A}, x, x1, y, y1), {B}), {B})", A=tempRastName, B=tempArrayName)

            #Statement for creating Final Raster 
            #sdb.query("create array %s <value:%s> [y=0:%s,?,0; x=0:%s,?,0] using %s " %  (rasterArrayName, rasterValueDataType, width-1, height-1, tempArrayName))
            #sdb.query("remove(rasterload)")

        #Time the redimensions
        start = timeit.default_timer()
        sdb.query("insert(redimension(apply( {A}, x, x1+{yOffSet}, y, y1 ), {B} ), {B})",A=tempRastName, B=rasterArrayName, yOffSet=y)
        stop = timeit.default_timer() 
        redimensionArrayTime = stop-start

        if version_num >= 1:
            CleanUpTemp(sdb, rasterArrayName, version_num, csvPath, tempRastName)
        
        totalstop = timeit.default_timer()    
        NumberOfIterations = int(round( height/float(yWindow) +.5))

        print('Completed %s of %s' % (version_num+1, NumberOfIterations) )

        if version_num == 0:
            totalTime = totalstop - totalstart
            print('Took %s seconds to complete' % (totalTime))
            print("Writing time: %s, Loading time: %s, Redimension time: %s " % (writeBinaryTime, loadBinaryTime, redimensionArrayTime) )
            print('Estimated time to load (%s) = time %s * loop %s' % ( totalTime*NumberOfIterations,  totalTime, NumberOfIterations) )
            CleanUpTemp(sdb, rasterArrayName, version_num, csvPath, tempRastName)
        
            

def WriteMultiDimensionalArray(rArray, csvPath ):
    '''This function write the multidimensional array as a binary '''
    with open(csvPath, 'wb') as fileout:
        arrayHeight, arrayWidth = rArray.shape
        it = np.nditer(rArray, flags=['multi_index'], op_flags=['readonly'])
        for counter, pixel in enumerate(it):
            row, col = it.multi_index

            indexvalue = np.array([row,col], dtype=np.dtype('int64'))

            fileout.write( indexvalue.tobytes() )
            fileout.write( it.value.tobytes() )
   
    return(arrayHeight, arrayWidth)
    
def CleanUpTemp(sdb, rasterArrayName, version_num, csvPath, tempRastName):
    'Remove all temporary files'
    if version_num > 0:
       sdb.query("remove_versions(%s, %s)" % (rasterArrayName, version_num))
    sdb.query("remove(%s)" % (tempRastName))
    os.remove(csvPath)


if __name__ == '__main__':
    GDALtoSciDB()

