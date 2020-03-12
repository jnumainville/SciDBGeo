from scidbpy import connect
from osgeo import gdal
import csv
import numpy as np
import os


yWindow = 100


# valueRasterPath = '/home/scidb/data/glc2000_int.tif'
# areaRasterPath = '/home/scidb/data/glc2000_area_ref.tif'



sdb = connect()
rasterDatasets = {
                'glc2000_pumas': {'path': r'/home/scidb/data/glc2000_pumas.tif', 'attribute': 'puma_id', 'type': 'int16'},
                #'glc2000_area_ref': {'path': r'/home/scidb/data/glc2000_area_ref_clipped.tif', 'attribute': 'area','type': 'double'},
                'glc2000_states': {'path': r'/home/scidb/data/glc2000_states.tif', 'attribute': 'state_id', 'type': 'int16'}
                #'glc2000': {'path': r'/home/scidb/data/glc2000_clipped.tif', 'attribute': 'value', 'type': 'int16'}
                }
#rPaths = [r'/home/scidb/data/glc2000_pumas.tif', r'/home/scidb/data/glc2000_area_ref_clipped.tif', r'/home/scidb/data/glc2000_states.tif', r'/home/scidb/data/glc2000_clipped.tif' ]

for r in rasterDatasets:
    #rName = rPath.split('/')[-1].split('.')[0]
    rasterArrayName = '%s_clipped' % r
    arrayName = '%s_array' % (rasterArrayName)

    dataRaster = gdal.Open(rasterDatasets[r]['path'])
    attributeName = rasterDatasets[r]['attribute']
    valueType = rasterDatasets[r]['type']


    width = dataRaster.RasterXSize 
    height  = dataRaster.RasterYSize

    print "Loading..", rasterArrayName, width, height
    

# if brokeline == '':
#     startline = 0
# else:
#     startline = int(brokeline) * yWindow

#rasterArrayName = 'meris2d'
#template =sdb.new_array((5, 5), dtype='double', name=rasterArrayName)
#SciQuery = 'create array %s <f0:uint8> [x=0:%s,%s,1; y=0:%s,%s,1]' % (rasterArrayName, width-1, chunk_size, height-1, chunk_size)

#sdb.query("create array %s <value:int16> [y=0:%s,100000,1, x=0:%s,100000,1]" %  (rasterArrayName, width-1, height-1) )
#sdb.query(SciQuery)


    for version_num, y in enumerate(range(0, height,yWindow)):
        
        
        theName = '%s_%s' % (arrayName,version_num)
        
        csvPath = '/home/scidb/data/%s.scidb' % (theName)
        with open(csvPath, 'wb') as fileout:
            #fileout = csv.writer(csvFile, delimiter = ',')
            fileout.write("{0}[\n")
            #arrayWidth = width-1
            if height - y < yWindow:
                print "Short read height: %s" % (abs(height-(y+yWindow)))
                data_array = dataRaster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=abs(height-(y+yWindow)) ) 
                # arearef_array = areaRaster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=abs(height-(y+yWindow)) )
            else:
                data_array = dataRaster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=yWindow)
                # arearef_array = areaRaster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=yWindow)
            #data_array, arearef_array =  stacked_array
            arrayHeight, arrayWidth = data_array.shape

            it = np.nditer([data_array], flags=['multi_index'], op_flags=['readonly'])
            for v in it:
                row, col = it.multi_index
                if col == arrayWidth-1 and row == arrayHeight-1:
                    fileout.write("(%s,%s,%s)\n]" % (row,col,v.tolist() ) )
                else:
                    fileout.write("(%s,%s,%s),\n" % (row,col,v.tolist() ) )
                    
                ###Use for writing a csv for aio input
                #csvWriter = csv.writer(fileout, delimiter = '\t')
                #csvWriter.writerow([row,col,v.tolist(),a.tolist()])
                
                
        #Create temporary 1D array
        #"create array meris1D <x1:int64, y1:int64, value:int16> [xy=0:*,?,?]"
        sdb.query("create array %s <x1:int64, y1:int64, %s:%s> [xy=0:*,?,?]" % (theName,attributeName, valueType) )
        #This is for an array with 2 attributes
        #sdb.query("create array %s <x1:int64, y1:int64, value:%s, area:double> [xy=0:*,?,?]" % (theName,valueType) )
            
        #Takes almost 4 minutes
        sdb.query("load(%s,'%s')" % (theName, csvPath))
        
        #AIO load requires additonal steps, not sure how much time this saves. Might be faster to go to the binary.
        #Does not need the previous 2 queries. instead it needs an additional redimension step and casting
        #https://github.com/Paradigm4/accelerated_io_tools
        #sdb.query("store(aio_input('%s', 'num_attributes=4' ), %s) "  % (csvPath, theName ))

        #Takes 9 minutes
        if version_num == 0:
            tempArrayName = 'rasterload'

            sdb.query("create array %s <x:int64, y:int64, %s:%s> [xy=0:*,?,?]" % (tempArrayName,attributeName, valueType) )
            #This is for 2 attributes
            #sdb.query("create array %s <x:int64, y:int64, value:%s, area:double> [xy=0:*,?,?]" % (tempArrayName,valueType) )
            sdb.query("store(redimension(apply({A}, x, x1, y, y1), {B}), {B})", A=theName, B=tempArrayName)


            sdb.query("create array %s <%s:%s> [y=0:%s,?,1; x=0:%s,?,1] using %s " % (rasterArrayName, attributeName, valueType, width-1, height-1, tempArrayName) )
            #This is for two attributes
            #sdb.query("create array %s <value:%s, area:double> [y=0:%s,?,1; x=0:%s,?,1] using %s " % (rasterArrayName, valueType, width-1, height-1, tempArrayName) )
        
        # if version_num == 0:
        #     sdb.query("create array %s <value:int16> [y=0:%s,1,?, x=0:%s,1,?] using %s " %  (rasterArrayName, width-1, height-1, theName) )
        #     #sdb.query("create array %s <value:int16> [y1=0:129599:0:?; x1=0:64799:0:?] using %s;" ) % (rasterArrayName, theName)
            
        sdb.query("insert(redimension(apply( {A}, x, x1+{yOffSet}, y, y1 ), {B} ), {B})",A=theName, B=rasterArrayName, yOffSet=y)    
            
        if version_num >= 1:
            #print "remove_versions(%s, %s)" % (rasterArrayName, version_num)
            sdb.query("remove_versions(%s, %s)" % (rasterArrayName, version_num))

        print '%s of %s' % (version_num+1, height/yWindow)
        sdb.query("remove(%s)" % (theName))
        os.remove(csvPath)

    
    
    #Create new Array 1D
    # height, width = rArray.shape()
    # create array %s <value:uint8> [x=0:%s,%s,1; y=0:%s,%s,1]"" % (width-1, chunk_size, height-1, chunk_size)
    # create array meris1 <x1:int64, y1:int64, value:int16> [xy=0:*,100000,1];
    # iquery -anq load(meris1, '/home/davd/data/meris_0.csv')
    #create array meris2d <value:int16> [x=0:129599,100000,1, y=0:64799,100000,1];

    # time anq "insert(redimension(apply(meris1, x, x1, y, y1), meris2d), meris2d);"


    # #This is a 1d array
    # SciArray = sdb.from_array(rArray)
    # #arrayName = str(SciArray.name)
    # #sdb.query('insert(redimension(apply( {A}, x, {A.d0}+4, y, {A.d1} ), {B} ), {B})',A = SciArray, B=rasterArrayName)    
    # sdb.query('insert(redimension(apply( {A}, y, {A.d0}+{yOffSet}, x, {A.d1} ), {B} ), {B})',A = SciArray, B=rasterArrayName, yOffSet=y)


        sdb.query("remove_versions(%s, %s)" % (rasterArrayName, version_num+1))

    sdb.query("remove(%s)" % (tempArrayName))
print 'Finished'




    