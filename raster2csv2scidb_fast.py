from scidbpy import connect
from osgeo import gdal
#import csv
import numpy as np
import os

chunk_size = 100000
yWindow = 100
rasterArrayName = 'meris2010_fast'
rasterPath = '/home/david/data/ESACCI_300m_2010.tif'

sdb = connect()

raster = gdal.Open(rasterPath)
width = raster.RasterXSize 
height  = raster.RasterYSize


#rasterArrayName = 'meris2d'
#template =sdb.new_array((5, 5), dtype='double', name=rasterArrayName)
#SciQuery = 'create array %s <f0:uint8> [x=0:%s,%s,1; y=0:%s,%s,1]' % (rasterArrayName, width-1, chunk_size, height-1, chunk_size)

#sdb.query("create array %s <value:int16> [y=0:%s,100000,1, x=0:%s,100000,1]" %  (rasterArrayName, width-1, height-1) )
#sdb.query(SciQuery)


for version_num, y in enumerate(range(0, height,yWindow)):
	fileName = 'meris1D_%s' % (version_num)
	csvPath = '/home/david/data/%s.scidb' % (fileName)
	with open(csvPath, 'wb') as fileout:
		#fileout = csv.writer(csvFile, delimiter = ',')
		fileout.write("{0}[\n")
		#arrayWidth = width-1
		rArray = raster.ReadAsArray(xoff=0, yoff=y, xsize=width, ysize=yWindow)
		arrayHeight, arrayWidth = rArray.shape
		it = np.nditer(rArray, flags=['multi_index'], op_flags=['readonly'])
		for pixel in it:
			row, col = it.multi_index
			if col == arrayWidth-1 and row == arrayHeight-1:
				fileout.write("(%s,%s,%s)\n]" % (row,col,it.value.tolist()) )
			else:
				fileout.write("(%s,%s,%s),\n" % (row,col,it.value.tolist()) )
			#fileout.writerow([row,col,it.value.tolist() ])

	#Create temporary 1D array

	#"create array meris1D <x1:int64, y1:int64, value:int16> [xy=0:*,?,?]"
	sdb.query("create array %s <x1:int64, y1:int64, value:int16> [xy=0:*,?,?]" % (fileName,) )
	#Takes almost 4 minutes
	sdb.query("load(%s,'%s')" % (fileName, csvPath))
	#Takes 9 minutes

	if version_num == 0:
		tempArrayName = 'rasterload'

		sdb.query("create array %s <x:int64, y:int64, value:int16> [xy=0:*,?,?]" % (tempArrayName,) )
		sdb.query("store(redimension(apply({A}, x, x1, y, y1), {B}), {B})", A=fileName, B=tempArrayName)


		sdb.query("create array %s <value:int16> [y=0:%s,?,1; x=0:%s,?,1] using %s " % (rasterArrayName, width-1, height-1, tempArrayName) )
	
	# if version_num == 0:
	# 	sdb.query("create array %s <value:int16> [y=0:%s,1,?, x=0:%s,1,?] using %s " %  (rasterArrayName, width-1, height-1, fileName) )
	# 	#sdb.query("create array %s <value:int16> [y1=0:129599:0:?; x1=0:64799:0:?] using %s;" ) % (rasterArrayName, fileName)
		
	sdb.query("insert(redimension(apply( {A}, x, x1+{yOffSet}, y, y1 ), {B} ), {B})",A=fileName, B=rasterArrayName, yOffSet=y)	
		
	if version_num >= 1:
		#print "remove_versions(%s, %s)" % (rasterArrayName, version_num)
		sdb.query("remove_versions(%s, %s)" % (rasterArrayName, version_num))

	print '%s of %s' % (version_num+1, height/yWindow)
	sdb.query("remove(%s)" % (fileName))
	os.remove(csvPath)

	#break
	
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


# sdb.query("remove_versions(%s, %s)" % (rasterArrayName, version_num+1))
print 'Finished'




	