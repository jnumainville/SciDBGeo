class iquery(object):

    def __init__(self,):
        import subprocess
        import csv
        import re
        self.subprocess = subprocess
        self.PIPE = subprocess.PIPE
        self.csv = csv


    def query(self, theQuery):
        scidbArguments = """iquery -anq "%s";""" % (theQuery)
        print(scidbArguments)
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, stderr=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()

        if err: 
            print("Error: %s" % (err))
            raise
        #print("Output: %s" % (out))
        del p
        return out

    def queryResults(self, theQuery, thePath):
        import os, csv
        
        # with open(thePath, 'w'):
        #     os.chmod(thePath, 0o777)

        scidbArguments = """iquery -aq "%s;" -o CSV -r %s""" % (theQuery, thePath)
        # scidbArguments.append( "-aq %s" % (theQuery))
        # scidbArguments.append( "-o CSV")
        # scidbArguments.append( "-r %s" % (thePath))
        
        #print(scidbArguments)
        p = self.subprocess.Popen(scidbArguments, shell=True)
        p.wait()
        with open(thePath) as infile:
            data = csv.reader(infile)
            #min(value), max(value), avg(value), count(value)
            print("geoid, min, max, average, count")
            for d in data:
                print(d)
        #print(p.communicate)
        del p
        

    def queryAFL(self, theQuery):
        scidbArguments = """iquery -aq "%s";""" % (theQuery)
        #print(scidbArguments)
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()
        #print("OUT", out)

        return out

    def queryCSV(self, theQuery, theCSVPath):
        """

        """

        import os
        
        #if os.pathisdir( "/".join(theCSVPath.split("/")[:-1]) ):
        
        scidbArguments = """iquery -aq "%s" -o csv+ -r %s;""" % (theQuery, theCSVPath)
        #print(scidbArguments)
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()

        # else:
        #     print("Bad CSV path: %s" % (theCSVPath))
            
        #out, err = p.communicate()
        #print("OUT", out)

        return theCSVPath
        
    def aql_query(self, theQuery):
        scidbArguments = """iquery -q "%s";""" % (theQuery)
        #print(scidbArguments)
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()
        #print("OUT", out)

        return out


    def versions(self, theArray):
        """

        """
        scidbArguments = """iquery -aq "versions(%s)"; """ % (theArray)
        #print(scidbArguments)
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()

        #print("OUT", out)
        #results = out.decode("utf-8")
        resultsList = out.decode("utf-8").split("\n")
        versions = []
        #b"{VersionNo} version_id,timestamp\n{1} 10,'2017-06-17 02:52:59'\n{2} 11,'2017-06-17 02:53:35'\n"
        try:
            for r in resultsList[1:]:
                if len(r) > 1:
                    positionversion, datetime = r.split(",")
                    position, version = positionversion.split(" ")
                    position = int(position.replace("{", "").replace("}", ""))
                    date, time = datetime.replace("'", "").split(" ")
                    versions.append(version)
        except:
            print(resultsList)
        
        return versions


    def list(self, item='arrays'):
        """
        Method for returning the list of arrays, could be easily used for other
        """

        scidbArguments = """iquery -aq "list('%s')"; """ % (item)
        
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()

        #print("OUT", out)
        #results = out.decode("utf-8")
        resultsList = out.decode("utf-8").split("\n")
        arrayNames = []
        #{No} name,uaid,aid,schema,availability,temporary
        #{0} 'glc_1000',7227,7227,'glc_1000<value:uint8> [y=0:16352:0:1000; x=0:40319:0:1000]',true,false
        
        try:
            for r in resultsList[1:]:
                if len(r) > 1:
                    positionArrayName, uaid, aid,schema,availability,temporary = r.split(",")
                    position, name = positionArrayName.split(" ")
                    name = name.replace("'", "")
                    arrayNames.append(name)
        except:
            print(resultsList)
        
        return arrayNames

    def OutputToArray(self, filePath, valueColumn, yColumn=1):
        """
        The CSV array, must output the y and x coordinate values.
        """
        import numpy as np

        with open(filePath, 'r') as filein:
            dataset = np.loadtxt(filein, delimiter=',', dtype=np.float)
            # thedoc = self.csv.reader(filein)
            # doclines = [line for line in thedoc]
            # dataset = np.array(doclines)

        # del doclines

        ycoordinates = dataset[:,yColumn]

        unique_y, number_of_y = np.unique(ycoordinates, return_counts=True)
        height = len(unique_y)
        width = number_of_y[0]
        valuearray = dataset[:,valueColumn]

        #array = self.np.array([row.split(',')[columnReader] for row in csv[:-1] ]).reshape((1960, width))

        return valuearray.reshape( (height,width) )

    def WriteRaster(self, inArray, inGeoTiff, outPath, noDataValue=-999):
        """

        """
        from osgeo import ogr, gdal
        driver = gdal.GetDriverByName('GTiff')

        height, width = inArray.shape
        #pixelType = self.NumpyToGDAL(inArray.dtype)
        #pixelType = gdal_array.NumericTypeCodeToGDALTypeCode(inArray.dtype)
        #https://gist.github.com/CMCDragonkai/ac6289fa84bcc8888035744d7e00e2e6
        r = gdal.Open(inGeoTiff)

        if driver:
            geoTiff = driver.Create(outPath, width, height, 1, 6)
            geoTiff.SetGeoTransform( r.GetGeoTransform() )
            geoTiff.SetProjection( r.GetProjection() )
            band = geoTiff.GetRasterBand(1)
            band.SetNoDataValue(noDataValue)

            band.WriteArray(inArray)
            geoTiff.FlushCache()

        del geoTiff

class Statements(object):

    def __init__(self,sdb):
        """
        Must supply the sdb connection
        """
        self.sdb  = sdb


    def CreateLoadArray(self, tempRastName, attribute_name, rasterArrayType):
        """
        Create the loading array
        """

        if rasterArrayType <= 2:
            theQuery = "create array %s <y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)
        elif rasterArrayType == 3:
            theQuery = "create array %s <z1:int64, y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)
        
        try:
            #print(theQuery)
            self.sdb.query(theQuery)
        except:
            #Silently deleting temp arrays
            self.sdb.query("remove(%s)" % (tempRastName))
            self.sdb.query(theQuery)
                

    def LoadOneDimensionalArray(self, sdb_instance, tempRastName, rasterAttributes, rasterType, binaryLoadPath):
        """
        Function for loading GDAL data into a single dimension
        """

        if rasterType == 2:                
            items = [attribute.split(":")[1].strip() for attribute in rasterAttributes.split(",")  ]
            attributeValueTypes = ", ".join(items)
        else:
            attributeValueTypes = rasterAttributes.split(":")[1]
        
        try:
            query = "load(%s, '%s' ,%s, '(int64, int64, %s)') " % (tempRastName, binaryLoadPath, sdb_instance, attributeValueTypes)
            #print(query)
            self.sdb.query(query)
            return 1
        except:
            print("Error Loading DimensionalArray")
            print(query)
            return 0

    def InsertRedimension(self, tempRastName, destArray, oldvalue="value", newvalue="id", minY=0, minX=0):
        """
        First part inserts the boundary array into larger global mask array
        """
        
        sdbquery ="insert(redimension(apply({A}, x, x1+{xOffSet}, y, y1+{yOffSet}, {newvalue}, {oldvalue}), {B} ), {B})".format( A=tempRastName, B=destArray, yOffSet=minY, xOffSet=minX, oldvalue=oldvalue, newvalue=newvalue)
        self.sdb.query(sdbquery)
        
        

        return 

    def CreateMask(self, SciDBArray, tempArray='mask', attributeName=None, attributeType=None):
        """
        Create an empty raster "Mask "that matches the SciDBArray
        """
        import re    

        results = self.sdb.queryAFL("show(%s)" % (SciDBArray))
        results = results.decode("utf-8")

        #R = re.compile(r'\<(?P<attributes>[\S\s]*?)\>(\s*)\[(?P<dim_1>\S+)(;\s|,\s)(?P<dim_2>\S+)(\])')
        R = re.compile(r'\<(?P<attributes>[\S\s]*?)\>(\s*)\[(?P<dim_1>\S+)(;\s|,\s)(?P<dim_2>[^\]]+)')
        results = results.lstrip('results').strip()
        match = R.search(results)

        #This code creates 2d planar mask
        dim = r'([a-zA-Z0-9]+(=[0-9]:[0-9]+:[0-9]+:[0-9]+))'
        alldimensions = re.findall(dim, results)
        thedimensions = [d[0] for d in alldimensions]
        if len(thedimensions) > 2:
            dimensions = "; ".join(thedimensions[1:])
        else:
            dimensions = "; ".join(thedimensions)
        
        try:
          A = match.groupdict()
          schema = A['attributes']
          #dimensions = "[%s; %s]" % (A['dim_1'], A['dim_2'])
        except:
          print(results)
          raise 

        if not attributeName and not attributeType:
            rasterValueDataType = A['attributes']
        
        elif attributeName and not attributeType:
            rasterValueDataType = "%s:%s" % (attributeName, A['attributes'].split(":")[-1])

        elif not attributeName and attributeType:
            rasterValueDataType = "%s:%s" % (A['attributes'].split(":")[0], attributeType)
        elif attributeName and attributeType:
            rasterValueDataType = "%s:%s" % (attributeName, attributeType)

        print(rasterValueDataType)

        try:
          sdbquery = r"create array %s <%s> [%s]" % (tempArray, rasterValueDataType, dimensions)
          self.sdb.query(sdbquery)
        except:
          self.sdb.query("remove(%s)" % tempArray)
          sdbquery = r"create array %s <%s> [%s]" % (tempArray, rasterValueDataType, dimensions)
          self.sdb.query(sdbquery)

        return thedimensions












class DataTypes(object):

    def __init__(self):

        pass

    def ExistingTypes(self):
        scidbType = {
        0: 'binary',
        1: 'bool',
        2: 'char',
        3: 'datetime',
        4: 'datetimetz',
        5: 'double',
        6: 'float',
        7: 'indicator',
        8: 'int16',
        9: 'int32',
        10: 'int64',
        11: 'int8',
        12: 'string',
        3: 'uint16',
        14: 'uint32',
        15: 'uint64',
        16: 'uint8',
        17: 'void',
        }
