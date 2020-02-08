class iquery(object):

    def __init__(self, ):
        """
        Initialize iquery class

        Input:
            None

        Output:
            An instance of the iquery class
        """

        import subprocess
        import csv
        self.subprocess = subprocess
        self.PIPE = subprocess.PIPE
        self.csv = csv

    def query(self, theQuery):
        """
        Run a query, returning the results

        Input:
            theQuery = The desired query to run

        Output:
            The result of the query
        """
        scidbArguments = """iquery -anq "%s";""" % theQuery
        print(scidbArguments)

        # Open up subprocess for query
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, stderr=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()

        if err:
            print("Error: %s" % err)
            raise
        del p
        return out

    def queryResults(self, theQuery, thePath):
        """
        Output the results of a query to a given file

        Input:
            theQuery = The query to run
            thePath = The path to write the results to

        Output:
            None
        """
        import csv

        scidbArguments = """iquery -aq "%s;" -o CSV -r %s""" % (theQuery, thePath)

        # Open up subprocess for query
        p = self.subprocess.Popen(scidbArguments, shell=True)
        p.wait()
        with open(thePath) as infile:
            data = csv.reader(infile)
            print("geoid, min, max, average, count")
            for d in data:
                print(d)
        del p

    def queryAFL(self, theQuery):
        """
        Run a query in SciDB AFL

        Input:
            theQuery = The desired query to run

        Output:
            The result of the query
        """

        # Open up subprocess for query
        scidbArguments = """iquery -aq "%s";""" % theQuery
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()

        return out

    def queryCSV(self, theQuery, theCSVPath):
        """
        Output the results of a query to a given CSV file

        Input:
            theQuery = The query to run
            theCSVPath = The path to write the CSV results to

        Output:
            theCSVPath, as originally given
        """

        # Open up subprocess for query
        scidbArguments = """iquery -aq "%s" -o csv+ -r %s;""" % (theQuery, theCSVPath)
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()

        return theCSVPath

    def aql_query(self, theQuery):
        """
        Run a query in SciDB AQL

        Input:
            theQuery = The desired query to run

        Output:
            The result of the query
        """

        # Open up subprocess for query
        scidbArguments = """iquery -q "%s";""" % theQuery
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()

        return out

    def versions(self, theArray):
        """
        Get the versions from SciDB

        Input:
            theArray = The array to run versions on

        Output:
            The versions
        """

        scidbArguments = """iquery -aq "versions(%s)"; """ % theArray

        # Open up subprocess for query
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()

        resultsList = out.decode("utf-8").split("\n")
        versions = []
        try:
            for r in resultsList[1:]:
                if len(r) > 1:
                    # Append on pulled versions
                    positionversion, datetime = r.split(",")
                    position, version = positionversion.split(" ")
                    versions.append(version)
        except:
            print(resultsList)

        return versions

    def list(self, item='arrays'):
        """
        List the desired objects

        Input:
            item = The item to list

        Output:
            The names of the listed items
        """

        scidbArguments = """iquery -aq "list('%s')"; """ % item

        # Open up subprocess for query
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()

        resultsList = out.decode("utf-8").split("\n")
        arrayNames = []

        try:
            for r in resultsList[1:]:
                if len(r) > 1:
                    # Append on names from results
                    positionArrayName, uaid, aid, schema, availability, temporary = r.split(",")
                    position, name = positionArrayName.split(" ")
                    name = name.replace("'", "")
                    arrayNames.append(name)
        except:
            print(resultsList)

        return arrayNames

    def OutputToArray(self, filePath, valueColumn, yColumn=1):
        """
        The CSV array, must output the y and x coordinate values.

        Input:
            filePath = The path to read from
            valueColumn = The column with the desired values to read
            yColumn = The column with the desired y values

        Output:
            A reshaped array
        """
        import numpy as np

        with open(filePath, 'r') as filein:
            dataset = np.loadtxt(filein, delimiter=',', dtype=np.float)

        # Load in relevant array data
        ycoordinates = dataset[:, yColumn]
        unique_y, number_of_y = np.unique(ycoordinates, return_counts=True)
        height = len(unique_y)
        width = number_of_y[0]
        valuearray = dataset[:, valueColumn]

        return valuearray.reshape((height, width))

    def WriteRaster(self, inArray, inGeoTiff, outPath, noDataValue=-999):
        """
        Write a raster to a given file

        Input:
            inArray = The raster to write
            inGeoTiff = GeoTIFF of the raster
            outPath = Where to write the raster to
            noDataValue = What to replace missing data with

        Output:
            None
        """

        from osgeo import ogr, gdal

        driver = gdal.GetDriverByName('GTiff')

        height, width = inArray.shape
        r = gdal.Open(inGeoTiff)

        # Write the array
        if driver:
            geoTiff = driver.Create(outPath, width, height, 1, 6)
            geoTiff.SetGeoTransform(r.GetGeoTransform())
            geoTiff.SetProjection(r.GetProjection())
            band = geoTiff.GetRasterBand(1)
            band.SetNoDataValue(noDataValue)

            band.WriteArray(inArray)
            geoTiff.FlushCache()

        del geoTiff


class Statements(object):

    def __init__(self, sdb):
        """
        Must supply the sdb connection for initialization

        Input:
            sdb = Connection to an sdb instance

        Output:
            An instance of the Statements class
        """
        self.sdb = sdb

    def CreateLoadArray(self, tempRastName, attribute_name, rasterArrayType):
        """
        Create the loading array

        Input:
            tempRastName = The name of the array to create
            attribute_name = The name of the attribute represented
            rasterArrayType = The type of the raster

        Output:
            None
        """

        # Build creation query, depending on type
        theQuery = None
        if rasterArrayType <= 2:
            theQuery = "create array %s <y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (tempRastName, attribute_name)
        elif rasterArrayType == 3:
            theQuery = "create array %s <z1:int64, y1:int64, x1:int64, %s> [xy=0:*,?,?]" % (
                tempRastName, attribute_name)

        # Attempt to create load array, deleting on exception
        try:
            self.sdb.query(theQuery)
        except:
            # Silently deleting temp arrays
            self.sdb.query("remove(%s)" % tempRastName)
            self.sdb.query(theQuery)

    def LoadOneDimensionalArray(self, sdb_instance, tempRastName, rasterAttributes, rasterType, binaryLoadPath):
        """
        Function for loading GDAL data into a single dimension

        Input:
            sdb_instance = The instance to load the data into
            tempRastName = The name of the raster to write to
            rasterAttributes = The attributes for the given raster
            rasterType = The type of the given raster
            binaryLoadPath = The path to load in from

        Output:
            1 for success, 0 for failure
        """

        # Retrieve attribute value types
        query = None
        if rasterType == 2:
            items = [attribute.split(":")[1].strip() for attribute in rasterAttributes.split(",")]
            attributeValueTypes = ", ".join(items)
        else:
            attributeValueTypes = rasterAttributes.split(":")[1]

        # Attempt to load array
        try:
            query = "load(%s, '%s' ,%s, '(int64, int64, %s)') " % (
                tempRastName, binaryLoadPath, sdb_instance, attributeValueTypes)
            self.sdb.query(query)
            return 1
        except:
            print("Error Loading DimensionalArray")
            print(query)
            return 0

    def InsertRedimension(self, tempRastName, destArray, oldvalue="value", newvalue="id", minY=0, minX=0):
        """
        Inserts the boundary array into larger global mask array

        Input:
            tempRastName = The raster to pull from
            destArray = Where to insert the data
            oldvalue = The value in the old array
            newvalue = The value in the new array
            minY = Where to start on Y dimension
            minX = Where to start on X dimension

        Output:
            None
        """

        sdbquery = "insert(redimension(apply({A}, x, x1+{xOffSet}, y, y1+{yOffSet}, {newvalue}, {oldvalue}), {B} ), " \
                   "{B})".format(A=tempRastName, B=destArray, yOffSet=minY, xOffSet=minX, oldvalue=oldvalue,
                                 newvalue=newvalue)
        self.sdb.query(sdbquery)

        return

    def CreateMask(self, SciDBArray, tempArray='mask', attributeName=None, attributeType=None):
        """
        Create an empty raster "Mask" that matches the SciDBArray

        Input:
            SciDBArray = The SciDBArray to match
            tempArray = The array to create
            attributeName = The names of the attributes
            attributeType = The types of the attributes

        Output:
            The resulting dimensions
        """
        import re

        results = self.sdb.queryAFL("show(%s)" % SciDBArray)
        results = results.decode("utf-8")

        # Clean and search through results
        R = re.compile(r'\<(?P<attributes>[\S\s]*?)\>(\s*)\[(?P<dim_1>\S+)(;\s|,\s)(?P<dim_2>[^\]]+)')
        results = results.lstrip('results').strip()
        match = R.search(results)

        # This code creates 2d planar mask
        dim = r'([a-zA-Z0-9]+(=[0-9]:[0-9]+:[0-9]+:[0-9]+))'
        alldimensions = re.findall(dim, results)
        thedimensions = [d[0] for d in alldimensions]
        if len(thedimensions) > 2:
            dimensions = "; ".join(thedimensions[1:])
        else:
            dimensions = "; ".join(thedimensions)

        try:
            A = match.groupdict()
        except:
            print(results)
            raise

        rasterValueDataType = None
        if not attributeName and not attributeType:
            rasterValueDataType = A['attributes']

        elif attributeName and not attributeType:
            rasterValueDataType = "%s:%s" % (attributeName, A['attributes'].split(":")[-1])

        elif not attributeName and attributeType:
            rasterValueDataType = "%s:%s" % (A['attributes'].split(":")[0], attributeType)
        elif attributeName and attributeType:
            rasterValueDataType = "%s:%s" % (attributeName, attributeType)

        print(rasterValueDataType)

        # Create the array, removing it on error
        try:
            sdbquery = r"create array %s <%s> [%s]" % (tempArray, rasterValueDataType, dimensions)
            self.sdb.query(sdbquery)
        except:
            self.sdb.query("remove(%s)" % tempArray)
            sdbquery = r"create array %s <%s> [%s]" % (tempArray, rasterValueDataType, dimensions)
            self.sdb.query(sdbquery)

        return thedimensions


# TODO: determine if necessary
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