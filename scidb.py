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
        
        if os.pathisdir( "/".join(theCSVPath.split("/")[:-1]) ):
        
            scidbArguments = """iquery -aq "%s" -o CSV -r %s;""" % (theQuery, theCSVPath)
            #print(scidbArguments)
            p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
            p.wait()

        else:
            print("Bad CSV path: %s" % (theCSVPath))
            
        #out, err = p.communicate()
        #print("OUT", out)

        return theCSVPath


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

class statements(object):

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
            self.sdb.query("create array %s <y1:int64, x1:int64, %s:%s> [xy=0:*,?,?]" % (tempRastName, attribute_name, rasterValueDataType) )
        except:
            #Silently deleting temp arrays
            self.sdb.query("remove(%s)" % (tempRastName))
            self.sdb.query(theQuery)
            #sdb.query("create array %s <y1:int64, x1:int64, %s:%s> [xy=0:*,?,?]" % (tempRastName, attribute_name,rasterValueDataType) )    

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