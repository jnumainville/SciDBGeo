class iquery(object):

    def __init__(self,):
        import subprocess
        import csv
        
        self.subprocess = subprocess
        self.PIPE = subprocess.PIPE
        self.csv = csv

    def query(self, theQuery):
        scidbArguments = """iquery -anq "%s";""" % (theQuery)
        print(scidbArguments)
        p = self.subprocess.Popen(scidbArguments, shell=True)
        p.wait()
        #print(p.communicate)
        del p

        return

    def queryResults(self, theQuery, thePath):
        import os
        
        with open(thePath, 'w'):
            os.chmod(thePath, 0o777)

        scidbArguments = ["iquery"]
        scidbArguments.append( "-aq %s" % (theQuery))
        scidbArguments.append( "-o CSV")
        scidbArguments.append( "-r %s" % (thePath))
        
        print(scidbArguments)
        p = self.subprocess.Popen(scidbArguments, shell=True)
        #print(p.communicate)
        del p
        return 

    def queryAFL(self, theQuery):
        scidbArguments = """iquery -aq "%s";""" % (theQuery)
        #print(scidbArguments)
        p = self.subprocess.Popen(scidbArguments, stdout=self.subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()
        #print("OUT", out)

        return out
