class iquery(object):

    def __init__():
        import subprocess
        import csv
        
        self.subprocess
        self.csv

    def query(self, theQuery):
        scidbArguments = ["iquery", "-an %s" % theQuery]
        p = self.subprocess.pOpen(scidbArguments, stdout=PIPE)
        print(p.communicate)

    def queryResults(theQuery):
        pass
