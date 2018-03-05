import csv
import scidb
sdb = scidb.iquery()

# {No} name,uaid,aid,schema,availability,temporary
# {0} 'glc_1000',7227,7227,'glc_1000<value:uint8> [y=0:16352:0:1000; x=0:40319:0:1000]',true,false
# {1} 'glc_1500',7534,7534,'glc_1500<value:uint8> [y=0:16352:0:1500; x=0:40319:0:1500]',true,false
# {2} 'glc_2000',7667,7667,'glc_2000<value:uint8> [y=0:16352:0:2000; x=0:40319:0:2000]',true,false
# {3} 'glc_2500',7749,7749,'glc_2500<value:uint8> [y=0:16352:0:2500; x=0:40319:0:2500]',true,false

filePath = '/home/04489/dhaynes/temparrays.csv'
with open(filePath, 'r') as fin:
    theReader = csv.reader(fin, delimiter=',')
    for row in theReader:
        #id, name = row[0].split(" ")
        #name = name.replace("'", "")
        print(row[0].replace("'", ""))
        arrayname = row[0].replace("'", "") 
        if 'temp' in arrayname: 
            sdb.query("remove(%s)" % (arrayname) )
       
