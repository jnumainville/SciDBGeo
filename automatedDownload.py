#The script will automate data downloading
#import urllib  Python 2
from urllib import request

#Input Parameters here
fileDir = r'/data/04489/dhaynes/treecover'
webaddress = r'http://commondatastorage.googleapis.com/earthenginepartners-hansen/GFC2013/treecover2000.txt'


#Use urllib to open the url and read it as text string
webpg = request.urlopen(webaddress)
data = webpg.read().decode()

#Split the text string on the new line character
tiles = data.split('\n')
print(len(tiles))

with open(r'/data/04489/dhaynes/treecover/treecover_files.txt','w') as outfile:
    for t, tile in enumerate(tiles):
        tilename = tile.split('/')[-1]
        fileName = "%s/%s" % (fileDir, tilename)
        print(fileName)
        request.urlretrieve(tile, fileName)
        #Remove this after it works
        #if t == 10: break
        outfile.write(fileName)
