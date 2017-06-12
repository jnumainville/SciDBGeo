import psycopg2, csv, timeit, argparse
from collections import OrderedDict


def WriteFile(filePath, theDictionary):
    "This function writes out the dictionary as csv"
    
    thekeys = list(theDictionary.keys())
    
    with open(filePath, 'w', newline='') as csvFile:
        #fields = list(theDictionary[thekeys[0]].keys())
        theWriter = csv.DictWriter(csvFile, fieldnames=theDictionary[thekeys[0]].keys())
        theWriter.writeheader()

        for k in theDictionary.keys():
            #theDictionary[k].update({"test": k})
            #print(theDictionary)
            theWriter.writerow(theDictionary[k])


def ZonalStats(NumberofTests, rasterName, shapefileName, tiles, filePath=None ):
    '''
    This function will submit the zonalstats query to postgresql
    '''
    
    outDictionary = OrderedDict()
    for t in range(NumberofTests):
        theTest = "test_%s" % (t+1)
        for tile in tiles:
            test_tile_id = "%s_%s" % (theTest, tile)
            query = ''' 
            SELECT p.id, p.name, (ST_SummaryStatsAgg( ST_Clip(rast, 1, p.geom, ST_BandNoDataValue(r.rast,1)) ,1, True )).*
            FROM %s p inner join %s_tilesize%s r on ST_Intersects(r.rast, p.geom)
            GROUP BY p.id, p.name''' % (shapefileName, rasterName, tile)
            
            #print(query)
            start = timeit.default_timer()
            pg_cursor.execute(query)
            stop = timeit.default_timer()
            queryTime = stop-start
            print("Timing for %s on raster %s with tile size: %s is ... %s" % (shapefileName, rasterName, tile, queryTime)) 
            outDictionary[test_tile_id] = OrderedDict( [ ("test",theTest), ("RasterTable",rasterName), ("TileSize", tile ), ("Shapefile",shapefileName), ("query_time",queryTime) ] )

    if filePath:
        #print(outDictionary)
        WriteFile(filePath, outDictionary)
            
    print("Finished")      


def argument_parser():
    """
    Parsing the command line arguments
    """
    parser = argparse.ArgumentParser(description="Conduct Zonal Stats in PostgreSQL with PostGIS 2.2 or above")   
    parser.add_argument('-Raster', required=True, dest='Raster')
    parser.add_argument('-Shapefile', required=True, dest='Shapefile')
    parser.add_argument('-host', required=True, default='localhost', dest='host')
    parser.add_argument('-database', required=True, default='research', dest='database')
    parser.add_argument('-port', required=True, type=int, default='5432', dest='port')
    parser.add_argument('-user', required=True, default='david', dest='user')
    parser.add_argument('-pass', required=True, default='hyanes', dest='pswd')
    parser.add_argument('-Tests', type=int, help="Number of tests you want to run", required=False, default=3, dest='Runs')
    parser.add_argument('-Tilesize', type=int, nargs='*', required=True, dest='tiles')
    parser.add_argument('-CSV', required=False, dest='CSV')


    #CreatePostgresql Connection
    # pg_host = 'localhost'
    # pg_database = 'david'
    # pg_user = 'david'
    # pg_word = 'haynes'
    return parser



if __name__ == '__main__':
    args = argument_parser().parse_args()
    
    conn_string = "host=%s dbname=%s user=%s password=%s" % (args.host, args.database, args.user, args.pswd)
    pg_connection = psycopg2.connect(conn_string)
    pg_cursor = pg_connection.cursor()    
    if pg_cursor:
        #print(args.tiles)
        ZonalStats(args.Runs, args.Raster, args.Shapefile, args.tiles, args.CSV)
    else:
        print("invalid connection")