# SciDBGeo Repo
This python repo contains code for loading and analyzing geospatial (raster) data in SciDB. The repo includes scripts to
assist in loading raster data into a SciDB cluster, a script for zonal stats, and functions used for spatial analysis.

### Project Files

1. GDALtoSciDB_multiprocessing script is a python file that will load your datasets into SciDB Cluster using multiple SciDB instances. This is compatible with single instances
```
usage: GDALtoSciDB_multiprocessing.py [-h] -r RASTERPATH -a ARRAYNAME -n
                                      [ATTRIBUTES [ATTRIBUTES ...]] [-t TILES]
                                      [-c CHUNK] [-o OVERLAP]
                                      [-TempOut OUTPATH]
                                      [-SciDBLoadPath SCIDBLOADPATH]
                                      [-csv CSV] [-p PARALLEL]
```
```
optional arguments:
  -h, --help            show this help message and exit
  -r RASTERPATH         Input file path for the raster
  -a ARRAYNAME          Name of the destination array
  -n [ATTRIBUTES [ATTRIBUTES ...]]
                        Name of the attribute(s) for the destination array
  -t TILES              Size in rows of the read window, default: 8
  -c CHUNK              Chunk size for the destination array, default: 1,000
  -o OVERLAP            Chunk overlap size. Adding overlap increases data
                        loading time. default: 0
  -TempOut OUTPATH
  -SciDBLoadPath SCIDBLOADPATH
  -csv CSV              Create CSV file
  -p PARALLEL           Parallel Redimensioning
```

2. SciDB_ZonalStats_CL is a command line script for conducting zonal stats
```
usage: SciDB_ZonalStats_CL.py [-h] -SciDBArray SCIARRAY -Raster RASTER
                              -Shapefile SHAPEFILE [-Tests RUNS] -Mode MODE
                              [-CSV CSV] [-v] [-Host HOST] [-ZoneCSV ZONECSV]
                              [-Binary BINARY]
```
```
optional arguments:
  -h, --help            show this help message and exit
  -SciDBArray SCIARRAY
  -Raster RASTER
  -Shapefile SHAPEFILE
  -Tests RUNS           Number of tests you want to run
  -Mode MODE            This allows you to choose the mode of analysis you
                        want to conduct
  -CSV CSV
  -v
  -Host HOST            SciDB host for connection
  -ZoneCSV ZONECSV      CSV path for options 2 and 3
  -Binary BINARY        Binary path for tmp files for options 4 and 5

```

3. SciDB_analyis contains the functions used for performing spatial analyses in SciDB

```
usage: SciDB_analysis.py [-h] [-csv CSV]
                         {zonal,count,reclassify,focal,overlap,add} ...
```
```
optional arguments:
  -h, --help            show this help message and exit
  -csv                  Output timing results to CSV file
```
SciDB_analysis uses a config file with the following format, as an example
```
[ZonalStatistics]
folder = "/home/research/storage"

[localDatasetPrep]
chunk_sizes = [500, 1000]
raster_tables = ["glc_2000_clipped"]
pixel_values = [16]

[zonalDatasetPrep]
raster_folder = "/home/research/datasets"
shape_folder = "/home/research/datasets/4326"
chunk_sizes = [500, 1000]
array_names = ["glc_2000_clipped"]
raster_files = ["glc2000_clipped_nodata.tif"]
shape_files = ["states.shp", "regions.shp", "counties.shp"]

[main]
runs = [1]
filePath = "/home/research/files/scidb_test.csv"
rasterStatsCSVBase = "/home/research/files/zonalstats"
```
These fields have the following usages:  
ZonalStatistics:  
folder: folder to write temporary files to  

localDatasetPrep:  
chunk_sizes = list of chunk sizes to test  
raster_tables = list of raster tables to use  
pixel_values = list of pixels to use for counting  

zonalDatasetPrep:  
raster_folder = location where the raster_files are located  
shape_folder = location where the shape_files are located  
chunk_sizes = list of chunk sizes to test  
array_names =  list of array names to use  
raster_files = corresponding location of raster files  
shape_files = shape files to use  

main:  
runs = list of runs to do  
filePath = filepath to write a timings CSV to  
rasterStatsCSVBase = where to write CSVs for zonal prep, note that .csv will be written to the end, so it should not
be a full CSV path  
