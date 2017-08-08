# scidb_analysis
Code for analysing big raster datasets in SciDB

1) GDALtoSciDB_multiprocessing script is a python file that will load your datasets into SciDB Clsuter using multiple SciDB instances. This is compatible with single instances

usage: GDALtoSciDB_multiprocessing.py [-h] -Instances
                                      [INSTANCES [INSTANCES ...]] -Host HOST
                                      -RasterPath RASTERPATH -SciDBArray
                                      RASTERNAME -AttributeNames ATTRIBUTES
                                      [-Tiles TILES] [-Chunk CHUNK]
                                      [-Overlap OVERLAP] [-TempOut OUTPATH]
                                      [-SciDBLoadPath SCIDBLOADPATH]
                                      [-CSV CSV]

multiprocessing module for loading GDAL read data into SciDB with multiple
instances

optional arguments:
  -h, --help            show this help message and exit
  -Instances [INSTANCES [INSTANCES ...]]
                        Number of SciDB Instances for parallel data loading
  -Host HOST            SciDB host for connection
  -RasterPath RASTERPATH
                        Input file path for the raster
  -SciDBArray RASTERNAME
                        Name of the destination array
  -AttributeNames ATTRIBUTES
                        Name of the destination array
  -Tiles TILES          Size in rows of the read window, default: 1
  -Chunk CHUNK          Chunk size for the destination array, default: 1,000
  -Overlap OVERLAP      Chunk overlap size. Adding overlap increases data
                        loading time. defalt: 0
  -TempOut OUTPATH
  -SciDBLoadPath SCIDBLOADPATH
  -CSV CSV              Create CSV file
(geospatial) scidb@scidb-vm:/media/sf_scid

2) SciDB_ZonalStats_CL is a command line script for conducting zonal stats

usage: SciDB_ZonalStats_CL.py [-h] -SciDBArray SCIARRAY -Raster RASTER
                              -Shapefile SHAPEFILE [-Tests RUNS] -Mode MODE
                              [-CSV CSV] [-v] [-Host HOST]

Conduct SciDB Zonal Stats

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


3) GDAltoSciDB script is a single core implementation of the script (outdated)
