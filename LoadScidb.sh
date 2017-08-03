#!/bin/bash
#Script for batch loading data into SciDB

while getopts a:r:i: option
do
    case "${option}"
    in
    i) instances=${OPTARG};;
    r) raster=${OPTARG};;
    a) array=${OPTARG};;
    esac
done



      
#python3 GDALtoSciDB_multiprocessing.py -Instances 10 11 12 13 14 15 16 17 -Host http://iuwrang-xfer2.uits.indiana.edu:8080 -RasterPath /home/04489/dhaynes/ESACCI_300m_2010.tif -ScidbArray meris_2000 -Tiles 4 -Chunk 2000 -AttributeNames value -TempOut /mnt -SciDBLoadPath /data/04489/dhaynes
#
for chunk in 500 1000 1500 2000 2500 3000 3500 4000; do
    args=("-Instances ${instances}" "-SciDBLoadPath /data/04489/dhaynes" "-TempOut /mnt" "-AttributeNames value" "-Tiles ${chunk}" "-RasterPath ${raster}" "-ScidbArray ${array}_${chunk}" "-Host http://localhost:8080")
    #LoadArray="python3 GDALtoSciDB_multiprocessing.py 
    python3 GDALtoSciDB_multiprocessing.py ${args[@]}
    #echo ${args[@]}
done
