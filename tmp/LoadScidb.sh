#!/bin/bash
#Script for batch loading data into SciDB

while getopts a:r: option
do
    case "${option}"
    in
    r) raster=${OPTARG};;
    a) array=${OPTARG};;
    esac
done



      
#python3 GDALtoSciDB_multiprocessing.py -Host NoSHIM -RasterPath ../ESACCI_300m_2010.tif -SciDBArray meris_1000 -AttributeNames value -Tiles 8 -Chunk 1000 -TempOut /mnt -SciDBLoadPath /data/04489/dhaynes

for i in 500,4000 1000,3500 1500,3000 2000,25000 2500,2000 3000,15000 3500,1000 4000,500; do
    IFS="," read chunk tiles <<< "${i}"
    #echo "${chunk}" and "${tiles}"
    args=("-SciDBLoadPath /data/04489/dhaynes" "-TempOut /mnt" "-AttributeNames value" "-Tiles ${tiles}" "-Chunk ${chunk}" "-RasterPath ${raster}" "-SciDBArray ${array}_${chunk}" "-Host NoSHIM")
    #LoadArray="python3 GDALtoSciDB_multiprocessing.py 
    python3 GDALtoSciDB_multiprocessing.py ${args[@]}
    echo ${args[@]}
done
