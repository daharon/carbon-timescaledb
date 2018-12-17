#!/bin/bash

DIR='/Users/daharon/Temp/storage/graphite/whisper'
HOST='127.0.0.1'
PORT=2003

for f in `find "${DIR}" -type f -name "*.wsp" -print`
do
    PREFIX=`echo "${f}" \
        | cut -d'/' -f8- \
        | sed 's#/#.#g' \
        | sed 's/\.wsp//g'`
    whisper-fetch.py --from 1513468800 --drop=nulls "${f}" \
        | awk -v prefix="${PREFIX}" '{print prefix " " $2 " " $1}'
done
#done | nc "${HOST}" "${PORT}"
