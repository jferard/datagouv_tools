#!/bin/bash
SAVEIFS=$IFS
IFS=$(echo -en "\n\b")
mkdir -p fantoir
for source in `(ls ~/datagouv/fantoir/*.zip)`
do
    dest="fantoir/`(basename -s .zip "$source")`_reduit.zip"
    echo "$source -> $dest"
    unzip -op $source | head -n 1000 | zip > $dest
done

mkdir -p sirene
for source in `(ls ~/datagouv/sirene/*.zip)`
do
    dest="sirene/`(basename -s _utf8.zip "$source")`_reduit_utf8.zip"
    echo "$source -> $dest"
    unzip -op $source | head -n 1000 | zip > $dest
done
for source in `(ls ~/datagouv/sirene/*.csv)`
do
    dest="sirene/`(basename -s .csv "$source")`_reduit.csv"
    echo "$source -> $dest"
    head -n 1000 $source > $dest
done
IFS=$SAVEIFS