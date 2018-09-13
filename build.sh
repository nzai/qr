#!/bin/sh

echo build list
cd cmd/list
go build -o list && zip -m ../../list.zip list

#echo build crawl
#cd ../crawl
#go build -o crawl && zip -m ../crawl.zip crawl

cd ../..
echo end