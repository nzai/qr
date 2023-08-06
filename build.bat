@echo off

echo build qr
SET CGO_ENABLED=0
SET GOOS=linux
SET GOARCH=amd64
go build -o qr

echo ok