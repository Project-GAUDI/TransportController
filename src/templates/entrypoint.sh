#! /bin/sh

cat application.info

exec dotnet TransportController.dll "$@"
