#! /bin/bash

cat application.info

exec dotnet TransportController.dll "$@"
