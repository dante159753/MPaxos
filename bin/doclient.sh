#!/bin/sh
./clean.sh
./client -id $ID
grep -v "^\[" client.*; grep Through client.*

