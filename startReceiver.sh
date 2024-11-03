#!/bin/bash
sudo rtl_433 -vv -f 868000000 -F json | python3 ./weathaStationReceiver.py
