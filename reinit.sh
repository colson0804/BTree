#!/bin/bash 
./deletedisk mydisk
./makedisk mydisk 1024 1024 1 16 64 100 10 .28
./btree_init mydisk 100 4 4