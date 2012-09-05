#!/bin/jexec

sudo killall -9 java ; rm -rf logs/* && ant jar compile.test && sudo ./bin/jexec src/test/peregrine/globalsort/TestSortViaMapReduce.java