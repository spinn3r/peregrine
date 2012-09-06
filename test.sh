#!/bin/sh


#sudo killall -9 java ; rm -rf logs/* && ant jar compile.test && sudo ./bin/jexec src/test/peregrine/globalsort/TestSortViaMapReduce.java

sudo killall -9 java ; rm -rf logs/* && ant jar compile.test && sudo ./bin/jexec src/test/peregrine/TestPagerank.java


./bin/fscat --render=int,int /tmp/peregrine-fs-11112/localhost/11112/0/pr/out/node_metadata_by_indegree/chunk000000.dat |head
./bin/fscat --render=int,int /tmp/peregrine-fs-11112/localhost/11112/0/pr/out/node_metadata_by_indegree/chunk000000.dat |tail

./bin/fscat --render=int,int /tmp/peregrine-fs-11113/localhost/11113/1/pr/out/node_metadata_by_indegree/chunk000000.dat |head
./bin/fscat --render=int,int /tmp/peregrine-fs-11113/localhost/11113/1/pr/out/node_metadata_by_indegree/chunk000000.dat |tail

./bin/fscat --render=int,int /tmp/peregrine-fs-11114/localhost/11114/2/pr/out/node_metadata_by_indegree/chunk000000.dat |head
./bin/fscat --render=int,int /tmp/peregrine-fs-11114/localhost/11114/2/pr/out/node_metadata_by_indegree/chunk000000.dat |tail

./bin/fscat --render=int,int /tmp/peregrine-fs-11115/localhost/11115/3/pr/out/node_metadata_by_indegree/chunk000000.dat |head
./bin/fscat --render=int,int /tmp/peregrine-fs-11115/localhost/11115/3/pr/out/node_metadata_by_indegree/chunk000000.dat |tail

0: 197-289
1: 435-205
2: 466-437
3: 488-474