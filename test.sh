#!/bin/sh


#sudo killall -9 java ; rm -rf logs/* && ant jar compile.test && sudo ./bin/jexec src/test/peregrine/globalsort/TestSortViaMapReduce.java

#sudo killall -9 java ; rm -rf logs/* && ant jar compile.test && sudo ./bin/jexec src/test/peregrine/globalsort/TestSortDescendingViaMapReduce.java

sudo killall -9 java ; rm -rf logs/* && ant jar compile.test && sudo ./bin/jexec src/test/peregrine/TestPagerank.java



#./bin/fscat --render=int,int,double,string /tmp/peregrine-fs-11112/localhost/11112/0/pr/out/rank_metadata_by_indegree/chunk000000.dat |head

 ./bin/fscat --render=%10d,%10d,%15f,%10s /tmp/peregrine-fs-11112/localhost/11112/0/pr/out/rank_metadata_by_indegree/chunk000000.dat |head -100


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






./bin/fscat --render=int,int /tmp/peregrine-fs-11112/localhost/11112/0/pr/out/node_metadata_by_indegree/chunk000000.dat |head
./bin/fscat --render=int,int /tmp/peregrine-fs-11112/localhost/11112/0/pr/out/node_metadata_by_indegree/chunk000000.dat |tail

./bin/fscat --render=int,int /tmp/peregrine-fs-11113/localhost/11113/1/pr/out/node_metadata_by_indegree/chunk000000.dat |head
./bin/fscat --render=int,int /tmp/peregrine-fs-11113/localhost/11113/1/pr/out/node_metadata_by_indegree/chunk000000.dat |tail






./bin/fscat --render=long /tmp/peregrine-fs-11112/localhost/11112/0/test/peregrine.globalsort.TestSortDescendingViaMapReduce/test1.out/chunk000000.dat  |head



# ASCENDING

 sudo killall -9 java ; rm -rf logs/* && ant jar compile.test && sudo ./bin/jexec src/test/peregrine/globalsort/TestSortViaMapReduce.java 


./bin/fscat --render=long /tmp/peregrine-fs-11112/localhost/11112/0/test/peregrine.globalsort.TestSortViaMapReduce/test1.out/chunk000000.dat |head
./bin/fscat --render=long /tmp/peregrine-fs-11112/localhost/11112/0/test/peregrine.globalsort.TestSortViaMapReduce/test1.out/chunk000000.dat |tail

./bin/fscat --render=long /tmp/peregrine-fs-11113/localhost/11113/1/test/peregrine.globalsort.TestSortViaMapReduce/test1.out/chunk000000.dat |head
./bin/fscat --render=long /tmp/peregrine-fs-11113/localhost/11113/1/test/peregrine.globalsort.TestSortViaMapReduce/test1.out/chunk000000.dat |tail

# DESCENDING
sudo killall -9 java ; rm -rf logs/* && ant jar compile.test && sudo ./bin/jexec src/test/peregrine/globalsort/TestSortDescendingViaMapReduce.java 

./bin/fscat --render=long /tmp/peregrine-fs-11112/localhost/11112/0/test/peregrine.globalsort.TestSortDescendingViaMapReduce/test1.out/chunk000000.dat  |head
./bin/fscat --render=long /tmp/peregrine-fs-11112/localhost/11112/0/test/peregrine.globalsort.TestSortDescendingViaMapReduce/test1.out/chunk000000.dat  |tail

./bin/fscat --render=long /tmp/peregrine-fs-11113/localhost/11113/1/test/peregrine.globalsort.TestSortDescendingViaMapReduce/test1.out/chunk000000.dat  |head
./bin/fscat --render=long /tmp/peregrine-fs-11113/localhost/11113/1/test/peregrine.globalsort.TestSortDescendingViaMapReduce/test1.out/chunk000000.dat  |tail
