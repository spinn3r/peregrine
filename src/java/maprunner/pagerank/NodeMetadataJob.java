package maprunner.pagerank;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class NodeMetadataJob {

    public static class Map extends Mapper {

        public void map( byte[] key_data,
                         byte[] value_data ) {

            // the node_indegree table should be written at this point and we
            // will need to join against it and then emit
            //
            // key, indegree, outdegree
            //
            // the reducer will need to also write some values ot the local disk
            // including a value to the 'dangling' graph and 'nonlinked_nodes'
            // using PartitionWriter so that we have data on ALL partitions and
            // it is backed up.
            
            HashSetValue value = new HashSetValue();
            value.fromBytes( value_data );
            
            byte[] source = key_data;

            for( byte[] target : value.getValues() ) {
                emit( target, source );
            }
            
        }

    }

    public static class Reduce extends Reducer {
        
        public void reduce( byte[] key, List<byte[]> values ) {

            int indegree = values.size();
            emit( key, new IntValue( indegree ).toBytes() );
            
        }
        
    }

}