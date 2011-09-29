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

            /*

              int outdegree = count( value_data );

              Tuple indegree_tuple = merge( "/pr/tmp/node_indegree", key_data );

              int indegree = 0;
              
              if ( indegree_tuple != null )
                  indegree = indegree_tuple.value;
              
              emit( key, {indegree, outdegree} );

             */

        }

    }

    public static class Reduce extends Reducer {
        
        public void reduce( byte[] key, List<byte[]> values ) {

            //emit( key, values );
            
        }
        
    }

}