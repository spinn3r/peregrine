package maprunner.pagerank;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class NodeMetadataJob {

    public static class Map extends Merger {

        @Override
        public void init() {

            //TODO: setup output streams ... node_metadata, dangling , and nonlinking
            
        }
        
        @Override
        public void map( byte[] key,
                         byte[]... values ) {

            // left should be node_indegree , right should be the graph... 

            int indegree  = 0;
            int outdegree = 0;
            
            if ( values[0] != null ) {
                indegree = new IntValue( values[0] ).value;
            }

            if ( values[1] != null ) {

                HashSetValue set = new HashSetValue();
                set.fromBytes( values[1] );

                outdegree = set.size();

            }

            // now emit key, [indegree, outdegree]

            ByteArrayListValue result = new ByteArrayListValue();
            result.addValue( new IntValue( indegree ) );
            result.addValue( new IntValue( outdegree ) );
            
            emit( key, result.toBytes() );
            
        }

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            if ( values.size() != 1 )
                throw new RuntimeException( "Too many values.  Error in computation: " + values.size() );
            
            //FIXME: ok this is god damn retarded.

            //ListPackedValue
            ByteArrayListValue list = new ByteArrayListValue( values.get( 0 ) );

            List<byte[]> split = list.getValues();
            
            int indegree  = new IntValue( split.get( 0 ) ).value;
            int outdegree = new IntValue( split.get( 1 ) ).value;

            System.out.printf( "indegree=%,d outdegree=%,d\n", indegree, outdegree );

            // NOTE that since this is being done in the reduce phase we don't
            // need to actually sort the output so we can write to local files
            // directly (sweet). We still have to use PartitionWriter though
            // because even though the files are able to be written to that
            // partition they need to be replicated.  I also need to find a way
            // to swap them in once the task is correctly executed.
            
            if ( indegree == 0 ) {
                //emit to dangling ...
                System.out.printf( "dangling\n" );
            }

            if ( outdegree == 0 ) {
                //emit to nonlinking ...
                System.out.printf( "nonlinking\n" );
            }

            //emit to node_metadata
            
            //FIXME: this is retarded tooo..... 
            emit( key, values.get( 0 ) );
            
        }
        
    }

}