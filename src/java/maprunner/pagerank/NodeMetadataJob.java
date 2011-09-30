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
            
            if ( indegree == 0 ) {
                //emit to dangling ...
                System.out.printf( "dangling\n" );
            }

            if ( outdegree == 0 ) {
                //emit to nonlinking ...
                System.out.printf( "nonlinking\n" );
            }

            //FIXME: this is retarded tooo..... 
            emit( key, values.get( 0 ) );
            
        }
        
    }

}