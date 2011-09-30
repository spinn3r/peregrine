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

    // not much to do here... identity is fine.
    public static class Reduce extends Reducer { }

}