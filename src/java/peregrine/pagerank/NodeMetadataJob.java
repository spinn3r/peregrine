package peregrine.pagerank;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.io.*;

public class NodeMetadataJob {

    public static class Map extends Merger {

        JobOutput nodeMetadataOutput  = null;
        JobOutput danglingOutput      = null;
        JobOutput nonlinkedOutput     = null;

        @Override
        public void init( JobOutput... output ) {
            nodeMetadataOutput  = output[0];
            danglingOutput      = output[1];
            nonlinkedOutput     = output[2];
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

            if ( indegree == 0 ) {
                
                //emit to dangling ...
                System.out.printf( "dangling\n" );

                // TODO would be NICE to support a sequence file where the
                // values are optional for better storage.
                danglingOutput.emit( key, BooleanValue.TRUE );
                
            }

            if ( outdegree == 0 ) {
                nonlinkedOutput.emit( key, BooleanValue.TRUE );
            }

            // now emit key, [indegree, outdegree]

            nodeMetadataOutput.emit( key, new Struct()
                                     .write( indegree )
                                     .write( outdegree )
                                     .toBytes() );
            
        }

    }

}