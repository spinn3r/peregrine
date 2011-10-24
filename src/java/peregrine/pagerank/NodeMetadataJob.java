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

        JobOutput nodeMetadataOutput         = null;
        JobOutput danglingOutput             = null;
        JobOutput nonlinkedOutput            = null;

        JobOutput nrNodesBroadcastOutput     = null;
        JobOutput nrDanglingBroadcastOutput  = null;

        int nrNodes = 0;
        int nrDangling = 0;
        
        @Override
        public void init( JobOutput... output ) {
            nodeMetadataOutput           = output[0];
            danglingOutput               = output[1];
            nonlinkedOutput              = output[2];
            nrNodesBroadcastOutput       = output[3];
            nrDanglingBroadcastOutput    = output[4];
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

            if ( outdegree == 0 ) {
                
                ++nrDangling;
                
                // TODO would be NICE to support a sequence file where the
                // values are optional for better storage.
                danglingOutput.emit( key, BooleanValue.TRUE );
                
            }

            if ( indegree == 0 ) {
                nonlinkedOutput.emit( key, BooleanValue.TRUE );
            }

            // now emit key, [indegree, outdegree]

            nodeMetadataOutput.emit( key, new Struct()
                                     .write( indegree )
                                     .write( outdegree )
                                     .toBytes() );

            ++nrNodes;
            
        }

        @Override
        public void cleanup() {

            if ( nrNodes == 0 )
                throw new RuntimeException();

            // *** broadcast nr dangling.

            byte[] key = Hashcode.getHashcode( "id" );
            
            nrNodesBroadcastOutput.emit( key, IntBytes.toByteArray( nrNodes ) );

            // *** broadcast nr dangling.
            nrDanglingBroadcastOutput.emit( key, IntBytes.toByteArray( nrDangling ) );

        }

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            int count = 0;
            
            for( byte[] val : values ) {
                count += IntBytes.toInt( val );
            }

            emit( key, IntBytes.toByteArray( count ) );

        }

    }

}