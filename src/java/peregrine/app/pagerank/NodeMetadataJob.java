package peregrine.app.pagerank;

import java.util.*;
import peregrine.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.util.primitive.IntBytes;
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
        public void merge( StructReader key,
                           List<StructReader> values ) {

            // left should be node_indegree , right should be the graph... 

            int indegree  = 0;
            int outdegree = 0;
            
            if ( values.get(0) != null ) {
                indegree = values.get(0).readInt();
            }

            if ( values.get(1) != null ) {

                HashSetValue set = new HashSetValue();
                set.fromChannelBuffer( values.get(1).getChannelBuffer() );

                outdegree = set.size();

            }

            if ( outdegree == 0 ) {
                
                ++nrDangling;
                
                // TODO would be NICE to support a sequence file where the
                // values are optional for better storage.
                danglingOutput.emit( key, StructReaders.TRUE );
                
            }

            if ( indegree == 0 ) {
                nonlinkedOutput.emit( key, StructReaders.TRUE );
            }

            nodeMetadataOutput.emit( key, new StructWriter()
                                             .writeInt( indegree )
                                             .writeInt( outdegree )
                                             .toStructReader() );

            ++nrNodes;
            
        }

        @Override
        public void cleanup() {

            if ( nrNodes == 0 )
                throw new RuntimeException();

            // *** broadcast nr dangling.

            StructReader key = StructReaders.hashcode( "id" );
            
            nrNodesBroadcastOutput.emit( key, StructReaders.wrap( nrNodes ) );

            // *** broadcast nr dangling.
            nrDanglingBroadcastOutput.emit( key, StructReaders.wrap( nrDangling ) );

        }

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            int count = 0;
            
            for( StructReader val : values ) {
                count += val.readInt();
            }

            emit( key, StructReaders.wrap( count ) );

        }

    }

}