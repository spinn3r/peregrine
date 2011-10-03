package peregrine.pagerank;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.io.*;

public class IterJob {

    // join graph_by_source, rank_vector, dangling
    
    public static class Map extends Merger {

        int nr_nodes;
        
        @Override
        public void init( JobOutput... output ) {

            super.init( output );
            
            BroadcastInput nrNodesBroadcastInput = getBroadcastInput().get( 0 );
            
            nr_nodes = new StructReader( nrNodesBroadcastInput.getValue() )
                .readVarint()
                ;

            System.out.printf( "Working with nr_nodes: %,d\n", nr_nodes );
            
        }

        @Override
        public void map( byte[] key,
                         byte[]... values ) {

            byte[] graph_by_source = values[0];
            byte[] rank_vector = values[1];

            System.out.printf( "key: %s , graph_by_source: %s, rank_vector: %s\n",
                               Hex.encode( key ), Hex.encode( graph_by_source ), Hex.encode( rank_vector ) );
            
            HashSetValue outbound = new HashSetValue( graph_by_source );

            int outdegree = outbound.size();

            double rank = 1 / nr_nodes;

            double grant = rank / outdegree;
            
            for ( byte[] target : outbound.getValues() ) {

                byte[] value = new StructWriter()
                    .write( grant )
                    .toBytes();
                
                emit( target, value );

            }
            
            /*
            if ( graph_by_source == null ) {
                System.out.printf( "X" );
                return;
            } else {
                System.out.printf( "V\n" );
            }
            
            double rank = 0.0;

            */
            
            // read the graph targets and for each one emit target rank_vector::rank / graph_by_source::outdegree ... 
            
        }

    }

}