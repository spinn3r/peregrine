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

    public static final double DAMPENING = 0.85;
    
    // join graph_by_source, rank_vector, dangling
    
    public static class Map extends Merger {

        int nr_nodes;

        double dangling_rank_sum = 0.0;

        private JobOutput danglingRankSumBroadcast = null;

        @Override
        public void init( JobOutput... output ) {

            super.init( output );

            danglingRankSumBroadcast = output[0];
            
            BroadcastInput nrNodesBroadcastInput = getBroadcastInput().get( 0 );
            
            nr_nodes = new StructReader( nrNodesBroadcastInput.getValue() )
                .readVarint()
                ;

            System.out.printf( "Working with nr_nodes: %,d\n", nr_nodes );
            
        }

        @Override
        public void map( byte[] key,
                         byte[]... values ) {

            byte[] graph_by_source  = values[0];
            byte[] rank_vector      = values[1];
            byte[] dangling         = values[2];

            System.out.printf( "key: %s , graph_by_source: %s, rank_vector: %s, dangling=%s\n",
                               Hex.encode( key ),
                               Hex.encode( graph_by_source ),
                               Hex.encode( rank_vector ),
                               Hex.encode( dangling ) );
            
            HashSetValue outbound = new HashSetValue( graph_by_source );

            int outdegree = outbound.size();

            double rank = 1 / nr_nodes;

            double grant = rank / outdegree;
            
            for ( byte[] target : outbound.getValues() ) {

                byte[] value = new StructWriter()
                    .writeDouble( grant )
                    .toBytes();
                
                emit( target, value );

            }

            // now ... if this is a dangling node... emit it so that we can sum
            // up over the dangling nodes.
            if ( dangling != null ) {
                dangling_rank_sum += rank;
            }

        }

        @Override
        public void cleanup() {

            byte[] key = new StructWriter()
                .writeHashcode( "dangling_rank_sum" )
                .toBytes();

            byte[] value = new StructWriter()
                .writeDouble( dangling_rank_sum )
                .toBytes();

            danglingRankSumBroadcast.emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            double sum = 0.0;
            
            // sum up the values... 
            for ( byte[] value : values ) {

                sum += new StructReader( value )
                    .readDouble()
                    ;
                
            }

            byte[] value = new StructWriter()
                .writeDouble( sum )
                .toBytes()
                ;

            emit( key, value );
            
        }

    }

}