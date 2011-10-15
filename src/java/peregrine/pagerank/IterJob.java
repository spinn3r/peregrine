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
        
        protected int nr_nodes;

        /**
         * Running count of dangling rank sum so that we can build
         * teleport_grant for the next iteration.
         */
        protected double dangling_rank_sum = 0.0;

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
            byte[] nonlinked        = values[3];

            System.out.printf( "key: %s , graph_by_source: %s, rank_vector: %s, dangling=%s, nonlinked=%s\n",
                               Hex.encode( key ),
                               Hex.encode( graph_by_source ),
                               Hex.encode( rank_vector ),
                               Hex.encode( dangling ),
                               Hex.encode( nonlinked ) );

            // for the first pass, the rank_vector will be null.
            // TODO expand this in the future to support iter > 0 
            double rank = 1 / nr_nodes;

            if ( dangling != null ) {

                // this is a dangling node.  It will not have any outbound
                // links so don't attempt to index them.

                dangling_rank_sum += rank;

            } else { 
            
                HashSetValue outbound = new HashSetValue( graph_by_source );

                int outdegree = outbound.size();

                double grant = rank / outdegree;
                
                for ( byte[] target : outbound.getValues() ) {

                    byte[] value = new StructWriter()
                        .writeDouble( grant )
                        .toBytes();
                    
                    emit( target, value );

                }

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

        /**
         * 
         */
        protected double teleport_grant = 0.0;

        protected int nr_nodes;

        protected int nr_dangling = 0;

        protected int iter = 0;
        
        @Override
        public void init( JobOutput... output ) {

            super.init( output );
            
            BroadcastInput nrNodesBroadcastInput = getBroadcastInput().get( 0 );
            
            nr_nodes = new StructReader( nrNodesBroadcastInput.getValue() )
                .readVarint()
                ;

            // for iter 0 teleport_grant would be:

            if ( iter == 0 ) {

                // FIXME: add this back in later when I can read in nr_dangling, etc. 
                
                /*
                  
                double teleport_grant_sum = nr_dangling * ( 1 / nr_nodes );
                teleport_grant = (1.0 - ( DAMPENING * (1.0 - teleport_grant_sum)) ) / nr_nodes;

                */
                
            } 
            
        }
        
        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            double rank_sum = 0.0;
            
            // sum up the values... 
            for ( byte[] value : values ) {

                rank_sum += new StructReader( value )
                    .readDouble()
                    ;
                
            }

            double rank = (DAMPENING * rank_sum) + teleport_grant;

            byte[] value = new StructWriter()
                .writeDouble( rank )
                .toBytes()
                ;

            emit( key, value );
            
        }

    }

}