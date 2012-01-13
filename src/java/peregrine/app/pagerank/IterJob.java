/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.app.pagerank;

import java.util.*;
import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
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
        public void init( List<JobOutput> output ) {

            super.init( output );
            
            danglingRankSumBroadcast = output.get(1);
            
            BroadcastInput nrNodesBroadcastInput = getBroadcastInput().get( 0 );
            
            nr_nodes = nrNodesBroadcastInput.getValue().readInt();

            System.out.printf( "Working with nr_nodes: %,d\n", nr_nodes );
            
        }

        @Override
        public void merge( StructReader key,
                           List<StructReader> values ) {

        	StructReader outbound         = values.get( 0 );
        	StructReader dangling         = values.get( 2 );

            // for the first pass, the rank_vector will be null.
            // TODO expand this in the future to support iter > 0 
            double rank = 1 / nr_nodes;

            if ( dangling != null ) {

                // this is a dangling node.  It will not have any outbound
                // links so don't attempt to index them.

                dangling_rank_sum += rank;

            } else { 
            
                int outdegree = outbound.length() / Hashcode.HASH_WIDTH;

                double grant = rank / outdegree;
                
                while ( outbound.isReadable() ) {

                    StructReader target = outbound.readSlice( Hashcode.HASH_WIDTH );
                    
                    StructReader value = new StructWriter()
                    	.writeDouble( grant )
                    	.toStructReader();
                    
                    emit( target, value );

                }

            }
                
        }

        @Override
        public void cleanup() {

            StructReader key = StructReaders.hashcode( "id" );
            StructReader value = StructReaders.wrap( dangling_rank_sum );

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
        public void init( List<JobOutput> output ) {

            super.init( output );
            
            nr_nodes = getBroadcastInput()
                           .get( 0 )
                           .getValue()
                           .readInt()
                ;

            nr_dangling = getBroadcastInput()
                           .get( 1 )
                           .getValue()
                           .readInt()
                ;

            // for iter 0 teleport_grant would be:

            if ( iter == 0 ) {

                double teleport_grant_sum = nr_dangling * ( 1 / nr_nodes );
                teleport_grant = (1.0 - ( DAMPENING * (1.0 - teleport_grant_sum)) ) / nr_nodes;
                
            } 
            
        }
        
        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            double rank_sum = 0.0;
            
            // sum up the values... 
            for ( StructReader value : values ) {

                rank_sum += value.readDouble()
                    ;
                
            }

            double rank = (DAMPENING * rank_sum) + teleport_grant;

            /*
            byte[] value = new StructWriter()
                .writeDouble( rank )
                .toBytes()
                ;
            */
                            
            emit( key, StructReaders.wrap( rank ) );
            
        }

    }

}
