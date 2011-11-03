package peregrine.pagerank;

import java.util.*;
import peregrine.*;
import peregrine.values.*;
import peregrine.io.*;

public class TeleportationGrantJob {

    public static class Reduce extends Reducer {

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
        public void reduce( byte[] key, List<byte[]> values ) {

            double sum = 0.0;
            
            // sum up the values... 
            for ( byte[] value : values ) {

                sum += new StructReader( value )
                    .readDouble()
                    ;
                
            }

            double result = (1.0 - (IterJob.DAMPENING * (1.0 - sum))) / nr_nodes;

            byte[] value = new StructWriter()
                .writeDouble( result )
                .toBytes();
            
            emit( key, value );
            
        }

    }

}