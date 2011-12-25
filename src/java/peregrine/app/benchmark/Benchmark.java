package peregrine.app.benchmark;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.values.*;

import com.spinn3r.log5j.*;

public class Benchmark {

    private static final Logger log = Logger.getLogger();

    public static class Map extends Mapper {

        public static boolean EMIT = true;
        
        @Override
        public void map( StructReader key,
                         StructReader value ) {

            if ( EMIT )
                emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        AtomicInteger count = new AtomicInteger();
        
        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            List<Integer> ints = new ArrayList();

            // decode these so we know what they actually mean.
            for( StructReader val : values ) {
                ints.add( val.readInt() );
            }

            count.getAndIncrement();

        }

        @Override
        public void cleanup() {

            if ( count.get() == 0 )
               throw new RuntimeException( "count is zero" );
            
        }

    }

    private Config config;
    
    public Benchmark( Config config ) {
        this.config = config;
    }

}