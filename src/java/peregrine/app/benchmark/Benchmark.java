package peregrine.app.benchmark;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.primitive.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

public class Benchmark {

    private static final Logger log = Logger.getLogger();

    public static class Map extends Mapper {

        @Override
        public void map( byte[] key,
                         byte[] value ) {

            emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        AtomicInteger count = new AtomicInteger();
        
        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            List<Integer> ints = new ArrayList();

            // decode these so we know what they actually mean.
            for( byte[] val : values ) {
                ints.add( IntBytes.toInt( val ) );
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
    
    public void exec( String in, String out ) throws Exception {

        Controller controller = new Controller( config );

        try {

            controller.map( Map.class, in );

            // make sure the shuffle output worked
            
            controller.reduce( Reduce.class, new Input(), new Output( out ) );

        } finally {
            controller.shutdown();
        }

    }
    
}