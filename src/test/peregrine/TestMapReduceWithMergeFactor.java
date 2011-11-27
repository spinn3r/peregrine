package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.primitive.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

public class TestMapReduceWithMergeFactor extends peregrine.BaseTestWithTwoDaemons {

    private static final Logger log = Logger.getLogger();

    public static class Map extends Mapper {

    }

    public static class Reduce extends Reducer {

    }

    public void test() throws Exception {

        // change the shuffle buffer so we have lots of smaller files.
        config.setShuffleBufferSize( 1000 );
        config0.setShuffleBufferSize( 1000 );
        config1.setShuffleBufferSize( 1000 );
        
        doTest( 25000 );
        
    }

    private void doTest( int max ) throws Exception {

        String path = String.format( "/test/%s/test1.in", getClass().getName() );
        
        ExtractWriter writer = new ExtractWriter( config, path );

        for( int i = 0; i < max; ++i ) {

            byte[] key = MD5.encode( "" + i );
            byte[] value = IntBytes.toByteArray( i );

            writer.write( key, value );
        }

        writer.close();

        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );

        try {
            
            controller.map( Map.class, path );

            int nr_shuffles = new File( "/tmp/peregrine-fs//localhost/11112/tmp/shuffle/default" ).list().length;

            System.out.printf( "FIXME: found nr_shuffles: %,d\n", nr_shuffles );
            
            controller.reduce( Reduce.class, new Input(), new Output( output ) );

        } finally {
            controller.shutdown();
        }
        
    }

    public static void main( String[] args ) throws Exception {

        //System.setProperty( "peregrine.test.config", "1:1:1" ); // 3sec

        System.setProperty( "peregrine.test.factor", "10" ); // 1m
        System.setProperty( "peregrine.test.config", "01:01:1" ); // takes 3 seconds

        // 256 partitions... 
        //System.setProperty( "peregrine.test.config", "08:01:32" );  // 1m

        runTests();

    }

}