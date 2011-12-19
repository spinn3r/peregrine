package peregrine;

import java.util.*;

import peregrine.controller.*;
import peregrine.io.*;
import peregrine.util.primitive.IntBytes;
import peregrine.values.*;
import peregrine.config.*;
import peregrine.io.partition.*;
import peregrine.task.*;

public class TestNewReduceCode extends peregrine.BaseTestWithMultipleConfigs {

    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
        		         StructReader value ) {

            emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        int count = 0;

        int nth = 0;
        
        @Override
        public void reduce( StructReader key, List<StructReader> values ) {
            
            ++count;

            List<Integer> ints = new ArrayList();

            for( StructReader val : values ) {
                ints.add( val.readInt() );
            }
            
            // full of fail... 
            if ( values.size() != 2 )
                throw new RuntimeException( String.format( "%s does not equal %s (%s) on nth reduce %s" ,
                                                           values.size(), 2, ints, nth ) );

            ++nth;
            
        }

        @Override
        public void cleanup() {

            if ( count == 0 )
                throw new RuntimeException( "no results reduced.... " );
            
        }

    }

    public void setUp() {
        
        super.setUp();

        for ( Config config : configs ) {
            config.setPurgeShuffleData( false );
        }

    }

    @Override
    public void doTest() throws Exception {

        String path = String.format( "/test/%s/test1.in", getClass().getName() );

        config.setChunkSize( 16384 );
        
        ExtractWriter writer = new ExtractWriter( config, path );

        int max = 10000;
        
        for( long i = 0; i < max; ++i ) {

        	StructReader key =StructReaders.wrap( i );
        	StructReader value = key;
            writer.write( key, value );
            
        }

        for( long i = 0; i < max; ++i ) {

        	StructReader key =StructReaders.wrap( i );
        	StructReader value = key;
            writer.write( key, value );
            
        }

        writer.close();
       
        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );

        try {
            controller.map( Map.class, path );
            //controller.reduce( Reduce.class, new Input(), new Output( output ) );
        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {
        System.setProperty( "peregrine.test.factor", "10" ); // 1m
        System.setProperty( "peregrine.test.config", "01:01:1" ); // takes 3 seconds
        runTests();
    }

}