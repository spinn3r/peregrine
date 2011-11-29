package peregrine;

import java.util.*;

import peregrine.controller.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.util.primitive.IntBytes;
import peregrine.values.*;
import peregrine.io.partition.*;
import peregrine.task.*;

public class TestNewReduceCode extends peregrine.BaseTestWithTwoDaemons {

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
                throw new RuntimeException( String.format( "%s does not equal %s (%s) on nth reduce %s" , values.size(), 2, ints, nth ) );

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
        ReducerTask.DELETE_SHUFFLE_FILES=false;
    }
    
    public void test1() throws Exception {

        String path = String.format( "/test/%s/test1.in", getClass().getName() );

        DefaultPartitionWriter.CHUNK_SIZE = 16384;
        
        ExtractWriter writer = new ExtractWriter( config, path );

        int max = 10000;
        
        for( long i = 0; i < max; ++i ) {

        	StructReader key =StructReaders.create( i );
        	StructReader value = key;
            writer.write( key, value );
            
        }

        for( long i = 0; i < max; ++i ) {

        	StructReader key =StructReaders.create( i );
        	StructReader value = key;
            writer.write( key, value );
            
        }

        writer.close();
       
        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );

        try {
            controller.map( Map.class, path );
            controller.reduce( Reduce.class, new Input(), new Output( output ) );
        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}