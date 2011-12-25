package peregrine.io;

import peregrine.*;

public class TestExtractWriter extends BaseTestWithTwoPartitions {

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        String path = "/test/extract1";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        for ( long i = 0; i < 100; ++i ) {

        	StructReader key = StructReaders.wrap( i );

        	StructReader value = key;

            writer.write( key, value );
            
        }

        writer.close();

        System.out.printf( "worked.\n" );

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
