package peregrine;

import java.io.*;
import peregrine.config.Partition;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;

public class TestMapOnlyJobs extends peregrine.BaseTestWithTwoDaemons {

    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
        		         StructReader value ) {

            emit( key, value );
            
        }

    }

    public void test1() throws Exception {

        String path = "/test/map.only/test1";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        for( int i = 0; i < 10; ++i ) {

        	StructReader key = StructReaders.wrap((long)i);
        	StructReader value = key;
            writer.write( key, value );
            
        }
        
        writer.close();

        // I think a more ideal API would be Controller.exec( path, mapper, reducer );

        //FIXME: /pr/test.graph will NOT be sorted on input even though the
        //values are unique.... on stage two we won't be able to join against
        //it.

        String output = "/test/map.only/test1.out";

        Controller controller = new Controller( config );

        try {
            
            controller.map( Map.class, new Input( path ), new Output( output ) );

            Partition part = new Partition( 1 );

            LocalPartitionReader reader = new LocalPartitionReader( config1, part, output );

            if ( reader.hasNext() == false )
                throw new IOException( "nothing written" );

        } finally {
            controller.shutdown();
        }
            
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}