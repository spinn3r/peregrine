package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;
import peregrine.io.partition.*;

public class TestMapOnlyJobs extends junit.framework.TestCase {

    public static class Map extends Mapper {

        @Override
        public void map( byte[] key,
                         byte[] value ) {

            emit( key, value );
            
        }

    }

    public void test1() throws Exception {

        // TRY with three partitions... 
        Config.addPartitionMembership( 0, "cpu0" );
        Config.addPartitionMembership( 1, "cpu1" );
        
        String path = "/test/map.only/test1";
        
        ExtractWriter writer = new ExtractWriter( path );

        for( int i = 0; i < 10; ++i ) {

            byte[] key = new IntKey( i ).toBytes();
            byte[] value = key;
            writer.write( key, value );
            
        }
        
        writer.close();

        // I think a more ideal API would be Controller.exec( path, mapper, reducer );

        //FIXME: /pr/test.graph will NOT be sorted on input even though the
        //values are unique.... on stage two we won't be able to join against
        //it.

        String output = "/test/map.only/test1.out";
        
        Controller.map( Map.class, new Input( path ), new Output( output ) );

        Partition part = new Partition( 0 );
        Host host = new Host( "cpu0", 0  );

        LocalPartitionReader reader = new LocalPartitionReader( part, host, output );

        if ( reader.hasNext() == false )
            throw new IOException( "nothing written" );

    }

}