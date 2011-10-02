package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;

public class TestMapReduce extends junit.framework.TestCase {

    public static class Map extends Mapper {

        @Override
        public void map( byte[] key,
                         byte[] value ) {

            emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        int count = 0;
        
        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            ++count;
            
            // full of fail... 
            if ( values.size() != 2 )
                throw new RuntimeException();
            
        }

        @Override
        public void cleanup() {

            if ( count == 0 )
                throw new RuntimeException();
            
        }

    }

    public void test1() throws Exception {

        // TRY with three partitions... 
        Config.addPartitionMembership( 0, "cpu0" );
        Config.addPartitionMembership( 1, "cpu1" );
        
        String path = String.format( "/test/%s/test1.in", getClass().getName() );
        
        ExtractWriter writer = new ExtractWriter( path );

        for( int i = 0; i < 1000; ++i ) {

            byte[] key = new IntKey( i ).toBytes();
            byte[] value = key;
            writer.write( key, value );
            
        }

        for( int i = 0; i < 1000; ++i ) {

            byte[] key = new IntKey( i ).toBytes();
            byte[] value = key;
            writer.write( key, value );
            
        }

        writer.close();

        String output = String.format( "/test/%s/test1.out", getClass().getName() );
        
        Controller.map( Map.class, path );
        Controller.reduce( Reduce.class, null, new Output( output ) );

    }

}