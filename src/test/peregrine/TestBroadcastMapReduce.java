package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;

public class TestBroadcastMapReduce extends junit.framework.TestCase {

    public static class Map extends Mapper {

        private int count = 0;

        private JobOutput countBroadcast = null;
        
        @Override
        public void init( JobOutput... output ) {
            super.init( output );

            countBroadcast = output[0];
            
        }

        @Override
        public void map( byte[] key,
                         byte[] value ) {

            //emit( key, value );
            ++count;
            
        }

        @Override
        public void cleanup() {

            if ( count == 0 )
                throw new RuntimeException();

            System.out.printf( "Writing count: %,d\n", count );
            
            //countBroadcast.emit( new IntKey( count ).toBytes(), new IntValue( count ).toBytes() );
            
        }

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            int count = 0;
            
            for( byte[] val : values ) {
                count += new IntValue( val ).value;
            }
            
            //if ( count != 1000 )
            //    throw new RuntimeException( "Wrong size: " + count );

            //System.out.printf( "FIXME: found %,d count\n", count );
            
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

        writer.close();

        String output = String.format( "/test/%s/test1.out", getClass().getName() );
        
        Controller.map( Map.class,
                        new Input( path ),
                        new Output( new BroadcastReference( "count" ) ) );

        Controller.reduce( Reduce.class,
                           new Input( new ShuffleInputReference( "count" ) ),
                           new Output( output ) );

    }

    public static void main( String[] args ) throws Exception {
        new TestBroadcastMapReduce().test1();
    }

}