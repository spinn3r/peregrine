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

            //FIXME: this is a bug because STDOUT will become a broadcast entry.
            //emit( key, value );
            ++count;
            
        }

        @Override
        public void cleanup() {

            if ( count == 0 )
                throw new RuntimeException();

            System.out.printf( "Writing count: %,d\n", count );

            byte[] key = new StructWriter()
                .writeHashcode( "count" )
                .toBytes();

            byte[] value = new StructWriter()
                .writeVarint( count )
                .toBytes();

            System.out.printf( "CLEANUP will emit %,d \n", count );
            
            countBroadcast.emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            int count = 0;
            
            for( byte[] val : values ) {
                count += new StructReader( val ).readVarint();
            }

            byte[] value = new StructWriter()
                .writeVarint( count )
                .toBytes();

            emit( key, value );
            
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
                        new Output( new BroadcastOutputReference( "count" ) ) );

        String count_out = String.format( "/test/%s/test1.count", getClass().getName() );

        Controller.reduce( Reduce.class,
                           new Input( new ShuffleInputReference( "count" ) ),
                           new Output( count_out ) );

        // FIXME: we have to actually emit values from the reducer and assert
        // their value across all partitions now.

        // now read all partition values...
        
        java.util.Map<Partition,List<Host>> membership = Config.getPartitionMembership();
        
        for( Partition part : membership.keySet() ) {

            for( Host host : membership.get( part ) ) {

                LocalPartitionReader reader = new LocalPartitionReader( part, host, count_out );

                Tuple t = reader.read();

                int count = new StructReader( t.value ).readVarint();

                if ( count != 1000 )
                    throw new Exception( "Invalid count: " + count );
                
                System.out.printf( "count: %,d\n", count );
                
                if ( reader.read() != null )
                    throw new IOException( "too many values" );
                
            }

        }

    }

    public static void main( String[] args ) throws Exception {
        new TestBroadcastMapReduce().test1();
    }

}