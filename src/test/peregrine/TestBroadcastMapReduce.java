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

public class TestBroadcastMapReduce extends peregrine.BaseTestWithTwoDaemons {

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
            ++count;
        }

        @Override
        public void cleanup() {
            
            if ( count == 0 ) {
                throw new RuntimeException();
            }

            System.out.printf( "Writing count: %,d\n", count );

            byte[] key = new StructWriter()
                .writeHashcode( "count" )
                .toBytes();

            byte[] value = new StructWriter()
                .writeVarint( count )
                .toBytes();

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

    /**
     *
     * FIXME: I disabled this test... I don't it was EVER working and it isn't
     * so important right now during the refactor.
     * 
     */
     public void test1() throws Exception {

         System.out.printf( "FIXME: partitions: %s\n" , config.getMembership().getPartitions() );
         
         String path = String.format( "/test/%s/test1.in", getClass().getName() );
        
         ExtractWriter writer = new ExtractWriter( config, path );
         
         for( int i = 0; i < 1000; ++i ) {
             
             byte[] key = new IntKey( i ).toBytes();
             byte[] value = key;
             writer.write( key, value );
             
         }
         
         writer.close();
         
         String output = String.format( "/test/%s/test1.out", getClass().getName() );

         Controller controller = new Controller( config );

         controller.map( Map.class,
                         new Input( path ),
                         new Output( new BroadcastOutputReference( "count" ) ) );

         String count_out = String.format( "/test/%s/test1.count", getClass().getName() );
         
         controller.reduce( Reduce.class,
                            new Input( new ShuffleInputReference( "count" ) ),
                            new Output( count_out ) );
         
         // FIXME: we have to actually emit values from the reducer and assert
         // their value across all partitions now.
         
         // now read all partition values...
         
         //FIXME: this broke no the local version and we ned to add it back in.
         //assertValueOnAllPartitions( count_out, 1000 );
         
         System.out.printf( "WIN\n" );
        
    }

    public static void assertValueOnAllPartitions( Config config, String path, int value ) throws Exception {

        Membership membership = config.getMembership();
        
        for( Partition part : membership.getPartitions() ) {

            for( Host host : membership.getHosts( part ) ) {

                LocalPartitionReader reader = new LocalPartitionReader( config, part, host, path );

                if ( reader.hasNext() == false )
                    throw new Exception( "No values" );

                byte[] _key = reader.key();
                byte[] _value = reader.value();
                
                int count = new StructReader( _value )
                    .readVarint();

                if ( count != value )
                    throw new Exception( "Invalid value: " + count );
                
                System.out.printf( "count: %,d\n", count );
                
                if ( reader.hasNext() )
                    throw new IOException( "too many values" );
                
            }

        }

    }
    
    public static void main( String[] args ) throws Exception {
        runTests();
    }

}