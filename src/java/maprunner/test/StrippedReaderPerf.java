package maprunner.test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 
 */
public class StrippedReaderPerf {

    public static void main( String[] args ) throws Exception {

        if ( args.length != 3 ) {

            System.out.printf( "SYNTAX full_file_size simulated_buffer_size nr_partitions\n" );
            System.exit( 1 );
        }
        
        //total size of the whole file
        long size = Long.parseLong( args[0] );

        //the simulated buffer on the client... we would then have to read one
        //chunk of data from size / buffer regions in the file.
        long buffer = Long.parseLong( args[1] );

        long partitions = Long.parseLong( args[2] );

        String path = "stripped-reader.test";

        System.out.printf( "Using size of %,d bytes.\n", size );
        System.out.printf( "Using buffer of %,d bytes.\n", buffer );
        System.out.printf( "Using partitions of %,d.\n", partitions );

        long before, after, duration;
        
        // ****************** WRITE our test file.

        before = System.currentTimeMillis();
        
        File file = new File( path );

        if ( file.exists() == false || file.length() != size ) {
            
            System.out.printf( "Creating input file..." );

            int BLOCK_SIZE = 16384;

            byte[] BLOCK = new byte[ BLOCK_SIZE ];
            
            BufferedOutputStream out = new BufferedOutputStream( new FileOutputStream( path ), BLOCK_SIZE );

            int nr_blocks = (int)(size / BLOCK_SIZE);
            
            for ( int i = 0; i < nr_blocks; ++i ) {
                out.write( BLOCK );
            }

            // finish out the file so that it has the right number of blocks
            int padding = (int)(size - (nr_blocks * BLOCK_SIZE));

            BLOCK = new byte[ padding ];
            out.write( BLOCK );
            
            out.close();

            System.out.printf( "done\n" );

        }
            
        DiskPerf.sync();
        DiskPerf.dropCaches();

        after = System.currentTimeMillis();
        duration = (after - before);
        
        System.out.printf( "duration: %,d ms\n", duration );

        // ******************

        long nr_regions = size / buffer;
        int read_size = (int)(buffer / partitions);
        long max_read_bytes = 10 * buffer; //would be nice to pass this into the command line.

        System.out.printf( "nr_regions: %,d\n", nr_regions );
        System.out.printf( "read_size: %,d\n", read_size );

        System.out.printf( "Starting read..." );
        
        RandomAccessFile raf = new RandomAccessFile( path, "r" );

        before = System.currentTimeMillis();
        
        long offset = 0;

        long bytes_read = 0;
        
        for ( int i = 0; i < nr_regions; ++i ) {

            offset = i * buffer;

            raf.seek( offset );

            byte[] data = new byte[ read_size ];
            int read_result = raf.read( data );

            if ( read_result != data.length )
                throw new Exception( String.format( "NR bytes read (%,d) doesn't equal bytes requested (%,d)", read_result, data.length ) );

            bytes_read += data.length;

            if ( bytes_read > max_read_bytes )
                break;
            
        }

        after = System.currentTimeMillis();

        System.out.printf( "done\n" );

        duration = (after - before);
        long duration_seconds = (duration / 1000);

        System.out.printf( "bytes read: %,d bytes\n", bytes_read );
        System.out.printf( "duration: %,d ms\n", duration );

        if ( duration_seconds > 0 ) {
        
            long bytes_per_sec = (long)((bytes_read / (double)duration) * 1000);
            long MB_per_sec = bytes_per_sec / 1000000;
            
            System.out.printf( "throughput: %,d B/s\n", bytes_per_sec );
            System.out.printf( "throughput: %,d MB/s\n", MB_per_sec );

        }

    }
    
}
