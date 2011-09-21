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
            
            OutputStream out = new BufferedOutputStream( new FileOutputStream( path ), BLOCK_SIZE );

            for ( int i = 0; i < size / BLOCK_SIZE; ++i ) {
                out.write( BLOCK );
            }

            // finish out the file so that it has the right number of blocks
            int padding = BLOCK_SIZE - (int)(size % BLOCK_SIZE);

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

        System.out.printf( "nr_regions: %,d\n", nr_regions );
        System.out.printf( "read_size: %,d\n", read_size );
        
        RandomAccessFile raf = new RandomAccessFile( path, "r" );

        before = System.currentTimeMillis();
        
        long offset = 0;

        long bytes_read = 0;
        
        for ( int i = 0; i < nr_regions; ++i ) {

            offset = i * buffer;
            raf.seek( offset );
            byte[] data = new byte[ read_size ];
            raf.read();

            bytes_read += data.length;
            
        }

        after = System.currentTimeMillis();

        duration = (after - before);
        
        System.out.printf( "bytes read: %,d bytes\n", bytes_read );
        System.out.printf( "duration: %,d ms\n", duration );

    }

    //public 
    
}
