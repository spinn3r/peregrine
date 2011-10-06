package peregrine.perf;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.util.*;
import peregrine.shuffle.*;

/**
 * 
 */
public class ExternalStorageSortPerf {

    public static void main( String[] args ) throws Exception {

        //write random data to a partition stream of chunks files.  This should
        //be MUCH more than memory so that we don't get caching impact.

        //now sort the data in those chunks

        //now merge it all back to disk.

        Config.PFS_ROOT = "/d2/peregrine-dfs";

        Config.addPartitionMembership( 0, "cpu0" );

        Partition part = new Partition( 0 );
        Host host = new Host( "cpu0", 0 );
        String path = "/tmp/benchmark.test";

        LocalPartitionWriter.CHUNK_SIZE = 100000000;

        if ( args.length == 1 ) {
                
            int max = Integer.parseInt( args[0] );

            Random r = new Random();

            int nr_random = 1000;

            List<byte[]> random = new ArrayList();

            for( int i = 0; i < nr_random; ++i ) {
                random.add( LongBytes.toByteArray( r.nextLong() ) );
            }
            
            PartitionWriter partitionWriter = new DefaultPartitionWriter( part, path );

            Iterator<byte[]> it = random.iterator();
            
            for( int i = 0; i < max; ++i ) {

                if ( ! it.hasNext() ) {
                    it = random.iterator();
                }

                byte[] key = it.next();
                
                partitionWriter.write( key, key );
                
            }

            partitionWriter.close();

        }

        long before, after;
        
        before = System.currentTimeMillis();
        
        DiskPerf.sync();
        DiskPerf.dropCaches();

        List<ChunkReader> readers = LocalPartition.getChunkReaders( part, host, path );

        System.out.printf( "Working with %,d chunks." , readers.size() );

        List<ChunkReader> sortedReaders = new ArrayList();
        
        int chunknr = 0;
        for( ChunkReader reader : readers ) {

            System.out.printf( "." );
            
            ChunkSorter sorter = new ChunkSorter();

            String chunk_path = "/d2/sort-chunk-" + chunknr;
            
            LocalChunkWriter writer = new LocalChunkWriter( chunk_path );
            
            sorter.sort( reader );

            sortedReaders.add( new DefaultChunkReader( chunk_path ) );
            
            ++chunknr;

        }

        System.out.printf( "done\n" );

        DiskPerf.sync();
        DiskPerf.dropCaches();

        after = System.currentTimeMillis();
        
        System.out.printf( "duration: %,d ms\n", (after-before) );

        before = System.currentTimeMillis();

        System.out.printf( "Working with %,d sorted chunks.\n" , sortedReaders.size() );

        final AtomicInteger nr_tuples = new AtomicInteger();
        
        ChunkMerger merger = new ChunkMerger( new SortListener() {

                public void onFinalValue( byte[] key, List<byte[]> values ) {
                    nr_tuples.getAndIncrement();
                }
                
            } );

        merger.merge( sortedReaders );

        DiskPerf.sync();
        DiskPerf.dropCaches();

        after = System.currentTimeMillis();

        System.out.printf( "Sorted %,d tuples.\n", merger.tuples );
        System.out.printf( "Merged into %,d tuples.\n", nr_tuples.get() );

        System.out.printf( "duration: %,d ms\n", (after-before) );

    }
    
}
