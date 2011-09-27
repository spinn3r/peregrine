package maprunner.test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.io.*;
import maprunner.util.*;
import maprunner.shuffle.*;

/**
 * 
 */
public class ExternalStorageSortPerf {

    public static void main( String[] args ) throws Exception {

        //write random data to a partition stream of chunks files.  This should
        //be MUCH more than memory so that we don't get caching impact.

        //now sort the data in those chunks

        //now merge it all back to disk.

        Config.DFS_ROOT = "/d2/maprunner-dfs";

        Config.addPartitionMembership( 0, "cpu0" );

        Partition part = new Partition( 0 );
        Host host = new Host( "cpu0", 0, 0 );
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
            
            PartitionWriter partitionWriter = new PartitionWriter( part, path );

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

        DiskPerf.sync();
        DiskPerf.dropCaches();
        
        PartitionReader partitionReader = new PartitionReader( part, host, path );

        List<ChunkReader> readers = partitionReader.getChunkReaders();

        List<ChunkReader> sortedReaders = new ArrayList();
        
        int chunknr = 0;
        for( ChunkReader reader : readers ) {
            
            ChunkSorter sorter = new ChunkSorter();

            String chunk_path = "/tmp/sort-chunk-" + chunknr;
            
            ChunkWriter writer = new ChunkWriter( chunk_path );
            
            sorter.sort( reader, writer );

            sortedReaders.add( new DefaultChunkReader( chunk_path ) );
            
            ++chunknr;

        }

        DiskPerf.sync();
        DiskPerf.dropCaches();

        final AtomicInteger nr_tuples = new AtomicInteger();
        
        ChunkMerger merger = new ChunkMerger( new SortListener() {

                public void onFinalValue( byte[] key, List<byte[]> values ) {
                    nr_tuples.getAndIncrement();
                }
                
            } );

        merger.merge( sortedReaders );

        System.out.printf( "Sorted %,d tuples.\n", nr_tuples.get() );
        
    }
    
}
