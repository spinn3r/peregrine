/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.io.chunk;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;
import peregrine.io.sstable.*;

public class TestChunkWriteAndRead extends BaseTest {

    private void showList( List list ) {

        for( Object obj : list ) {
            System.out.printf( "    %s\n", obj );
        }
        
    }

    private void doTestSeekTo( int max, boolean minimal ) throws Exception {

        System.out.printf( "Writing new chunk data.\n" );

        Config config = ConfigParser.parse();
        
        File file = new File( "/tmp/test.chunk" );

        DefaultChunkWriter writer = new DefaultChunkWriter( config, file );
        writer.setBlockSize( 1000 );
        writer.setMinimal( minimal );
        
        for( long i = 0; i < max; ++i ) {
            writer.write( StructReaders.wrap( i ), StructReaders.wrap( i ) );
        }
        
        writer.close();

        System.out.printf( "trailer: %s\n", writer.getTrailer() );
        System.out.printf( "fileInfo: %s\n", writer.getFileInfo() );
        System.out.printf( "dataBlocks: \n" );
        showList( writer.getDataBlocks() );
        System.out.printf( "metaBlocks: \n" );
        showList( writer.getMetaBlocks() );
        
        DefaultChunkReader reader = new DefaultChunkReader( config, file );

        int count = 0;
        
        while( reader.hasNext() ) {

            reader.next();
            assertEquals( 8, reader.key().length() );
            assertEquals( 8, reader.value().length() );
            ++count;

        }

        assertEquals( count, max );
        assertEquals( reader.count(), max );

        if ( minimal == false ) {
        
            for( long i = 0; i < max; ++i ) {
                assertNotNull( reader.findDataBlock( StructReaders.wrap( i ) ) );
            }

        }
            
        System.out.printf( "==============\n" );
        
        System.out.printf( "trailer: %s\n", reader.getTrailer() );
        System.out.printf( "fileInfo: %s\n", reader.getFileInfo() );
        System.out.printf( "dataBlocks: \n" );
        showList( writer.getDataBlocks() );
        System.out.printf( "metaBlocks: \n" );
        showList( writer.getMetaBlocks() );

        if ( minimal == false ) {
            
            for( long i = 0; i < max; ++i ) {

                Record record = reader.seekTo( StructReaders.wrap( i ) );

                if ( record == null )
                    throw new RuntimeException( "Could not find entry: " + i );
                
            }

            assertNull( reader.findDataBlock( StructReaders.wrap( Long.MAX_VALUE ) ) );
            assertNull( reader.findDataBlock( StructReaders.wrap( (long)max * 2 ) ) );
            assertNull( reader.findDataBlock( StructReaders.wrap( (long)max ) ) );

            if ( max > 0 ) {
                assertNotNull( reader.findDataBlock( StructReaders.wrap( (long)max-1 ) ) );
            }

        }

        reader.close();

    }
    
    /**
     * test running with two lists which each have different values.
     */
    public void testSeekTo() throws Exception {

        doTestSeekTo( 0, false );
        doTestSeekTo( 0, true );

        doTestSeekTo( 1000, false );
        doTestSeekTo( 1000, true );
    }

    private void doTestScan( Scan scan, int max, List<StructReader> keys ) throws IOException {

        File file = new File( "/tmp/test.chunk" );

        Config config = ConfigParser.parse();

        DefaultChunkWriter writer = new DefaultChunkWriter( config, file );
        writer.setBlockSize( 1000 );
        writer.setMinimal( false );

        for( long i = 0; i < max; ++i ) {
            writer.write( StructReaders.wrap( i ), StructReaders.wrap( i ) );
        }
        
        writer.close();

        DefaultChunkReader reader = new DefaultChunkReader( config, file );

        final List<StructReader> found = new ArrayList();
        
        reader.scan( scan, new ScanListener() {

                @Override
                public void onRecord( StructReader key, StructReader value ) {

                    System.out.printf( "  scan.onRecord: key=%s\n", Hex.encode( key ) );

                    found.add( key );

                }

            } );

        assertEquals( found , keys );

        reader.close();

    }

    // get a list of StructReader between the given range (inclusive)
    private List<StructReader> range( long start, long end ) {

        List<StructReader> result = new ArrayList();

        for( long i = start; i <= end; ++i ) {
            result.add( StructReaders.wrap( i ) );
        }

        return result;
        
    }

    public void testScan() throws Exception {

        // we have to test the powerset of all possible options
        //
        // no start
        // start inclusive
        // start exclusive
        //
        // no end
        // end inclusive
        // end exclusive
        //
        // beginning of chunk reader
        // empty chunk reader
        // at end of chunk reader

        Scan scan;
        
        // ********* no start / no end.

        scan = new Scan();
        scan.setLimit( 10 );
        doTestScan( scan, 1000, range( 0, 9 ) );

        // ********* no start / end inclusive
        scan = new Scan();
        scan.setEnd( StructReaders.wrap( 1L ), true );
        scan.setLimit( 10 );
        doTestScan( scan, 1000, range( 0, 1 ) );

        // ********* no start / end exclusive

        scan = new Scan();
        scan.setEnd( StructReaders.wrap( 1L ), false );
        scan.setLimit( 10 );
        doTestScan( scan, 1000, range( 0, 0 ) );

        // ********* start inclusive / no end
        scan = new Scan();
        scan.setStart( StructReaders.wrap( 0L ), true );
        scan.setLimit( 10 );
        doTestScan( scan, 1000, range( 0, 9 ) );

        // ********* start inclusive / end inclusive.
        scan = new Scan();
        scan.setStart( StructReaders.wrap( 1L ), true );
        scan.setEnd( StructReaders.wrap( 2L ), true );
        scan.setLimit( 10 );
        doTestScan( scan, 1000, range( 1, 2 ) );

        // ********* start inclusive / end exclusive
        scan = new Scan();
        scan.setStart( StructReaders.wrap( 1L ), true );
        scan.setEnd( StructReaders.wrap( 2L ), false );
        scan.setLimit( 10 );
        doTestScan( scan, 1000, range( 1, 1 ) );

        // ********* start exclusive / no end
        scan = new Scan();
        scan.setStart( StructReaders.wrap( 0L ), false );
        scan.setLimit( 10 );
        doTestScan( scan, 1000, range( 1, 10 ) );

        // ********* start exclusive / end exclusive.
        scan = new Scan();
        scan.setStart( StructReaders.wrap( 1L ), false );
        scan.setEnd( StructReaders.wrap( 2L ), true );
        scan.setLimit( 10 );
        doTestScan( scan, 1000, range( 2, 2 ) );

        // ********* start exclusive / end exclusive
        scan = new Scan();
        scan.setStart( StructReaders.wrap( 1L ), false );
        scan.setEnd( StructReaders.wrap( 3L ), false );
        scan.setLimit( 10 );
        doTestScan( scan, 1000, range( 2, 2 ) );

    }
    
    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
