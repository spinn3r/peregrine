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

import peregrine.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;

public class TestChunkWriteAndRead extends BaseTest {

    private void showList( List list ) {

        for( Object obj : list ) {
            System.out.printf( "    %s\n", obj );
        }
        
    }

    private void doTest( int max, boolean minimal ) throws Exception {

        System.out.printf( "Writing new chunk data.\n" );

        File file = new File( "/tmp/test.chunk" );

        DefaultChunkWriter writer = new DefaultChunkWriter( null, file );
        writer.setBlockSize( 1000 );
        writer.setMinimal( minimal );
        
        for( long i = 0; i < max; ++i ) {
            writer.write( StructReaders.wrap( i ), StructReaders.wrap( i ) );
        }
        
        writer.close();

        System.out.printf( "trailer: %s\n", writer.trailer );
        System.out.printf( "fileInfo: %s\n", writer.fileInfo );
        System.out.printf( "dataBlocks: \n" );
        showList( writer.dataBlocks );
        System.out.printf( "metaBlocks: \n" );
        showList( writer.metaBlocks );
        
        DefaultChunkReader reader = new DefaultChunkReader( null, file );

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
        
        System.out.printf( "trailer: %s\n", reader.trailer );
        System.out.printf( "fileInfo: %s\n", reader.fileInfo );
        System.out.printf( "dataBlocks: \n" );
        showList( writer.dataBlocks );
        System.out.printf( "metaBlocks: \n" );
        showList( writer.metaBlocks );


        if ( minimal == false ) {
            
            for( long i = 0; i < max; ++i ) {

                Record record = reader.seekTo( StructReaders.wrap( i ) );

                if ( record == null )
                    throw new RuntimeException( "Could not find entry: " + i );
                
            }

        }

        assertNull( reader.findDataBlock( StructReaders.wrap( max * 2 ) ) );
        
        reader.close();

    }
    
    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {
        doTest( 1000, false );
        doTest( 1000, true );
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
