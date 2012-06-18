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
package peregrine;

import java.io.*;
import java.util.*;
import peregrine.*;
import peregrine.config.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;

public class TestChunkWriterClose extends peregrine.BaseTest {

    public void test1() throws Exception {

        String path = "/tmp/test.chunk";
        
        File file = new File( path );

        DefaultChunkWriter writer = new DefaultChunkWriter( null, file );

        writer.write( StructReaders.hashcode( "1" ),
                      StructReaders.wrap( 1 ) );

        writer.close();

        //now read from it... 

        DefaultChunkReader reader = new DefaultChunkReader( null, file );

        List<StructReader> keys = new ArrayList();
        List<StructReader> values = new ArrayList();
        
        if ( reader.hasNext() ) {
            reader.next();

            keys.add( reader.key() );
            values.add( reader.value() );
                
        }

        reader.close();

        int count = 0;
        
        try {
        
            for( int i = 0; i < keys.size(); ++i ) {

                StructReader key = keys.get( i ) ;
                StructReader value = values.get( i ) ;

                byte[] hashcode = key.readHashcode();
                int v = value.readInt();

                ++count;
                
            }

        } catch ( Exception e ) {
            // this is correct behavior.
        }

        if ( count > 0 )
            throw new Exception( "Test failed.  count=" + count );
        
    }

    public static void main( String[] args ) throws Exception {
        System.setProperty( "peregrine.test.factor", "10" ); // 1m
        System.setProperty( "peregrine.test.config", "01:01:1" ); // takes 3 seconds
        runTests();
    }

}
