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
package peregrine.pfsd;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;

public class TestExtractWriterPerformance extends BaseTestWithMultipleProcesses {

    private ExtractWriter writer;

    private String path = "/test/extract1";

    public void init() throws Exception {

        Config config = getConfig();

        long before = System.currentTimeMillis();

        writer = new ExtractWriter( config, path );

        long after = System.currentTimeMillis();

        long duration = after-before;

        System.out.printf( "init duration: %,d ms\n", duration );

    }

    public void close() throws Exception {

        long before = System.currentTimeMillis();

        writer.close();

        long after = System.currentTimeMillis();

        long duration = after-before;

        System.out.printf( "close duration: %,d ms\n", duration );

    }

    public void write() throws Exception {

        long before = System.currentTimeMillis();

        //int max = 100000 * getFactor();

        //int max = 12500 * getFactor();
        int max = 300 * getFactor();

        //byte[] value = new byte[8];
        StructReader value = StructReaders.wrap( new byte[8192] );
        
        for ( long i = 0; i < max; ++i ) {

        	StructReader key = StructReaders.wrap( i );

            writer.write( key, value );
            
        }

        long after = System.currentTimeMillis();

        long duration = after-before;

        System.out.printf( "write duration: %,d ms\n", duration );

    }

    /**
     * test running with two lists which each have different values.
     */
    public void doTest() throws Exception {

        long before = System.currentTimeMillis();

        init();

        write();
        
        close();
        
        long after = System.currentTimeMillis();

        long duration = (after-before);

        long throughput = -1;

        try {
            throughput = writer.length() / ( duration / 1000 );
        } catch ( Throwable t ) {}
        
        System.out.printf( "wrote %,d bytes to %s with duration %,d ms with %,d b/s\n", writer.length(), path, duration, throughput );

    }

    public static void main( String[] args ) throws Exception {

        System.setProperty( "peregrine.test.factor", "150" ); 
        //System.setProperty( "peregrine.test.config", "02:02:5" ); 
        //System.setProperty( "peregrine.test.config", "01:01:1" ); 
        //System.setProperty( "peregrine.test.config", "01:01:64" ); 
        System.setProperty( "peregrine.test.config", "1:1:256" ); 
        
        //runTests();

        TestExtractWriterPerformance test;

        for( int i = 0; i < 2; ++i ) {
            
            test = new TestExtractWriterPerformance();
            
            test.setUp();
            test.test();
            //test.shutdown();
            test.tearDown();

        }

    }

}
