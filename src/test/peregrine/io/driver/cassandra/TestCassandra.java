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
package peregrine.io.driver.cassandra;

import java.util.*;
import java.net.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.http.*;
import peregrine.io.*;
import peregrine.io.driver.*;
import peregrine.io.driver.cassandra.*;
import peregrine.worker.*;
import peregrine.task.*;

public class TestCassandra extends peregrine.BaseTestWithTwoDaemons {

    public void doTest() throws Exception {
        _test3();
        _test1();
        _test2();
    }
    
    public void _test1() throws Exception {

        IODriver driver = IODriverRegistry.getInstance( "cassandra" );

        assertNotNull( driver );
        
        CassandraInputReference ref =
            (CassandraInputReference)driver.getInputReference( "cassandra://localhost:9160/mykeyspace/graph" );

        assertEquals( "localhost", ref.getHost() );
        assertEquals( "9160", ref.getPort() );
        assertEquals( "mykeyspace", ref.getKeyspace() );
        assertEquals( "graph", ref.getColumnFamily() );
        
    }

    public void _test2() throws Exception {

        IODriver driver = IODriverRegistry.getInstance( "cassandra" );

        String uri = "cassandra://localhost:9160/mykeyspace/graph";
        
        CassandraInputReference ref =
            (CassandraInputReference)driver.getInputReference( uri );

        // get the work units from cassandra now.

        Config config = configs.get( 0 );
        
        Map<Host,List<Work>> workMap = driver.getWork( config, ref );

        assertTrue( workMap.size() > 0 );

        int count = 0;
        
        for ( Host host : workMap.keySet() ) {

            List<Work> workList = workMap.get( host );

            for( Work work : workList ) {

                for( WorkReference workRef : work.getReferences() ) {

                    JobInput input = driver.getJobInput( config, ref, workRef );

                    while( input.hasNext() ) {

                        input.next();

                        input.key();
                        input.value();

                        ++count;
                        
                    }
                    
                }

            }

        }

        assertTrue( count > 0 );

        System.out.printf( "found %,d entries.\n", count );
        
    }

    public void _test3() throws Exception {

        // test writes 

        IODriver driver = IODriverRegistry.getInstance( "cassandra" );

        CassandraInputReference inputRef =
            (CassandraInputReference)driver.getInputReference( "cassandra://localhost:9160/mykeyspace/graph" );

        CassandraOutputReference outputRef =
            (CassandraOutputReference)driver.getOutputReference( "cassandra://localhost:9160/mykeyspace/rank" );

        StructReader key = StructReaders.hashcode( "1" );

        StructSequenceWriter structSequenceWriter = new StructSequenceWriter();
        structSequenceWriter.write( StructReaders.hashcode( "1" ),
                                    StructReaders.hashcode( "1" ) );

        StructReader value = structSequenceWriter.toStructReader();

        Config config = configs.get( 0 );

        Map<Host,List<Work>> workMap = driver.getWork( config, inputRef );

        assertTrue( workMap.size() > 0 );

        int count = 0;
        
        for ( Host host : workMap.keySet() ) {

            List<Work> workList = workMap.get( host );

            for( Work work : workList ) {

                for( WorkReference workRef : work.getReferences() ) {

                    JobOutput output = driver.getJobOutput( config, outputRef, workRef );

                    output.emit( key, value );

                    output.close();
                    
                }

            }

        }

    }
    
    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
