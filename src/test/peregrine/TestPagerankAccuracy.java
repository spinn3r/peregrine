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

import java.util.*;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.app.pagerank.*;
import peregrine.util.*;
import peregrine.controller.*;

/**
 * Tests the mathematical accuracy of our pagerank implementation.
 * 
 * Since all IDs need to be hashcodes we Base15 encode them first and then
 * decode them in the GraphBuilder.  I ended up picking hashcodes that when
 * encoded end in 0-9:
 * 
 * 6512bd43d9caa6e0
 * c9f0f895fb98ab91
 * c4ca4238a0b92382
 * c81e728d9d4c2f63
 * 98f13708210194c4
 * e4da3b7fbbce2345
 * 67c6a1e7ce56d3d6
 * 70efdf2ec9b08607
 * 02e74f10e0327ad8
 * d3d9446802a44259
 * 
 */
public class TestPagerankAccuracy extends peregrine.BaseTestWithMultipleProcesses {

    @Override
    public void doTest() throws Exception {

        doTest( 5000 * getFactor() , 100 ); 

    }

    private void doTest( int nr_nodes,
                         int max_edges_per_node ) throws Exception {

        Config config = getConfig();

        // only 0 and 1 should be dangling.

        String graph = "/pr/graph";
        String nodes_by_hashcode = "/pr/nodes_by_hashcode";

        ExtractWriter writer = new ExtractWriter( config, graph );
        
        //GraphBuilder builder = new GraphBuilder( config, graph, nodes_by_hashcode );

        /*
        builder.addRecord( "c9f0f895fb98ab91", "c4ca4238a0b92382", "c81e728d9d4c2f63" );
        builder.addRecord( "c81e728d9d4c2f63", "c9f0f895fb98ab91", "c4ca4238a0b92382", "e4da3b7fbbce2345" );
        builder.addRecord( "98f13708210194c4", "e4da3b7fbbce2345", "67c6a1e7ce56d3d6" );
        builder.addRecord( "e4da3b7fbbce2345", "98f13708210194c4", "67c6a1e7ce56d3d6" );
        builder.addRecord( "67c6a1e7ce56d3d6", "98f13708210194c4" );
        */

        writer.write( StructReaders.hashcode( 1 ), StructReaders.hashcode( 2, 3 ) );
        writer.write( StructReaders.hashcode( 3 ), StructReaders.hashcode( 1, 2, 5 ) );
        writer.write( StructReaders.hashcode( 4 ), StructReaders.hashcode( 5, 6 ) );
        writer.write( StructReaders.hashcode( 5 ), StructReaders.hashcode( 4, 6 ) );
        writer.write( StructReaders.hashcode( 6 ), StructReaders.hashcode( 4 ) );
        writer.close();
        
        Pagerank pr = null;

        Controller controller = null;
        
        try {

            controller = new Controller( config );

            controller.map( new Job().setDelegate( Mapper.class )
                                     .setInput( graph )
                                     .setOutput( "shuffle:default" ) );

            controller.reduce( new Job().setDelegate( Reducer.class )
                                        .setInput( "shuffle:default" )
                                        .setOutput( graph ) );
            
            pr = new Pagerank( config, graph, nodes_by_hashcode );
            //pr.prepare();

            pr.init();
            pr.iter();   // 1 OK.. the computed teleport_grat at this iteration is correct since it is virtual
            pr.iter();   // 2 OK... the loaded one is right at this level too!
            pr.iter();   // 3 FIXME: seems to be 0.4.1667 from the last iter... which is wrong.. I thik.
            pr.iter();   // 4 FIXME: seems to be the same ... 0.4.1667... so it isn't being updated... I think.

            for ( Job job : pr.getJobs() ) {
                System.out.printf( "    %s\n", job );
            }

            controller.exec( pr );
            
        } finally {
            if ( controller != null ) 
                controller.shutdown();
        }

        // now read all results from ALL partitions so that we can verify that
        // we have accurate values.

        List<StructPair> rank_vector_data           = read( "/pr/out/rank_vector" );
        List<StructPair> teleportation_grant_data   = read( "/pr/out/teleportation_grant" );

        Map<StructReader,Double> rank_vector = new HashMap();
        Map<StructReader,Double> teleportation_grant = new HashMap();

        for( StructPair pair : rank_vector_data ) {
            rank_vector.put( pair.key, pair.value.readDouble() );
        }

        for( StructPair pair : teleportation_grant_data ) {
            teleportation_grant.put( pair.key, pair.value.readDouble() );
        }

        // print the keys in the rank vector now...

        for( long i = 1; i <= 6; ++i ) {
            System.out.printf( "%10s=%2.10f\n", i, rank_vector.get( StructReaders.hashcode( i ) ) );
        }

        System.out.printf( "teleportation_grant: %s\n", teleportation_grant );
        
        //System.out.printf( "rank_vector: %s\n", rank_vector );

        // addEdge( lr, "5", "4" );
        // addEdge( lr, "5", "6" );
        // addEdge( lr, "6", "4" );

        // lr.exec();

        // //0.037212       0.053957       0.041506       0.375081       0.205998       0.286246 

        /*
        assertEquals( rank_vector.get( "98f13708210194c4" ), 0.26666666666666666 );
        assertEquals( rank_vector.get( "c4ca4238a0b92382" ), 0.16666666666666666 );
        assertEquals( rank_vector.get( "c81e728d9d4c2f63" ), 0.11666666666666667 );
        assertEquals( rank_vector.get( "c9f0f895fb98ab91" ), 0.09166666666666666 );
        assertEquals( rank_vector.get( "e4da3b7fbbce2345" ), 0.16666666666666666 );
        assertEquals( rank_vector.get( "67c6a1e7ce56d3d6" ), 0.19166666666666665 );
        */
    }

    private void dump() throws Exception {
        dump( "/pr/out/node_metadata", "h", "ii" );
        dump( "/pr/out/rank_vector",   "h", "d" );
    }
    
    public static void main( String[] args ) throws Exception {

        //System.setProperty( "peregrine.test.config", "04:1:32" ); 
        //System.setProperty( "peregrine.test.config", "01:1:1" ); 
        //System.setProperty( "peregrine.test.config", "8:1:32" );
        //System.setProperty( "peregrine.test.config", "2:1:3" ); 
        //System.setProperty( "peregrine.test.config", "2:1:3" ); 

        System.setProperty( "peregrine.test.config", "1:1:1" ); 
        runTests();
        
    }

}
