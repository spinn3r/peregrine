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
import peregrine.app.pagerank.*;
import peregrine.util.*;

/**
 * Tests the mathematical accuracy of our pagerank implementation.
 * 
 * Since all IDs need to be hashcodes we Base64 encode them first and then
 * decode them in the GraphBuilder.  I ended up stripping the last char and
 * replaced it with '_' and 0-9 for readability
 * 
 * 0 = 080ghJXVZ_0
 * 1 = xMpCOKC5I_1
 * 2 = yB5yjZ1ML_2
 * 3 = 7MvIfktc4_3
 * 4 = qH_2eaLz5_4
 * 5 = 5No7f7vOI_5
 * 6 = FnkJHFqID_6
 * 7 = jxTkX87qF_7
 * 8 = yfD4lfuYq_8
 * 9 = RcSMzi4tf_9
 */
public class TestPagerankAccuracy extends peregrine.BaseTestWithMultipleConfigs {

    @Override
    public void doTest() throws Exception {

        doTest( 5000 * getFactor() , 100 ); 

    }

    private void doTest( int nr_nodes,
                         int max_edges_per_node ) throws Exception {

        // only 0 and 1 should be dangling.

        String path = "/pr/test.graph";

        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder builder = new GraphBuilder( writer );

        /*
        addEdge( lr, "1", "2" );
        addEdge( lr, "1", "3" );
        addEdge( lr, "3", "1" );
        addEdge( lr, "3", "2" );
        addEdge( lr, "3", "5" );
        addEdge( lr, "4", "5" );
        addEdge( lr, "4", "6" );
        addEdge( lr, "5", "4" );
        addEdge( lr, "5", "6" );
        addEdge( lr, "6", "4" );
        */

        /*
        builder.addRecord( 1, 2, 3 );
        builder.addRecord( 3, 1, 2, 5 );
        builder.addRecord( 4, 5, 6 );
        builder.addRecord( 5, 4, 6 );
        builder.addRecord( 6, 4 );
        */

        builder.addRecord( "xMpCOKC5I_1", "yB5yjZ1ML_2", "7MvIfktc4_3" );
        builder.addRecord( "7MvIfktc4_3", "xMpCOKC5I_1", "yB5yjZ1ML_2", "5No7f7vOI_5" );
        builder.addRecord( "qH_2eaLz5_4", "5No7f7vOI_5", "FnkJHFqID_6" );
        builder.addRecord( "5No7f7vOI_5", "qH_2eaLz5_4", "FnkJHFqID_6" );
        builder.addRecord( "FnkJHFqID_6", "qH_2eaLz5_4" );
        
        writer.close();
        
        Pagerank pr = null;

        try {
            
            pr = new Pagerank( config, path );

            pr.init();

            dump();
            
            pr.iter();

            dump();
            
        } finally {
            pr.shutdown();
        }

        // now read all results from ALL partitions so that we can verify that
        // we have accurate values.

        //Map<String,Double> rank_vector;

    }

    public static String hash( int id ) {
        return hash( "" + id );
    }

    /**
     * base64_filesafe( truncate( md5( utf8( data ) ) ) )
     */
    public static String hash( String id ) {
        return Base64.encode( Hashcode.getHashcode( id ) );
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

        for ( int i = 0; i < 10; ++i ) {
            System.out.printf( "%s=%s\n", i , hash( i ) );
        }
        
        System.setProperty( "peregrine.test.config", "1:1:1" ); 
        runTests();
        
    }

}
