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

import peregrine.io.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.app.pagerank.*;
import peregrine.app.wikirank.*;

public class TestWikirank extends peregrine.BaseTestWithMultipleProcesses {

    public static int LIMIT = 10000;
    
    @Override
    public void doTest() throws Exception {

        Config config = getConfig();

        writeNodes( "corpus/wikirank/enwiki-20120502-page-sample.sql" );
        writeLinks( "corpus/wikirank/enwiki-20120502-pagelinks-sample.sql" );

        Controller controller = null;

        try {

            controller = new Controller( config );

            controller.map( Mapper.class,
                            new Input( "/wikirank/nodes" ),
                            new Output( "shuffle:default" ) );

            controller.reduce( Reducer.class,
                               new Input( "shuffle:default" ),
                               new Output( "/wikirank/nodes.sorted" ) );
                            
            controller.map( FlattenLinksJob.Map.class,
                            new Input( "/wikirank/links" ),
                            new Output( "shuffle:default" ) );

            controller.reduce( FlattenLinksJob.Reduce.class,
                               new Input( "shuffle:default" ),
                               new Output( "/wikirank/links.flattened" ) );

            // this joins the node table AND the links table and then writes a
            // raw hashcode graph for use with pagerank.
            controller.merge( MergePagesAndLinksJob.Merge.class,
                              new Input( "/wikirank/nodes.sorted",
                                         "/wikirank/links.flattened" ),
                              new Output( "/wikirank/graph" ) );

            // the graph is written, now launch a job to finish it up.

            Pagerank pr = new Pagerank( config, "/wikirank/graph", controller );
            pr.exec();

            //now join against nodes.sorted and and rank graph so that we can
            //have rank per node

            // merge /pr/out/rank_vector and nodes.sorted and node_metadata
            
        } finally {
            controller.shutdown();
        }
            
    }

    private void writeNodes( String input ) throws Exception {

        Config config = getConfig();
        
        ExtractWriter writer = new ExtractWriter( config, "/wikirank/nodes" );

        PageParser parser = new PageParser( input );

        int index = 0;
        
        while( true ) {

            PageParser.Page page = parser.next();

            if ( page == null )
                break;

            ++index;

            if ( index > LIMIT )
                break;
            
            StructReader key = StructReaders.hashcode( "" + page.id );
            StructReader value = StructReaders.wrap( page.name );
            
            writer.write( key , value );

        }

        writer.close();

    }

    private void writeLinks( String input ) throws Exception {

        Config config = getConfig();
        
        ExtractWriter writer = new ExtractWriter( config, "/wikirank/links" );

        PageLinkParser parser = new PageLinkParser( input );

        int index = 0;
        
        while( true ) {

            PageLinkParser.Link link = parser.next();

            if ( link == null )
                break;

            ++index;

            if ( index > LIMIT )
                break;
            
            StructReader key = StructReaders.hashcode( "" + link.id );
            StructReader value = StructReaders.wrap( link.name );
            
            writer.write( key , value );

        }

        writer.close();
        
    }
    
    public static void main( String[] args ) throws Exception {

        setPropertyDefault( "peregrine.test.config", "1:1:1" ); 
        runTests();

    }

}
