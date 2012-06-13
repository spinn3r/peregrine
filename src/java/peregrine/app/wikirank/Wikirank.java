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
package peregrine.app.wikirank;

import java.io.*;

import peregrine.*;
import peregrine.config.Config;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.app.pagerank.*;

import com.spinn3r.log5j.Logger;

public class Wikirank {
    
    private static final Logger log = Logger.getLogger();

    private Config config;

    private Controller controller;
    
    private String nodes_path;

    private String links_path;
    
    public Wikirank( Config config, Controller controller, String nodes_path, String links_path ) {
        this.config = config;
        this.controller = controller;
        this.nodes_path = nodes_path;
        this.links_path = links_path;
    }

    public void run() throws Exception {
        extract();
        fixup();
        transform();
        load();
    }

    public void extract() throws Exception {

        writeNodes( nodes_path );
        writeLinks( links_path );

    }

    public void fixup() throws Exception {

        controller.map( CreateNodeLookupJob.Map.class,
                        new Input( "/wikirank/nodes" ),
                        new Output( "shuffle:nodesByPrimaryKey",
                                    "shuffle:nodesByHashcode" ) );

        controller.reduce( Reducer.class,
                           new Input( "shuffle:nodesByPrimaryKey" ),
                           new Output( "/wikirank/nodesByPrimaryKey" ) );

        controller.reduce( Reducer.class,
                           new Input( "shuffle:nodesByHashcode" ),
                           new Output( "/wikirank/nodesByHashcode" ) );
                           
        controller.map( FlattenLinksJob.Map.class,
                        new Input( "/wikirank/links" ),
                        new Output( "shuffle:default" ) );
                        
        controller.reduce( FlattenLinksJob.Reduce.class,
                           new Input( "shuffle:default" ),
                           new Output( "/wikirank/links.flattened" ) );

        // this joins the node table AND the links table and then writes a
        // raw hashcode graph for use with pagerank.
        controller.merge( MergePagesAndLinksJob.Merge.class,
                          new Input( "/wikirank/nodesByPrimaryKey",
                                     "/wikirank/links.flattened" ),
                          new Output( "/wikirank/graph" ) );

    }

    public void transform() throws Exception {

        // the graph is written, now launch a job to finish it up.

        Pagerank pr = new Pagerank( config, "/wikirank/graph", controller );
        pr.exec( false );

    }
    
    public void load() throws Exception {

        //now join against nodesByPrimaryKey and and rank graph so that we can
        //have rank per node

        // merge /pr/out/rank_vector and nodesByPrimaryKey and node_metadata

        controller.merge( MergeNodeAndRankMetaJob.Merge.class,
                          new Input( "/pr/out/node_metadata",
                                     "/pr/out/rank_vector",
                                     "/wikirank/nodesByHashcode" ),
                          new Output( "/wikirank/rank_metadata" ) );

    }
    
    private int writeNodes( String input ) throws Exception {

        ExtractWriter writer = new ExtractWriter( config, "/wikirank/nodes" );

        PageParser parser = new PageParser( input );

        int wrote = 0;
        
        while( true ) {

            PageParser.Page page = parser.next();

            if ( page == null )
                break;

            ++wrote;
            
            StructReader key = StructReaders.hashcode( "" + page.id );
            StructReader value = StructReaders.wrap( page.name );
            
            writer.write( key , value );

        }

        writer.close();

        log.info( "Wrote %,d nodes", wrote );
        
        return wrote;
        
    }

    private int writeLinks( String input ) throws Exception {

        ExtractWriter writer = new ExtractWriter( config, "/wikirank/links" );

        PageLinkParser parser = new PageLinkParser( input );

        int wrote = 0;

        while( true ) {

            PageLinkParser.Link link = parser.next();

            if ( link == null )
                break;

            //if ( index > LIMIT )
            //    break;
            
            StructReader key = StructReaders.hashcode( "" + link.id );
            StructReader value = StructReaders.wrap( link.name );
            
            writer.write( key , value );
            ++wrote;
        }

        writer.close();

        log.info( "Wrote %,d links", wrote );

        return wrote;
        
    }

}

