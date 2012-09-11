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

/**
 * Compute Pagerank from the Wikipedia data.
 */
public class Wikirank {
    
    private static final Logger log = Logger.getLogger();

    /**
     * The namespace to import and compute rank over.  
     */
    private static int NAMESPACE = 0;
    
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
                        new Output( "shuffle:nodes_by_primary_Key",
                                    "shuffle:nodes_by_hashcode" ) );

        controller.reduce( Reducer.class,
                           new Input( "shuffle:nodes_by_primary_Key" ),
                           new Output( "/wikirank/nodes_by_primary_Key" ) );

        controller.reduce( Reducer.class,
                           new Input( "shuffle:nodes_by_hashcode" ),
                           new Output( "/wikirank/nodes_by_hashcode" ) );
                           
        controller.map( FlattenLinksJob.Map.class,
                        new Input( "/wikirank/links" ),
                        new Output( "shuffle:default" ) );
                        
        controller.reduce( FlattenLinksJob.Reduce.class,
                           new Input( "shuffle:default" ),
                           new Output( "/wikirank/links.flattened" ) );
        
        // this joins the node table AND the links table and then writes a
        // raw hashcode graph for use with pagerank.
        controller.merge( MergePagesAndLinksJob.Merge.class,
                          new Input( "/wikirank/nodes_by_primary_Key",
                                     "/wikirank/links.flattened" ),
                          new Output( "/wikirank/graph" ) );

    }

    public void transform() throws Exception {

        // the graph is written, now launch a job to finish it up.

        Pagerank pr = new Pagerank( config, "/wikirank/graph", "/wikirank/nodes_by_hashcode", controller );
        pr.exec( false );

    }
    
    public void load() throws Exception {

    }
    
    private int writeNodes( String input ) throws Exception {

        ExtractWriter writer = new ExtractWriter( config, "/wikirank/nodes" );

        WikiPageParser parser = new WikiPageParser( input );

        int wrote = 0;
        int skipped = 0;

        while( true ) {

            WikiPage match = parser.next();

            if ( match == null )
                break;

            if ( match.namespace != NAMESPACE ) {
                ++skipped;
                continue;
            }
            
            StructReader key = StructReaders.hashcode( "" + match.id );
            StructReader value = StructReaders.wrap( match.name );
            
            writer.write( key , value );

            ++wrote;

            if ( (wrote % 10000) == 0 )
                log.info( "Wrote %,d nodes and skipped %,d", wrote, skipped );

        }

        writer.close();

        log.info( "Wrote %,d nodes and skipped %,d", wrote, skipped );
        
        return wrote;
        
    }

    private int writeLinks( String input ) throws Exception {

        ExtractWriter writer = new ExtractWriter( config, "/wikirank/links" );

        WikiLinkParser parser = new WikiLinkParser( input );

        int wrote = 0;
        int skipped = 0;

        while( true ) {

            WikiLink link = parser.next();

            if ( link == null )
                break;

            if ( link.namespace != NAMESPACE ) {
                ++skipped;
                continue;
            }

            //if ( index > LIMIT )
            //    break;
            
            StructReader key = StructReaders.hashcode( "" + link.id );
            StructReader value = StructReaders.wrap( link.name );
            
            writer.write( key , value );

            ++wrote;

            if ( (wrote % 10000) == 0 )
                log.info( "Wrote %,d links and skipped %,d" , wrote, skipped );

        }

        writer.close();

        log.info( "Wrote %,d links and skipped %,d" , wrote, skipped );

        return wrote;
        
    }

}

