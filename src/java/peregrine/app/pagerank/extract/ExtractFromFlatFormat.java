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
package peregrine.app.pagerank.extract;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.config.Config;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.app.pagerank.*;
import peregrine.util.*;

import com.spinn3r.log5j.Logger;

/**
 * Extract data into Peregrine for use with Pagerank.  We export from a flat
 * format file and into the data that Pagerank expects.
 *
 * This also involved two map reduces to elide and sort the data correctly.
 */
public class ExtractFromFlatFormat {
    
    private static final Logger log = Logger.getLogger();

    private Config config = null;

    private Controller controller = null;
    
    private String path = null;

    private int written = 0;
    
    public ExtractFromFlatFormat( Config config, Controller controller, String path ) {
        this.config = config;
        this.controller = controller;
        this.path = path;
    }

    /**
     * Init PR ... setup all our vectors, node metadata table, etc.
     */
    public void extract() throws Exception {

        // ***** INIT stage... 

        log.info( "Running extract() on %s", path );

        CorpusParserListener listener = new CorpusParserListener();

        CorpusExtracter extracter = new CorpusExtracter( path, listener );

        extracter.exec();

        listener.close();
        
        System.out.printf( "Wrote %,d entries\n", written );

        controller.map( Mapper.class,
                        new Input( "/pr/corpus-nodes" ),
                        new Output( "shuffle:default" ) );

        controller.reduce( UniqueNodeJob.Reduce.class,
                           new Input( "shuffle:default" ),
                           new Output( "/pr/nodes_by_hashcode" ) );

        controller.map( Mapper.class,
                        new Input( "/pr/corpus-links" ),
                        new Output( "shuffle:default" ) );

        controller.reduce( Reducer.class,
                           new Input( "shuffle:default" ),
                           new Output( "/pr/graph" ) );
                           
    }

    /**
     * Write out the corpus that we can use to reduce with... 
     */  
    class CorpusParserListener implements ParserListener {

        ExtractWriter linksWriter = null;
        ExtractWriter nodesWriter = null;

        public CorpusParserListener() throws Exception {
            
            this.linksWriter = new ExtractWriter( config, "/pr/corpus-links" );
            this.nodesWriter = new ExtractWriter( config, "/pr/corpus-nodes" );

        }

        @Override
        public void onEntry( String source, List<String> targets ) throws Exception {

            if ( targets.size() == 0 )
                return;
            
            writeNode( source );

            for( String target : targets ) {
                writeNode( target );
            }

            linksWriter.write( StructReaders.hashcode( source ), StructReaders.hashcode( Strings.toArray( targets ) ) );

            ++written;
            
        }

        public void close() throws IOException {
            linksWriter.close();
            nodesWriter.close();                
        }
        
        private void writeNode( String name ) throws Exception {

            StructReader key = StructReaders.hashcode( name);
            StructReader value = StructReaders.wrap( name );

            nodesWriter.write( key, value );
            
        }
        
    }
    
}

