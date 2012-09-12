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

    private String path = null;
    
    public ExtractFromFlatFormat( Config config, String path ) {
        this.config = config;
        this.path = path;
    }

    /**
     * Init PR ... setup all our vectors, node metadata table, etc.
     */
    public void extract() throws Exception {

        // ***** INIT stage... 

        log.info( "Running extract() on %s", path );

        CorpusParserListener listener = new CorpusParserListener();
        
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

            writeNode( source );

            for( String target : targets ) {
                writeNode( target );
            }

            List<StructReader> outbound = new ArrayList();

            for( String target : targets ) {
                outbound.add( StructReaders.wrap( target ) );
            }

            linksWriter.write( StructReaders.hashcode( source ),
                               StructReaders.wrap( outbound ) );
            
        }

        private void writeNode( String name ) throws Exception {

            StructReader key = StructReaders.hashcode( name);
            StructReader value = StructReaders.wrap( name );

            nodesWriter.write( key, value );
            
        }
        
    }
    
}

