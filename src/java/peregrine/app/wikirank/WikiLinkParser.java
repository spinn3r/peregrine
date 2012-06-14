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
import java.util.regex.*;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.worker.*;
import peregrine.os.*;

/**
 * Parse out the wikipedia sample data.
 */
public class WikiLinkParser extends BaseParser<WikiLink> {

    /**
     * 
     * 
     *
     */
    public WikiLinkParser( String path ) throws IOException {

        super( path, "\\(([0-9]+),([0-9]+),'([^']+)'\\)" );
        
    }

    @Override
    public WikiLink newInstance( Matcher m ) {

        WikiLink link = new WikiLink();
        link.id = Integer.parseInt( m.group( 1 ) );
        link.namespace = Integer.parseInt( m.group( 2 ) );
        link.name = m.group( 3 ).trim();
        return link;

    }

    public static void main( String[] args ) throws Exception {

        WikiLinkParser parser = new WikiLinkParser( args[0] );

        while( true ) {

            WikiLink link = parser.next();

            if ( link == null )
                break;

            System.out.printf( "%s=>%s\n", link.id , link.name );
            
        }
        
    }

}

