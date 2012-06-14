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
public class PageParser {

    Pattern p;
    Matcher m;
    
    /**
     * 
     * 
     *
     */
    public PageParser( String path ) throws IOException {

        System.out.printf( "Going to read: %s\n", path );
        
        FileCharSequence sequence = new FileCharSequence( path );
        sequence.setTrace( true );
        
        p = Pattern.compile( "\\(([0-9]+),[0-9]+,'([^']+)'[^)]+\\)"  );
        //p = Pattern.compile( "\\(([0-9]+),[0-9]+,'([^']+)'"  );
        m = p.matcher( sequence );
        
    }

    public Match next() throws IOException {

        System.out.printf( "FIXME next()\n" );
        
        if ( m.find() ) {
            Match match = new Match();
            match.id = Integer.parseInt( m.group( 1 ) );
            match.name = m.group( 2 ).trim();
            return match;
        }

        return null;

    }

    public class Match {

        public int id = -1;
        public String name = null;
        
    }
    
    public static void main( String[] args ) throws Exception {

        PageParser parser = new PageParser( args[0] );

        while( true ) {

            Match match = parser.next();

            if ( match == null )
                break;

            System.out.printf( "%s=>%s\n", match.id , match.name );
            
        }
        
    }

}

