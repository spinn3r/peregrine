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

        CharSequence sequence = new FileCharSequence( path );

        //p = Pattern.compile( "\\(([0-9]+),[0-9]+,'([^']+)'[^)]+\\)" );
        p = Pattern.compile( "." );
        m = p.matcher( sequence );
        
    }

    public Page next() throws IOException {

        if ( m.find() ) {
            
            Page page = new Page();

            System.out.printf( "." );
            
            //page.id = Integer.parseInt( m.group( 1 ) );
            //page.name = m.group( 2 ).trim();
            return page;
        }

        return null;

    }

    public class Page {

        public int id = -1;
        public String name = null;
        
    }
    
    public static void main( String[] args ) throws Exception {

        PageParser parser = new PageParser( args[0] );

        while( true ) {

            Page page = parser.next();

            if ( page == null )
                break;

            System.out.printf( "%s=>%s\n", page.id , page.name );
            
        }
        
    }

}

