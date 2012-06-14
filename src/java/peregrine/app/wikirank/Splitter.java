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
import java.util.*;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.worker.*;
import peregrine.controller.*;

/**
 * Take large files and break them up into input splits.
 */
public class Splitter {

    private int split_size = 134217728; /* 2^27 ... 100MB splits */

    private File file = null;

    private RandomAccessFile raf = null;
    
    /**
     * 
     * 
     *
     */
    public Splitter( String path ) throws IOException {

        this.file = new File( path );
        this.raf = new RandomAccessFile( file, "r" );

        long length = file.length();
        long offset = 0;

        //FIXME: is ( encoded in names?  NO ... it is not.  WHAT about COMMAS?

        List<InputSplit> splits = new ArrayList();
        
        while ( offset < length ) {

            long end = offset + split_size;

            while( true ) {

                if ( end <= offset )
                    throw new IOException( String.format( "Reached previous offset before finding split ( end=%s vs offset=%s )",
                                                          end , offset ) );
                
                if ( (char)read(end - 0) == '(' &&
                     (char)read(end - 1) == ',' &&
                     (char)read(end - 2) == ')' ) {
                    
                    --end;
                    splits.add( new InputSplit( offset, end ) );
                    break;
                    
                }

                --end;

            }

            offset = end;

        }

        for ( InputSplit split : splits ) {
            System.out.printf( "%s\n", split );
        }
        
    }

    private char read( long pos ) throws IOException {
        raf.seek( pos );
        return (char)raf.read();
    }
    
    public static void main( String[] args ) throws Exception {

        String path = args[0];
        new Splitter( path );
        
    }

    class InputSplit {

        public long start = 0;
        public long end = 0;

        public InputSplit( long start, long end ) {
            this.start = start;
            this.end = end;
        }

        public String toString() {
            return String.format( "start=%,d , end=%,d" , start, end );
        }
        
    }
    
}