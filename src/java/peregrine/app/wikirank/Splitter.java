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

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.worker.*;
import peregrine.controller.*;

/**
 * Take large files and break them up into input splits.
 */
public class Splitter {

    public static void main( String[] args ) throws Exception {

        int split_size = 134217728; /* 2^27 ... 100MB splits */
        
        String path = args[0];

        File file = new File( path );

        int length = file.length();
        int offset = 0;

        RandomAccessFile raf = new RandomAccessFile( file, "r" );

        //FIXME: is ( encoded in names?  NO ... it is not.  WHAT about COMMAS?

        List<InputSplit> splits = new ArrayList();
        
        while ( offset < length ) {

            int end = offset + split_size;

            while( true ) {

                if ( (char)raf.read(end - 0) == '(' &&
                     (char)raf.read(end - 1) == ',' &&
                     (char)raf.read(end - 2) == ')' ) {
                    --end;
                    splits.add( new InputSplit( offset, end ) );
                    break;
                }

                --end;

            }

            offset = end;

        }
        
    }

    class InputSplit {

        public int start = 0;
        public int end = 0;

        public InputSplit( int start, int end ) {
            this.start = start;
            this.end = end;
        }

        public String toString() {
            return String.format( "start=%,d , end=%,d" , start, end );
        }
        
    }
    
}