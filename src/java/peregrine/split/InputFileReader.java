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
package peregrine.split;

import java.io.*;
import java.util.*;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.worker.*;
import peregrine.controller.*;

/**
 * Facade which we pass to the API so that we can expose a read() method easily
 * without exposing any other internals.
 */
public class InputFileReader {

    private RandomAccessFile raf;

    private int prev;
    
    public InputFileReader( RandomAccessFile raf , int prev ) {
        this.raf = raf;
        this.prev = prev;
    }

    /**
     * Read a character from a position in the file so we can return the end
     * point of a record.
     */
    public char read( int pos ) throws IOException {

        if ( pos <= prev ) {

            throw new IOException( String.format( "Reached previous offset before finding split ( prev=%s vs pos=%s )",
                                                  prev , pos ) );

        }

        raf.seek( pos );
        return (char)raf.read();

    }
    
}