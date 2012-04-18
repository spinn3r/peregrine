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
package peregrine.io.util;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

import java.io.*;
import java.util.*;

/**
 *
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables.
 *
 * @see <a href='http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html'>try-with-resources</a>
 */
public class DumpSequenceFile {

    public static void main( String[] args ) throws Exception {

        String path = args[0];

        SequenceReader reader = new DefaultChunkReader( new File( path ) );

        while ( reader.hasNext() ) {
            reader.next();

            StructReader key   = reader.key();
            StructReader value = reader.value();

            System.out.printf( "%s= %s\n", Hex.encode( key.toByteArray() ),
                                          Hex.encode( key.toByteArray() ) );

        }
        
    }
    
}

