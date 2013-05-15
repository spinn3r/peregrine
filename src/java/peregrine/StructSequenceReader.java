/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine;

import java.io.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.io.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * A reader for working with struct sequences.  Supports the standard hasNext()
 * and next() interface from SequenceReader.
 */
public class StructSequenceReader implements SequenceReader {

    private int idx = 0;

    private int size = 0;

    private StructReader reader = null;

    private StructReader key = null;

    private StructReader value = null;
    
    public StructSequenceReader( StructReader reader ) {

        size = reader.readInt();
        
        this.reader = reader;
        
    }

    public boolean hasNext() throws IOException {
        return idx < size;
    }

    public void next() throws IOException {

        ++idx;
        
        key   = read();
        value = read();
        
    }
    
    public StructReader key() throws IOException {
        return key;
    }

    public StructReader value() throws IOException {
        return value;
    }

    private StructReader read() {
        return reader.readStruct( reader.readInt() );
    }
    
    @Override
    public void close() throws IOException {
    }

}