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
package peregrine.reduce;

import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.chunk.*;

import com.spinn3r.log5j.Logger;

/**
 * Take the result of a sort request, and potentially merge values and when that 
 * value is complete emit and write it to disk.
 *
 */
public class SortResult implements Closeable {

    private static final Logger log = Logger.getLogger();

    public int idx = 0;

    public SortEntry last = null;

    private SortListener listener = null;

    private ChunkWriter writer = null;
    
    public SortResult( ChunkWriter writer ) {
    	this.writer = writer;
    }
    
    public SortResult( ChunkWriter writer, SortListener listener ) {
        
        this.writer = writer;
        this.listener = listener;
        
    }

    public void accept( SortEntry entry ) throws IOException {

        FullKeyComparator comparator = new FullKeyComparator();

        if ( last == null || comparator.compare( last.keyAsByteArray, entry.keyAsByteArray ) != 0 ) {

            emit( last );
            last = entry;

        } else {

            // merge the values together ... 
            last.addValues( entry.getValues() );

        }
        
    }

    public void flush() throws IOException {

        if ( last != null ) {
            emit( last );
            last = null;
        }

    }
    
    public void close() throws IOException {
        flush();
    }

    private void emit( SortEntry entry ) throws IOException {

        if ( entry == null ) {
            return;
        }
        
        if ( listener != null ) {
            listener.onFinalValue( entry.key , entry.getValues() );
        }

        if ( writer != null ) {
            writer.write( entry.key, StructReaders.wrap( entry.getValues() ) );
        }

    }
    
}
