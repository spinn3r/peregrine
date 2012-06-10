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
package peregrine.merge;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.io.util.*;
import peregrine.io.util.*;
import peregrine.reduce.*;
import peregrine.reduce.merger.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * Run a merge by taking to chunk readers and blending them into a merge stream.
 * 
 */
public class MergeRunner extends IdempotentCloser {

    private static final Logger log = Logger.getLogger();

    private MergerPriorityQueue queue;
    private MergeQueueEntry last = null;
    private ChunkMergeComparator comparator = new ChunkMergeComparator();

    private List<SequenceReader> readers;

    public MergeRunner( List<SequenceReader> readers )
        throws IOException {

        if ( readers.size() == 0 )
            throw new IllegalArgumentException( "readers" );
        
        this.readers = readers;
        
        this.queue = new MergerPriorityQueue( readers );
        
    }
    
    public MergedValue next() throws IOException {

        int nr_readers = readers.size();
        
        List<StructReader> joined = newEmptyList( nr_readers );
        
        while( true ) {

        	MergeQueueEntry ref = queue.poll();

            try {

                if ( ref == null && last == null )
                    return null;

                if ( last != null ) {
                    joined.set( last.id, last.value );
                }

                boolean changed = ref == null || ( last != null && comparator.compare( last, ref ) != 0 );
                
                if ( changed ) {
                    
                    MergedValue result = new MergedValue( last.key, joined );
                    joined = newEmptyList( nr_readers );
                    
                    return result;
                    
                }

            } finally {
                last = ref;
            }
            
        }
        
    }

    @Override
    public void doClose() throws IOException {
        new Closer( readers ).close();
    }
    
    private List<StructReader> newEmptyList(int size) {

        List<StructReader> result = new ArrayList( size );

        for( int i = 0; i < size; ++i ) {
            result.add( null );
        }
        
        return result;
        
    }

}
