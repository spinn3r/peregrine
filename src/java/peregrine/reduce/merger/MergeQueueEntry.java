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
package peregrine.reduce.merger;

import java.io.*;

import peregrine.*;
import peregrine.io.chunk.*;

public class MergeQueueEntry {
    
	public byte[] keyAsByteArray;
    public StructReader key;
    public StructReader value;

    protected ChunkReader reader = null;

    protected MergeQueueEntry() {}

    public MergeQueueEntry( ChunkReader reader ) throws IOException {

        this( reader.key(), reader.value() );
        
        this.reader = reader;

    }

    public MergeQueueEntry( StructReader key, StructReader value ) {
        setKey( key );
        setValue( value );
    }

    public void setKey( StructReader key ) {
    	this.keyAsByteArray = key.toByteArray();
    	this.key = key;

    }

    public void setValue( StructReader value ) {
        this.value = value;
    }

    public MergeQueueEntry copy() {

        MergeQueueEntry copy = new MergeQueueEntry();

        copy.keyAsByteArray = keyAsByteArray;
        copy.key = key;
        copy.value = value;
        copy.reader = reader;
        
        return copy;
        
    }
    
}

