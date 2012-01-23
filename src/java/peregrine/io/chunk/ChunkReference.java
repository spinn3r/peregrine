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
package peregrine.io.chunk;

import peregrine.config.*;
import peregrine.task.*;

/**
 * Read data from a partition from local storage.
 */
public class ChunkReference {

    public int local = -1;

    public WorkReference work = null;

    public String path = null;
    
    public ChunkReference() {}

    public ChunkReference( Partition partition ) {
        this( new PartitionWorkReference( partition ) );
    }
    
    public ChunkReference( WorkReference work ) {
    	this( work, null );
    }
    
    /**
     * Used when generating chunk references for tasks.
     */
    public ChunkReference( WorkReference work, String path ) {
        this.work = work;
        this.path = path;
    }

    /**
     * Increment the chunk reference during a read.  This bumps up the local
     * chunk ID by one and then uses the partition as a prefix to update global.
     */
    public void incr() {
        ++local;
    }

    public String toString() {
        return String.format( "%06d", local );
    }
    
}
