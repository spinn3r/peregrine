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
package peregrine.task;

import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.config.*;

/**
 * Represents a split on a partition.
 */
public class PartitionWork implements Work {
    
    protected Partition partition = null;

    public PartitionWork( String data ) {
        this.partition = new Partition( Integer.parseInt( data ) );
    }
    
    public PartitionWork( Partition partition ) {
        this.partition = partition;
    }

    public void setPartition( Partition partition ) { 
        this.partition = partition;
    }

    public Partition getPartition() { 
        return this.partition;
    }

    @Override
    public String toString() {
        return String.format( "%s", partition.getId() );
    }

    @Override
    public boolean equals( Object obj ) {

        if ( obj instanceof PartitionWork ) {
            return partition.getId() == ((PartitionWork)obj).partition.getId();
        }

        return false;
        
    }

    @Override
    public int hashCode() {
        return partition.hashCode();
    }
    
}