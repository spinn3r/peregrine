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
public class ReplicaWorkReference implements WorkReference<ReplicaWorkReference>, Comparable<ReplicaWorkReference> {
    
    protected Replica replica = null;
    
    public ReplicaWorkReference( Replica replica ) {
        this.replica = replica;
    }

    public void setReplica( Replica replica ) { 
        this.replica = replica;
    }

    public Replica getReplica() { 
        return this.replica;
    }

    @Override
    public String toString() {
        return replica.toString();
    }

    @Override
    public boolean equals( Object obj ) {

        if ( obj instanceof ReplicaWorkReference ) {
            return replica.equals( ((ReplicaWorkReference)obj).replica );
        }

        return false;
        
    }

    @Override
    public int hashCode() {
        return replica.hashCode();
    }
   
    @Override
    public int compareTo( ReplicaWorkReference val ) {
        return replica.compareTo( val.replica );
    }
    
}