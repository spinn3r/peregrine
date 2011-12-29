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
package peregrine.config;

/**
 * Represents a specific instance / replica of a partition on a given host.
 */
public class Replica implements Comparable<Replica> {
    
    protected Partition partition;

    protected int priority = 0;

    protected Host host;

    public Replica( Host host, Partition partition, int priority ) {
        this.host = host;
        this.partition = partition;
        this.priority = priority;
    }

    public Host getHost() { 
        return this.host;
    }

    public Partition getPartition() { 
        return this.partition;
    }

    public int getPriority() { 
        return this.priority;
    }

    @Override
    public int compareTo( Replica r ) {

        int result = priority - r.priority;

        if ( result != 0 )
            return result;

        result = partition.getId() - r.partition.getId();

        return result;
        
    }
    
    @Override
    public String toString() {
        return String.format( "replica:%s, priority=%s", partition, priority  );
    }

}
