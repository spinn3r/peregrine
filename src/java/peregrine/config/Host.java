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

import peregrine.util.*;
import peregrine.util.primitive.LongBytes;

/**
 * Represents a host running a controller or PFS daemon.
 */
public class Host implements Comparable<Host> {

    public static final int DEFAULT_PORT = 11112;

    protected long id = 0;
    protected String name = null;

    protected int partitionMemberId = 0;

    protected int port;

    protected String ref;
    
    public Host( String name ) {
        this( name, DEFAULT_PORT );
    }

    public Host( String name, int port ) {
        this.name = name;
        this.port = port;
        this.id   = LongBytes.toLong( Hashcode.getHashcode( String.format( "%s:%s", name, port ) ) );
        this.ref  = String.format( "%s:%s", name, port );
    }

    public Host( String name, int partitionMemberId, int port ) {
        this( name, port );
        this.partitionMemberId = partitionMemberId;
    }

    public String getName() {
        return name;
    }

    public long getId() {
        return id;
    }

    public int getPort() {
        return port;
    }
    
    public int getPartitionMemberId() {
        return partitionMemberId;
    }

    @Override
    public boolean equals( Object obj ) {

        if ( obj == null )
            return false;
        
        return id == ((Host)obj).id;
        
    }

    @Override
    public int hashCode() {
        return ref.hashCode();
    } 

    @Override
    public String toString() {
        return ref;
    }

    @Override
    public int compareTo(Host o) {
        return toString().compareTo( o.toString() );
    }
    
    /**
     * Parse a host:port pair and return a new Host.
     */
    public static Host parse( String value ) {

        String[] split = value.split( ":" );

        int port =  DEFAULT_PORT;

        if ( split.length > 1 )
            port = Integer.parseInt( split[1] );
            
        return new Host( split[0], port );
        
    }

}
