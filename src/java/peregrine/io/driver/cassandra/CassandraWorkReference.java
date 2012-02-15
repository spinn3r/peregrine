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
package peregrine.io.driver.cassandra;

import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.config.*;
import peregrine.task.*;

/**
 * Represents a split on a partition.
 */
public class CassandraWorkReference implements WorkReference<CassandraWorkReference>, Comparable<CassandraWorkReference> {

    protected String startToken;
    protected String endToken;
    protected String[] dataNodes;

    public CassandraWorkReference( String startToken, String endToken, String[] dataNodes ) {
        this.startToken = startToken;
        this.endToken   = endToken;
        this.dataNodes  = dataNodes;
    }

    public CassandraWorkReference( String uri ) {

        String[] split = uri.split( ":" );

        this.startToken = split[0];
        this.endToken   = split[1];
        this.dataNodes  = split[2].split( "," );
        
    }

    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "%s:%s:", startToken, endToken ) );

        for( int i = 0; i < dataNodes.length; ++i ) {

            if ( i > 0 )
                buff.append( "," );

            buff.append( dataNodes[i] );

        }
        
        return buff.toString();
    }

    @Override
    public boolean equals( Object obj ) {

        return toString().equals( obj.toString() );
        
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public int compareTo( CassandraWorkReference p ) {
        return toString().compareTo( p.toString() );
    }

}