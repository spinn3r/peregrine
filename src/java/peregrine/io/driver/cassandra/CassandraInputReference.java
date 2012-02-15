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

import java.io.*;
import java.util.*;
import java.util.regex.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.*;
import peregrine.io.partition.*;
import peregrine.task.*;

public class CassandraInputReference implements InputReference {

    protected String uri;

    private String port = null;
    
    private String host = null;
    
    private String keyspace = null;
    
    private String columnFamily = null;

    public CassandraInputReference( String uri ) {

        this.uri = uri;

        Matcher m = CassandraIODriver.PATTERN.matcher( uri );

        if ( ! m.find() )
            throw new RuntimeException( "Invalid URI: " + uri );

        this.host          = m.group( 1 );
        this.port          = m.group( 3 );
        this.keyspace      = m.group( 4 );
        this.columnFamily  = m.group( 5 );
        
    }

    @Override
    public String toString() {
        return uri;
    }

    @Override 
    public String getScheme() {
    	return "cassandra";
    }

    public String getColumnFamily() { 
        return this.columnFamily;
    }

    public void setColumnFamily( String columnFamily ) { 
        this.columnFamily = columnFamily;
    }

    public String getKeyspace() { 
        return this.keyspace;
    }

    public void setKeyspace( String keyspace ) { 
        this.keyspace = keyspace;
    }

    public String getHost() { 
        return this.host;
    }

    public void setHost( String host ) { 
        this.host = host;
    }

    public String getPort() { 
        return this.port;
    }

    public void setPort( String port ) { 
        this.port = port;
    }

}
