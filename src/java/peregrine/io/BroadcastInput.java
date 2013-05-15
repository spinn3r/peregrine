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
package peregrine.io;

import java.io.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.partition.*;

import com.spinn3r.log5j.Logger;

/**
 * Reader for pulling in broadcast data and exposing the key/value pairs to the
 * consumer.
 */
public final class BroadcastInput {

    private static final Logger log = Logger.getLogger();

	private StructReader key = null;
	private StructReader value = null;

    public BroadcastInput( Config config,
                           Partition part,
                           String path ) throws IOException {
        
        LocalPartitionReader reader = new LocalPartitionReader( config, part, path );

        if ( reader.hasNext() == false ) {
            log.warn( "No broadcast values found at: " + path );
            return;
        }

        reader.next();

        if ( reader.hasNext() )
            throw new IOException( "Too many broadcast values for: " + path );

        this.key   = reader.key();
        this.value = reader.value();

    }
    
    public BroadcastInput( StructReader value ) {
        this.value = value;
    }

    /**
     * Get the reduced broadcast key.  May return null if none were found.
     */
    public StructReader getKey() {
        return key;
    }

    /**
     * Get the reduced broadcast value.  May return null if none were found.
     */
    public StructReader getValue() {
        return value;
    }

    public StructReader getValue( StructReader _default ) {

        if ( value == null )
            return _default;
        
        return value;

    }

}

