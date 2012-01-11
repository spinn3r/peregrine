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
package peregrine.io;

import java.io.*;

import peregrine.*;
import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.partition.*;

public final class BroadcastInput {

	StructReader value ;

    public BroadcastInput( Config config,
                           Partition part,
                           String path ) throws IOException {
        
        LocalPartitionReader reader = new LocalPartitionReader( config, part, path );

        if ( reader.hasNext() == false )
            throw new IOException( "No broadcast values found at: " + reader );

        reader.next();
        
        StructReader key   = reader.key();
        StructReader value = reader.value();

        if ( reader.hasNext() )
            throw new IOException( "Too many broadcast values for: " + path );

        this.value = value;

    }
    
    public BroadcastInput( StructReader value ) {
        this.value = value;
    }

    public StructReader getValue() {
        return value;
    }
    
}

