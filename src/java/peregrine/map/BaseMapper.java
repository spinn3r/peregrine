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
package peregrine.map;

import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.shuffle.sender.*;

public abstract class BaseMapper {

    public int partitions = 0;

    private JobOutput stdout = null;

    private List<BroadcastInput> broadcastInput = new ArrayList();

    public void init( List<JobOutput> output ) {

        if ( output.size() > 0 )
            this.stdout = output.get(0);

        if ( this.stdout instanceof BroadcastJobOutput ) {
            throw new RuntimeException( "Standard out may not be a broadcast reference: " + this.stdout );
        }
        
    }

    public final void emit( StructReader key,
                            StructReader value ) {

        if ( stdout == null )
            throw new RuntimeException( "stdout not defined." );
        
        stdout.emit( key, value );
        
    }

    public void cleanup() { }

    public List<BroadcastInput> getBroadcastInput() { 
        return this.broadcastInput;
    }

    public void setBroadcastInput( List<BroadcastInput> broadcastInput ) { 
        this.broadcastInput = broadcastInput;
    }

}

