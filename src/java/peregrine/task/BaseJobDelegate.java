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

public abstract class BaseJobDelegate implements JobDelegate {
    
    private Config config = null;
    
    private Partition partition = null;

    protected JobOutput stdout = null;

    private List<BroadcastInput> broadcastInput = new ArrayList();
    
    @Override
    public void init( List<JobOutput> output ) {
        this.stdout = output.get(0);
    }

    @Override
    public void emit( StructReader key, StructReader value ) {

        if ( stdout == null )
            throw new RuntimeException( "stdout not defined." );
        
        stdout.emit( key, value );
        
    }
    
    @Override
    public void cleanup() {}
        
    public List<BroadcastInput> getBroadcastInput() { 
        return this.broadcastInput;
    }

    @Override
    public void setBroadcastInput( List<BroadcastInput> broadcastInput ) { 
        this.broadcastInput = broadcastInput;
    }

    public Partition getPartition() { 
        return this.partition;
    }

    public void setPartition( Partition partition ) { 
        this.partition = partition;
    }

    public Config getConfig() { 
        return this.config;
    }

    public void setConfig( Config config ) { 
        this.config = config;
    }

}
