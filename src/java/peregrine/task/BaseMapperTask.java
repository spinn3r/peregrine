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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.config.Membership;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.map.*;
import peregrine.shuffle.sender.*;

public abstract class BaseMapperTask extends BaseTask implements Callable {

    protected Host host;

    protected List<ShuffleJobOutput> shuffleJobOutput = new ArrayList();

    protected List<BroadcastInput> broadcastInput = new ArrayList();

    /**
     * This tasks partition listeners.
     */
    protected List<LocalPartitionReaderListener> listeners = new ArrayList();

    private Input input = null;

    public void init( Config config, 
                      Membership partitionMembership,
                      Partition partition,
                      Host host,
                      Class delegate ) {

        super.init( partition );

        this.config         = config;
        this.host           = host;
        this.delegate       = delegate;
        
    }

    public List<BroadcastInput> getBroadcastInput() { 
        return this.broadcastInput;
    }

    public Input getInput() { 
        return this.input;
    }

    public void setInput( Input input ) { 
        this.input = input;
    }

    @Override
    public void setup() throws IOException {

        if ( output == null || output.getReferences().size() == 0 ) {
        
            setJobOutput( new ArrayList() {{ add( new ShuffleJobOutput( config, partition ) ); }} );

        } else {
            super.setup();
        }

        // now process the job output correctly...
        
        for ( JobOutput current : jobOutput ) {

            if ( current instanceof ShuffleJobOutput ) {
                shuffleJobOutput.add( (ShuffleJobOutput)current );
            }
            
        }

        // setup broadcast input... 

        broadcastInput = BroadcastInputFactory.getBroadcastInput( config, getInput(), partition );

    }

    /**
     * Construct a set of partition readers from the input.
     */
    protected List<LocalPartitionReader> getLocalPartitionReaders()
        throws IOException {

        for( ShuffleJobOutput current : shuffleJobOutput ) {
            listeners.add( current );
        }
        
        List<LocalPartitionReader> readers = new ArrayList();

        for( InputReference ref : getInput().getReferences() ) {

            if ( ref instanceof BroadcastInputReference )
                continue;
            
            FileInputReference file = (FileInputReference) ref;

            readers.add( new LocalPartitionReader( config, partition, file.getPath(), listeners ) );
            
        }

        return readers;
        
    }

}
