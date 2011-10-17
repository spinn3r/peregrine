
package peregrine.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.reduce.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;

public abstract class BaseMapperTask extends BaseOutputTask implements Callable {

    protected Host host;
    protected int nr_partitions;
    protected Class mapper_clazz = null;

    protected List<ShuffleJobOutput> shuffleJobOutput = new ArrayList();

    protected List<BroadcastInput> broadcastInput = new ArrayList();

    private Input input = null;

    public void init( Config config, 
                      Membership partitionMembership,
                      Partition partition,
                      Host host,
                      Class mapper_clazz ) {

        super.init( partition );

        this.config         = config;
        this.host           = host;
        this.nr_partitions  = partitionMembership.size();
        this.mapper_clazz   = mapper_clazz;
        
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

    /**
     * Create the mapper backing.
     */
    protected Object newMapper() {

        try {

            return mapper_clazz.newInstance();

        } catch ( Exception e ) {

            // this IS a runtime exeption because we have actually already
            // instantiated the class, we just need another instance to use.

            throw new RuntimeException( e );
            
        }

    }

    @Override
    public void setup() throws IOException {

        if ( output == null || output.getReferences().size() == 0 ) {
        
            setJobOutput( new JobOutput[] { new ShuffleJobOutput( config ) } );

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

        List<LocalPartitionReaderListener> listeners = new ArrayList();

        for( ShuffleJobOutput current : shuffleJobOutput ) {
            
            if ( current instanceof LocalPartitionReaderListener ) {
                listeners.add( (LocalPartitionReaderListener) current );
            }
            
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
