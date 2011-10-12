
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
import peregrine.shuffle.*;
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

        broadcastInput = BroadcastInputFactory.getBroadcastInput( config, getInput(), partition, host );

    }

    protected void fireOnChunk( ChunkReference chunkRef ) {

        for( ShuffleJobOutput current : shuffleJobOutput ) {
            current.onChunk( chunkRef );
        }
        
    }

    protected void fireOnChunkEnd( ChunkReference chunkRef ) {

        for( ShuffleJobOutput current : shuffleJobOutput ) {
            current.onChunkEnd( chunkRef );
        }
        
    }

    /**
     * Construct a set of partition readers from the input.
     */
    protected List<LocalPartitionReader> getLocalPartitionReaders( LocalPartitionReaderListener listener )
        throws IOException {

        List<LocalPartitionReaderListener> listeners = new ArrayList();

        listeners.add( listener );

        for( ShuffleJobOutput current : shuffleJobOutput ) {

            System.out.printf( "FIXME found shuffle job output: %s\n", shuffleJobOutput );
            
            if ( current instanceof LocalPartitionReaderListener )
                listeners.add( (LocalPartitionReaderListener) current );
            
        }
        
        List<LocalPartitionReader> readers = new ArrayList();

        for( InputReference ref : getInput().getReferences() ) {

            if ( ref instanceof BroadcastInputReference )
                continue;
            
            FileInputReference file = (FileInputReference) ref;

            readers.add( new LocalPartitionReader( config, partition, host, file.getPath(), listeners ) );
            
        }

        return readers;
        
    }
    
}

/**
 * Used so that tasks can listen to map job progress and broadcast output.
 */
class MapperChunkRolloverListener implements LocalPartitionReaderListener {

    private BaseMapperTask task = null;
    
    public MapperChunkRolloverListener( BaseMapperTask task ) {

        System.out.printf( "FIXME0 in constructor for %s\n", getClass() );
        this.task = task;
    }

    @Override
    public void onChunk( ChunkReference ref ) {
        System.out.printf( "MapperChunkRolloverListener.onChunk\n" );
        task.fireOnChunk( ref );
    }

    @Override
    public void onChunkEnd( ChunkReference ref ) {
        System.out.printf( "MapperChunkRolloverListener.onChunkEnd\n" );
        
        task.fireOnChunkEnd( ref );
    }

}