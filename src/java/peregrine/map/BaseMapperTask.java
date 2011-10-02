
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

public abstract class BaseMapperTask extends BaseOutputTask implements Callable {

    protected Host host;
    protected int nr_partitions;
    protected Class mapper_clazz = null;

    protected List<ShuffleJobOutput> shuffleJobOutput = new ArrayList();

    private Input input = null;

    public void init( Map<Partition,List<Host>> partitionMembership,
                      Partition partition,
                      Host host ,
                      Class mapper_clazz ) {

        super.init( partition );
        
        this.host           = host;
        this.nr_partitions  = partitionMembership.size();
        this.mapper_clazz   = mapper_clazz;
        
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
        
            setJobOutput( new JobOutput[] { new ShuffleJobOutput() } );

        } else {
            super.setup();
        }

        for ( JobOutput current : jobOutput ) {

            if ( current instanceof ShuffleJobOutput ) {
                shuffleJobOutput.add( (ShuffleJobOutput)current );
            }
            
        }
        
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

}

class MapperChunkRolloverListener implements LocalPartitionReaderListener {

    private BaseMapperTask task = null;
    
    public MapperChunkRolloverListener( BaseMapperTask task ) {
        this.task = task;
    }

    @Override
    public void onChunk( ChunkReference ref ) {
        task.fireOnChunk( ref );
    }

    @Override
    public void onChunkEnd( ChunkReference ref ) {
        task.fireOnChunkEnd( ref );
    }

}