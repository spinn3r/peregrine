
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

public abstract class BaseMapperTask implements Callable {

    protected Partition partition;
    protected Host host;
    protected int nr_partitions;
    protected Class mapper_clazz = null;

    protected ShuffleJobOutput shuffleJobOutput;

    private Input input = null;

    public void init( Map<Partition,List<Host>> partitionMembership,
                      Partition partition,
                      Host host ,
                      Class mapper_clazz ) {
        
        this.partition      = partition;
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

    protected void setup( BaseMapper mapper ) {

        shuffleJobOutput = new ShuffleJobOutput( nr_partitions );
        
        JobOutput[] output = new JobOutput[] { shuffleJobOutput };

        mapper.init( output );
    }

    protected void teardown( BaseMapper mapper ) {
        mapper.cleanup();
    }

}

class ShuffleJobOutput implements JobOutput {

    private int partitions = 0;

    protected long global_chunk_id = -1;

    public ShuffleJobOutput( int partitions ) {
        this.partitions = partitions;
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Partition target_partition = Config.route( key, partitions, true );

        MapOutputIndex mapOutputIndex = ShuffleManager.getMapOutputIndex( target_partition );
        
        mapOutputIndex.accept( global_chunk_id, key, value );

    }

    @Override 
    public void close() throws IOException {

    }
    
}

