
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
    private Output output = null;

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

    public Output getOutput() { 
        return this.output;
    }

    public void setOutput( Output output ) { 
        this.output = output;
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

    protected void setup( BaseMapper mapper ) throws IOException {

        JobOutput[] result;

        if ( output == null || output.getReferences().size() == 0 ) {
        
            shuffleJobOutput = new ShuffleJobOutput( nr_partitions );
            
            result = new JobOutput[] { shuffleJobOutput };

        } else {

            result = JobOutputFactory.getJobOutput( partition, output );
            
        }

        mapper.init( result );

    }

    protected void teardown( BaseMapper mapper ) {
        mapper.cleanup();
    }

}
