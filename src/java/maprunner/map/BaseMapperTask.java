
package maprunner.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

public abstract class BaseMapperTask implements Callable {

    protected Partition partition;
    protected Host host;
    protected int nr_partitions;
    protected Class mapper_clazz = null;

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
        mapper.setPartitions( nr_partitions );
        mapper.init();
    }

    protected void teardown( BaseMapper mapper ) {
        mapper.cleanup();
    }

}