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

public class FullOuterJoinMapperCallable implements Callable {

    private Partition partition;
    private Host host;
    private String path;
    private int nr_partitions;
    
    final private Mapper mapper;
    
    public FullOuterJoinMapperCallable( Map<Partition,List<Host>> partitionMembership,
                                        Partition partition,
                                        Host host ,
                                        Class mapper_clazz,
                                        String... paths ) {

        this.partition      = partition;
        this.host           = host;
        this.path           = path;
        this.nr_partitions  = partitionMembership.size();

        try {

            this.mapper = (Mapper)mapper_clazz.newInstance();

        } catch ( Exception e ) {

            // this IS a runtime exeption because we have actually already
            // instantiated the class, we just need another instance to use.

            throw new RuntimeException( e );
            
        }
        
    }

    public Object call() throws Exception {

        mapper.init( nr_partitions );

        System.out.printf( "Running map jobs on host: %s\n", host );

        LocalPartitionReader reader = new LocalPartitionReader( partition, host, path );

        for( Tuple t = reader.read(); t != null ; ) {

        }
        
        return null;
        
    }

}