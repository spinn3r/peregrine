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
    private String[] paths;
    private int nr_partitions;
    
    final private JoinMapper mapper;
    
    public FullOuterJoinMapperCallable( Map<Partition,List<Host>> partitionMembership,
                                        Partition partition,
                                        Host host ,
                                        Class mapper_clazz,
                                        String... paths ) {

        this.partition       = partition;
        this.host            = host;
        this.paths           = paths;
        this.nr_partitions   = partitionMembership.size();

        try {

            this.mapper = (JoinMapper)mapper_clazz.newInstance();

        } catch ( Exception e ) {

            // this IS a runtime exeption because we have actually already
            // instantiated the class, we just need another instance to use.

            throw new RuntimeException( e );
            
        }
        
    }

    public Object call() throws Exception {

        mapper.init( nr_partitions );

        System.out.printf( "Running map jobs on host: %s\n", host );

        List<LocalPartitionReader> readers = new ArrayList();
        
        for( String path : paths ) {
            readers.add( new LocalPartitionReader( partition, host, path ) );
        }

        Joiner joiner = new Joiner( readers );

        while( true ) {

            byte[][] value = joiner.next();

            if ( value == null )
                break;

            mapper.map( new byte[0], value );
            
        }

        return null;
        
    }

}