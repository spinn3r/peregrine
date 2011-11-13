
package peregrine.task;

import java.util.*;
import peregrine.*;
import peregrine.io.partition.*;
import peregrine.map.*;
import com.spinn3r.log5j.*;

public class MapperTask extends BaseMapperTask {

    private static final Logger log = Logger.getLogger();

    private Mapper mapper;

    public Object call() throws Exception {

        mapper = (Mapper)super.newMapper();

        try {

            log.info( "Running %s on %s", delegate, partition );
            
            setup();
            mapper.setBroadcastInput( getBroadcastInput() );
            mapper.init( getJobOutput() );

            try {
                doCall();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            try {
                mapper.cleanup();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            try {
                teardown();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

        } catch ( Throwable t ) { 
            handleFailure( log, t );
        } finally {
            report();
        }
        
        return null;
        
    }

    private void doCall() throws Exception {

        // note a map job with ZERO input files is acceptable.  This would be
        // used for some generator that just emits values on init.
        
        if ( getInput().getReferences().size() > 1 ) {
            throw new Exception( "Map jobs must not have more than one input." );
        }

        List<LocalPartitionReader> readers = getLocalPartitionReaders();

        if ( readers.size() == 0 )
            return;
        
        LocalPartitionReader reader = readers.get( 0 );

        int count = 0;
        
        while( reader.hasNext() ) {
            mapper.map( reader.key(), reader.value() );
            ++count;
        }

        log.info( "Mapped %,d entries on %s on host %s from %s", count, partition, config.getHost(), reader );

    }

}
