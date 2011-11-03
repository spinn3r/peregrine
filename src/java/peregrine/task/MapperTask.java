
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

            setup();
            mapper.setBroadcastInput( getBroadcastInput() );
            mapper.init( getJobOutput() );

            try {
                doCall();
            } finally {
                mapper.cleanup();
            }

            setStatus( TaskStatus.COMPLETE );

        } catch ( Throwable t ) { 

            log.error( "Task failed for partition: " + partition, t );

            setStatus( TaskStatus.FAILED );
            setCause( t );

        } finally {
            teardown();
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

        while( reader.hasNext() ) {
            mapper.map( reader.key(), reader.value() );
        }

    }

}
