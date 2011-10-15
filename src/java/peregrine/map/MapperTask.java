
package peregrine.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.io.*;
import peregrine.io.partition.*;

import peregrine.map.*;
import peregrine.shuffle.*;

public class MapperTask extends BaseMapperTask {

    private Mapper mapper;

    public Object call() throws Exception {

        mapper = (Mapper)super.newMapper();

        try {

            setup();
            mapper.setBroadcastInput( getBroadcastInput() );
            mapper.init( getJobOutput() );
            
            doCall();
            
        } finally {
            mapper.cleanup();
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
