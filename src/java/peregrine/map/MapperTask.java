
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

import peregrine.map.*;
import peregrine.shuffle.*;

public class MapperTask extends BaseMapperTask {

    private Mapper mapper;

    public Object call() throws Exception {

        mapper = (Mapper)super.newMapper();

        try {

            setup();
            mapper.init( getJobOutput() );
            
            doCall();
            
        } finally {
            mapper.cleanup();
            teardown();
        }
        
        return null;
        
    }

    private void doCall() throws Exception {

        LocalPartitionReaderListener listener = new MapperChunkRolloverListener( this );

        if ( getInput().getReferences().size() != 1 ) {
            throw new Exception( "Map jobs must be provided with one input." );
        }

        List<LocalPartitionReader> readers = getLocalPartitionReaders( listener );

        LocalPartitionReader reader = readers.get( 0 );

        while( true ) {

            Tuple t = reader.read();

            if ( t == null )
                break;

            mapper.map( t.key, t.value );

        }

    }

}
