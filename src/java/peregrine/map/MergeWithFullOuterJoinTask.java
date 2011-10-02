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

public class MergeWithFullOuterJoinTask extends BaseMapperTask {
    
    private Merger mapper;

    public Object call() throws Exception {

        //FIXME: this task doesn't surface global_chunk_id so technically this
        //would not work with the distributed version.
        
        this.mapper = (Merger)super.newMapper();
        super.setup( this.mapper );

        System.out.printf( "Running map jobs on host: %s\n", host );

        List<LocalPartitionReader> readers = new ArrayList();

        for( InputReference ref : getInput().getReferences() ) {

            FileInputReference file = (FileInputReference) ref;
            
            readers.add( new LocalPartitionReader( partition, host, file.getPath() ) );
            
        }

        LocalMerger merger = new LocalMerger( readers );

        while( true ) {

            JoinedTuple joined = merger.next();

            if ( joined == null )
                break;
            
            mapper.map( joined.key, joined.values );
            
        }

        super.teardown( mapper );

        return null;
        
    }

}