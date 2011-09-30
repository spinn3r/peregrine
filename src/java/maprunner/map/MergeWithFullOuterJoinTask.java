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

public class MergeWithFullOuterJoinTask extends BaseMapperTask {
    
    private Merger mapper;

    public Object call() throws Exception {

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