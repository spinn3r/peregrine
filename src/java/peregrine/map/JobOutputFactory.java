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
import peregrine.io.partition.*;

public class JobOutputFactory {

    public static JobOutput[] getJobOutput( Config config,
                                            Partition partition,
                                            Output output ) throws IOException {

        JobOutput[] jobOutput = new JobOutput[ output.getReferences().size() ];

        int idx = 0;
        for( OutputReference ref : output.getReferences() ) {

            if ( ref instanceof FileOutputReference ) {

                FileOutputReference fileref = (FileOutputReference)ref;

                PartitionWriter writer = new DefaultPartitionWriter( config, partition, fileref.getPath(), fileref.getAppend() );

                jobOutput[idx++] = new PartitionWriterJobOutput( writer );

            } else if ( ref instanceof BroadcastOutputReference ) {

                BroadcastOutputReference bcast = (BroadcastOutputReference) ref;
                
                jobOutput[idx++] = new BroadcastShuffleJobOutput( config, bcast.getName() );
                
            } else {
                //FIXME: right now we only support file output... 
                throw new IOException( "ref not supported: " + ref.getClass().getName() );
            }

        }

        return jobOutput;

    }
    
}
