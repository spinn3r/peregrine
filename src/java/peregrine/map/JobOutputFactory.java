package peregrine.map;

import java.io.*;
import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.shuffle.sender.*;

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
                
                jobOutput[idx++] = new BroadcastJobOutput( config, bcast.getName(), partition );

            } else if ( ref instanceof ShuffleOutputReference ) {

                ShuffleOutputReference sref = (ShuffleOutputReference) ref;
                
                jobOutput[idx++] = new ShuffleJobOutput( config, sref.getName(), partition );

            } else {
                throw new IOException( "ref not supported: " + ref.getClass().getName() );
            }

        }

        return jobOutput;

    }
    
}
