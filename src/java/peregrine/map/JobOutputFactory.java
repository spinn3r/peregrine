package peregrine.map;

import java.io.*;
import java.util.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.shuffle.sender.*;

public class JobOutputFactory {

    public static List<JobOutput> getJobOutput( Config config,
                                                Partition partition,
                                                Output output ) throws IOException {

        List<JobOutput> result = new ArrayList( output.getReferences().size() );

        for( OutputReference ref : output.getReferences() ) {

            if ( ref instanceof FileOutputReference ) {

                FileOutputReference fileref = (FileOutputReference)ref;

                PartitionWriter writer = new DefaultPartitionWriter( config, partition, fileref.getPath(), fileref.getAppend() );

                result.add( new PartitionWriterJobOutput( writer ) );

            } else if ( ref instanceof BroadcastOutputReference ) {

                BroadcastOutputReference bcast = (BroadcastOutputReference) ref;
                
                result.add( new BroadcastJobOutput( config, bcast.getName(), partition ) );

            } else if ( ref instanceof ShuffleOutputReference ) {

                ShuffleOutputReference sref = (ShuffleOutputReference) ref;
                
                result.add( new ShuffleJobOutput( config, sref.getName(), partition ) );

            } else {
                throw new IOException( "ref not supported: " + ref.getClass().getName() );
            }

        }

        return result;
        
    }
    
}
