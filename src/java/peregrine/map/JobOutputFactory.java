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

public class JobOutputFactory {

    public static JobOutput[] getJobOutput( Partition partition,
                                            Output output ) throws IOException {

        JobOutput[] jobOutput = new JobOutput[ output.getReferences().size() ];

        int idx = 0;
        for( OutputReference ref : output.getReferences() ) {

            //FIXME: right now we only support file output... 
            
            String path = ((FileOutputReference)ref).getPath();
            PartitionWriter writer = new PartitionWriter( partition, path );
            jobOutput[idx++] = new PartitionWriterJobOutput( writer );

        }

        return jobOutput;

    }
    
}
