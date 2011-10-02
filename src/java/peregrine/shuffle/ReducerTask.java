
package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;

public class ReducerTask implements Callable {

    private MapOutputIndex mapOutputIndex = null;

    private final Reducer reducer;

    private Output output;
    
    public ReducerTask( MapOutputIndex mapOutputIndex,
                        Class reducer_class,
                        Output output )
        throws Exception {
        
        this.mapOutputIndex = mapOutputIndex;

        this.output = output;

        this.reducer = (Reducer)reducer_class.newInstance();

    }

    public Object call() throws Exception {

        try {
        
            //FIXME: this implements the DEFAULT sort everything approach not the
            //hinted pre-sorted approach which in some applications would be MUCH
            //faster for the reduce operation.

            Partition partition = mapOutputIndex.partition;
            
            JobOutput[] reducerOutput = new JobOutput[ output.getReferences().size() ];

            List<PartitionWriter> openPartitionWriters = new ArrayList();
            
            int idx = 0;
            for( OutputReference ref : output.getReferences() ) {

                //FIXME: right now we only support file output... 
                
                String path = ((FileOutputReference)ref).getPath();
                PartitionWriter writer = new PartitionWriter( partition, path );
                reducerOutput[idx++] = new PartitionWriterJobOutput( writer );

                openPartitionWriters.add( writer );
                
            }
            
            this.reducer.init( reducerOutput );

            final AtomicInteger nr_tuples = new AtomicInteger();

            SortListener listener = new SortListener() {
        
                    public void onFinalValue( byte[] key, List<byte[]> values ) {

                        try {
                            reducer.reduce( key, values );
                            nr_tuples.getAndIncrement();

                        } catch ( Exception e ) {
                            throw new RuntimeException( "Reduce failed: " , e );
                        }
                            
                    }
                    
                };
            
            LocalReducer reducer = new LocalReducer( listener );
            
            Collection<MapOutputBuffer> mapOutputBuffers = mapOutputIndex.getMapOutput();
            
            for ( MapOutputBuffer mapOutputBuffer : mapOutputBuffers ) {
                reducer.add( mapOutputBuffer.getChunkReader() );
            }

            reducer.sort();

            System.out.printf( "Sorted %,d entries for partition %s \n", nr_tuples.get() , mapOutputIndex.partition );

            // we have to close ALL of our output streams now.

            for( PartitionWriter opened : openPartitionWriters ) {
                opened.close();
            }

        } finally {
            
            this.reducer.cleanup();

        }

        return null;

    }

}

class PartitionWriterJobOutput implements JobOutput {

    protected PartitionWriter writer;
    
    public PartitionWriterJobOutput( PartitionWriter writer ) {
        this.writer = writer;
    }

    public void emit( byte[] key , byte[] value ) {

        try {

            writer.write( key, value );
            
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        
    }

    public void close() throws IOException {
        writer.close();
    }
    
}

