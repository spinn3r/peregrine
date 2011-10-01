
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

public class ReducerTask implements Callable {

    private MapOutputIndex mapOutputIndex = null;

    private final Reducer reducer;

    private boolean triggerReducer = false;

    private Output output;
    
    public ReducerTask( MapOutputIndex mapOutputIndex,
                        Class reducer_class,
                        Output output )
        throws ExecutionException {
        
        this.mapOutputIndex = mapOutputIndex;

        this.output = output;

        //FIXME: unify this with the BaseMapperTask
        try { 
            this.reducer = (Reducer)reducer_class.newInstance();
        } catch ( Exception e ) {
            throw new ExecutionException( e );
        }

        this.reducer.setOutput( output );
        
    }

    public Object call() throws Exception {

        //FIXME: this implements the DEFAULT sort everything approach not the
        //hinted pre-sorted approach which in some applications would be MUCH
        //faster for the reduce operation.

        //FIXME: make this WHOLE thing testable externally ... 

        // the first output path will always be required.

        Partition partition = mapOutputIndex.partition;
        
        ReducerOutput[] reducerOutput = new ReducerOutput[ output.getReferences().size() ];

        List<PartitionWriter> openPartitionWriters = new ArrayList();
        
        int idx = 0;
        for( OutputReference ref : output.getReferences() ) {

            String path = ((FileOutputReference)ref).getPath();
            PartitionWriter writer = new PartitionWriter( partition, path );
            reducerOutput[idx++] = new PartitionWriterReducerOutput( writer );

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
        
        this.reducer.cleanup();

        return null;

    }

}

class PartitionWriterReducerOutput implements ReducerOutput {

    protected PartitionWriter writer;
    
    public PartitionWriterReducerOutput( PartitionWriter writer ) {
        this.writer = writer;
    }

    public void emit( byte[] key , byte[] value ) {

        try {

            writer.write( key, value );
            
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        
    }

}

