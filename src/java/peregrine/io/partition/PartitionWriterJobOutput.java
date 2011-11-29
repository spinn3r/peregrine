package peregrine.io.partition;

import java.io.*;
import peregrine.values.*;
import peregrine.io.*;

public class PartitionWriterJobOutput implements JobOutput {

    protected PartitionWriter writer;
    
    public PartitionWriterJobOutput( PartitionWriter writer ) {
        this.writer = writer;
    }

    @Override
    public void emit( StructReader key , StructReader value ) {

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
