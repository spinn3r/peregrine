package peregrine.io.partition;

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

public class PartitionWriterJobOutput implements JobOutput {

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
