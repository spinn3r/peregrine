package peregrine.io;

import java.io.*;
import peregrine.values.*;

public interface JobOutput extends Closeable, Flushable {

    public void emit( StructReader key , StructReader value );

    /**
     * Close the output.  Mappers/reducers should NOT call this method but
     * instead leave it up to the task to close any output.
     * 
     */
    @Override
    public void close() throws IOException;
    
}