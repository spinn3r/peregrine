package peregrine.io;

import java.io.*;

public interface JobOutput extends Closeable {

    public void emit( byte[] key , byte[] value );

    // mappers/reducers should NOT call this method but instead leave it up to
    // the task to close any output.
    public void close() throws IOException;
    
}