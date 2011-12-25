package peregrine.io;

import java.io.*;

import peregrine.*;
import peregrine.values.*;

/**
 * Represents the output stream from a Mapper / Merger job which can write to an
 * emit stream of key/value pairs.
 */
public interface JobOutput extends Closeable, Flushable {

    /**
     * Emit a key / value pair to this job output.
     */
    public void emit( StructReader key , StructReader value );

    /**
     * Close this job output.  Mappers/reducers should NOT call this method but
     * instead leave it up to the task to close any output.
     * 
     */
    @Override
    public void close() throws IOException;
    
}