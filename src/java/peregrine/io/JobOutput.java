package peregrine.io;

import java.io.*;
import peregrine.values.*;

public interface JobOutput {

    public void emit( StructReader key , StructReader value );

    // mappers/reducers should NOT call this method but instead leave it up to
    // the task to close any output.
    public void close() throws IOException;
    
}