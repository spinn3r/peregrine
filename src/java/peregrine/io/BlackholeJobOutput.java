package peregrine.io;

import java.io.*;

import peregrine.values.*;

/**
 * @see BlacholeOutputReference
 */
public class BlackholeJobOutput implements JobOutput {

    @Override
    public void emit( StructReader key , StructReader value ) {
        //noop
    }

    @Override
    public void flush() throws IOException {
        //noop
    }

    @Override
    public void close() throws IOException {
        //noop
    }
    
}