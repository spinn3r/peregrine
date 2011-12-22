package peregrine.io;

import java.io.*;

/**
 * @see BlacholeOutputReference
 */
public class BlackholeJobOutput implements JobOutput {

    @Override
    public void emit( byte[] key , byte[] value ) {
        //noop
    }

    @Override
    public void close() throws IOException {
        //noop
    }
    
}