package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;

public interface JobOutput {

    public void emit( byte[] key , byte[] value );

    // mappers/reducers should NOT call this method but instead leave it up to
    // the task to close any output.
    public void close() throws IOException;
    
}