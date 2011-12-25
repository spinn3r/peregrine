package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import com.spinn3r.log5j.Logger;

public interface ShuffleJobOutputDelegate
    extends JobOutput, LocalPartitionReaderListener, Closeable, Flushable {

    public void emit( int to_partition, StructReader key , StructReader value );

    public long length();

    @Override
    public void flush() throws IOException;

    @Override
    public void close() throws IOException;
    
}
