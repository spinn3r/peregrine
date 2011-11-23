package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import com.spinn3r.log5j.Logger;

public interface ShuffleJobOutputDelegate extends JobOutput, LocalPartitionReaderListener {

    public void emit( int to_partition, byte[] key , byte[] value );

    public long length();
    
}
