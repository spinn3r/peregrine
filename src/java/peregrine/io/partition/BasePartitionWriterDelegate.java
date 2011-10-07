package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public abstract class BasePartitionWriterDelegate implements PartitionWriterDelegate {

    protected Partition partition;

    protected Host host;

    protected String path = null;

    public void init( Partition partition,
                      Host host,
                      String path ) throws IOException {

        this.partition = partition;
        this.host = host;
        this.path = path;
        
    }

    public String toString() {
        return path;
    }

}