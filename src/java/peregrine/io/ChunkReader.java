package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

public interface ChunkReader {

    public boolean hasNext() throws IOException;

    public byte[] key() throws IOException;
    public byte[] value() throws IOException;
    
    public int size() throws IOException;

    public void close() throws IOException;
    
}