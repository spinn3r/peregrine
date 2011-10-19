package peregrine.reduce.sorter;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.io.chunk.*;

public interface SorterIntermediate {
    
    public ChunkReader getChunkReader() throws IOException;

    public ChunkWriter getChunkWriter() throws IOException;

}
