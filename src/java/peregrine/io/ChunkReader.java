package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

public interface ChunkReader {

    public Tuple read() throws IOException;

    public int size() throws IOException;

    public void close() throws IOException;
    
}