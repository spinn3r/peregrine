package peregrine.shuffle;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.async.*;
import peregrine.io.chunk.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public interface ShuffleInputChunkReader {

    // TODO: this probably isn't portable but I'm not 100% sure... 
    public ShuffleInputReader getShuffleInputReader();
    
    public String toString();

    public int size() throws IOException;

    public boolean hasNext() throws IOException;

    public ShufflePacket getShufflePacket();

    public int keyOffset();

}
