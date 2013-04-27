package peregrine.io.sstable;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.os.*;

/**
 *
 * Interface for describing an SSTable.  Both the memtable and disk tables have
 * to implement this interface.
 */
public interface SSTableReader extends SequenceReader {

    /**
     * Seek to a given key by using the SSTable index.  For disk based indexes
     * we use the meta block information.  For Memtable we can just seek
     * directly to the key in memory.
     */
    public boolean seekTo( StructReader key );

}