package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

/**
 * Read data from a partition from local storage.
 */
public class LocalPartitionReaderListener {

    /**
     * Called when we find a new chunk to process.
     */
    public void onChunkStart( int local_chunk_id ) { }

    public void onChunkEnd( int local_chunk_id ) { }

}