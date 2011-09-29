package maprunner.io;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.io.*;

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