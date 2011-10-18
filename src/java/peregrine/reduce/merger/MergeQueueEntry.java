
package peregrine.reduce.merger;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.reduce.*;

public class MergeQueueEntry {

    public byte[] key;
    public byte[] value;
    
    protected MergerPriorityQueue queue = null;

    protected ChunkReader reader = null;

}

