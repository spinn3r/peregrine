package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * On every chunk write in partition writer, broadcast the block ID, nr of
 * blocks written, and nr of entries.  This will require about 10 bytes of
 * storage per partition per chunk (which really isn't much).
 *
 * On a 1TB file with 100MB chunks this will require an additional 12k of
 * storage which is totally reasonable.  It can ALSO be reduced in the future if
 * we want.
 */
public class Stat {

    public long count;
    
    public Stat( long count ) {
        this.count = count;
    }

}

