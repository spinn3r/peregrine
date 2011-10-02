package maprunner.io;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

/**
 * On every chunk write in partition writer, broadcast the block ID, nr of
 * blocks written, and nr of entries.  This will require about 10 bytes of
 * storage per partition per chunk (which really isn't much).
 *
 * On a 1TB file with 100MB chunks this will require an additional 12k of
 * storage which is totally reasonable.  It can ALSO be reduced in the future if
 * we want.
 */
class StatWriter {

    public StatWriter( String path ) {

    }

}

