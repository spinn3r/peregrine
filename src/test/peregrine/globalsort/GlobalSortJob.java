package peregrine.globalsort;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.reduce.*;
import peregrine.io.*;

import com.spinn3r.log5j.*;

/**
 * Map reduce job which computes the partition routing table.
 */
public class GlobalSortJob {

    private static final Logger log = Logger.getLogger();
    
    public static class Map extends Mapper {

        @Override
        public void init( List<JobOutput> output ) {

            super.init( output );
            
            BroadcastInput partitionTable = getBroadcastInput().get( 0 );
            
        }

    }

}

