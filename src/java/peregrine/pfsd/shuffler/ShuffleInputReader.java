package peregrine.pfsd.shuffler;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.async.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public class ShuffleInputReader {

    private static final Logger log = Logger.getLogger();

    // header lookup information for partition and where to start reading.
    private Map<Integer,Integer> lookup = new HashMap();

    /**
     */
    private String path;

    public ShuffleInputReader( String path ) {
        this.path = path;
    }

    public ShufflePacket read( int partition ) throws IOException {

        return null;
        
    }
    
}
