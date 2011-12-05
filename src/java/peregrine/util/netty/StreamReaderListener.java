package peregrine.util.netty;

import org.jboss.netty.buffer.*;

import java.io.*;
import java.util.*;

/**
 * 
 */
public interface StreamReaderListener {

    /**
     * Fires BEFORE we attempt to read N bytes off the stream reader.
     */
    public void onRead( int length );
    
}
