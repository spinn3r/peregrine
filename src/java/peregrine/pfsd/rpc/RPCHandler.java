package peregrine.pfsd;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 */
public abstract class RPCHandler {

    private static final Logger log = Logger.getLogger();

    public abstract void handleMessage( FSDaemon daemon, Map<String,List<String>> message )
        throws Exception;
    
}
