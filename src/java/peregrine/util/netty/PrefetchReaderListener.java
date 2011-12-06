package peregrine.util.netty;

import java.io.*;
import java.util.*;

import peregrine.config.*;
import peregrine.os.*;
import peregrine.util.*;

/**
 * 
 */
public interface PrefetchReaderListener {

    public void onCacheExhausted();
    
}
