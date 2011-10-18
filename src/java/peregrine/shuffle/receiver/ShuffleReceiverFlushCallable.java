package peregrine.shuffle.receiver;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.async.*;
import peregrine.shuffle.*;

import com.spinn3r.log5j.Logger;

public class ShuffleReceiverFlushCallable implements Callable {

    private ShuffleOutputWriter writer;
    
    ShuffleReceiverFlushCallable( ShuffleOutputWriter writer ) {
        this.writer = writer;
    }

    @Override
    public Object call() throws Exception {

        // close this in a background task since this blocks.
        this.writer.close();
        
        return null;
        
    }
    
}