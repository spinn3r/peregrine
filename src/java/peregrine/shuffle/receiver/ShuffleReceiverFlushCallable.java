package peregrine.shuffle.receiver;

import java.util.concurrent.*;
import peregrine.shuffle.*;

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