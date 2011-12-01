package peregrine.shuffle.receiver;

import java.util.concurrent.*;
import peregrine.shuffle.*;

public class ShuffleReceiverFlushCallable implements Callable {

    private ShuffleOutputWriter writer;
    
    ShuffleReceiverFlushCallable( ShuffleOutputWriter writer ) {

        if ( writer == null )
            throw new NullPointerException( "writer" );
        
        this.writer = writer;
    }

    @Override
    public Object call() throws Exception {

        // close this in a background task since this blocks.
        writer.close();
        
        return null;
        
    }
    
}