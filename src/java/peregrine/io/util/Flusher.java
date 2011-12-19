package peregrine.io.util;

import java.io.*;
import java.util.*;

/**
 * Flusher similar to {@link Closer} which calls {@ #flush}.
 */
public class Flusher extends BaseCloser<Flushable> implements Flushable {

    public Flusher() { }

    public Flusher( List<Flushable> delegates ) {
        this.delegates = delegates;
    }
    
    public Flusher( Flushable... delegates ) {
        add( delegates );
    }

    @Override
    public void flush() throws IOException {
        exec();
    }

    public boolean isFlushed() {
        return executed();
    }

    protected void onDelegate( Flushable delegate ) throws IOException {
        delegate.flush();
    }

}

