package peregrine.io.util;

import java.io.*;
import java.util.*;

/**
 *
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables.
 *
 * @see <a href='http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html'>try-with-resources</a>
 */
public class Closer extends BaseCloser<Closeable> implements Closeable {

    public Closer() { }

    public Closer( List delegates ) {
        this.delegates = (List<Closeable>)delegates;
    }

    public Closer( Closeable... delegates ) {
        add( delegates );
    }

    @Override
    public void close() throws IOException {
        exec();
    }

    public boolean closed() {
        return executed();
    }

    public boolean isClosed() {
        return executed();
    }

    protected void onDelegate( Closeable delegate ) throws IOException {
        delegate.close();
    }

}

