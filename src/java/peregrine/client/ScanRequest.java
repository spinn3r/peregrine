package peregrine.client;

import java.io.*;

import peregrine.*;
import peregrine.io.sstable.ClientRequest;

/**
 * A scan basically represents an interval (in set theory) for requests.  It can
 * either be open on either end, inclusive, or finite.   One can specify a start
 * key and end key and then read all keys in a given range.
 */
public class ScanRequest {

    private Bound start = null;

    private Bound end = null;

    private int limit = 10;

    private ClientRequest client;

    // noarg constructor.  ClientRequest is needed but set it later.
    public ScanRequest() { }

    public ScanRequest(ClientRequest client) {
        this.client = client;
    }

    public void setStart( StructReader key, boolean inclusive ) {
        this.start = new Bound( key, inclusive );
    }

    public Bound getStart() {
        return start;
    }

    public void setEnd( StructReader key, boolean inclusive ) {
        this.end = new Bound( key, inclusive );
    }

    public Bound getEnd() {
        return end;
    }

    /**
     * Specify the limit for this request.  
     */
    public int getLimit() { 
        return this.limit;
    }

    public void setLimit( int limit ) { 
        this.limit = limit;
    }

    public ClientRequest getClient() {
        return client;
    }

    public void setClient(ClientRequest client) {
        this.client = client;
    }

    /**
     * Represents the ends of an interval (either inclusive or exclusive).
     */
    public class Bound {

        private StructReader key;
        private boolean inclusive;

        public Bound( StructReader key, boolean inclusive ) {
            this.key = key;
            this.inclusive = inclusive;
        }

        public StructReader key() {
            return key;
        }

        public boolean isInclusive() {
            return inclusive;
        }
        
    }
    
}