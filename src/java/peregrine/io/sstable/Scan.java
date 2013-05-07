package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;

/**
 * A scan basically represents an interval (in set theory) for requests.  It can
 * either be open on either end, inclusive, or finite.
 */
public class Scan {

    private Bound start = null;

    private Bound end = null;

    private int limit = 10;

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