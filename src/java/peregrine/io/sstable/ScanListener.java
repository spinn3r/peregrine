package peregrine.io.sstable;

import peregrine.*;

/**
 * Listen to scan requests.
 * 
 */
public interface ScanListener {

    /**
     * Called when a scan finds a new record.
     */
    public void onRecord( StructReader key, StructReader value );
    
}
