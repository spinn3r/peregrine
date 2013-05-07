package peregrine.io.sstable;

import peregrine.*;

/**
 * Listen to scan requests.
 * 
 */
public interface ScanListener {

    public void onRecord( StructReader key, StructReader value );
    
}
