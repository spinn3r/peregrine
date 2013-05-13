package peregrine.io.sstable;

import peregrine.*;

/**
 * Listen to scan and seekTo requests.
 * 
 */
public interface RecordListener {

    /**
     * Called when a scan or seekTo finds a new record.
     */
    public void onRecord( ClientRequest request, StructReader key, StructReader value );
    
}
