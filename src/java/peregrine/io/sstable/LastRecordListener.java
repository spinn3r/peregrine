package peregrine.io.sstable;

import peregrine.Record;
import peregrine.StructReader;

/**
 * Regular record listener which forwards events to the target record
 * listener but also keeps track of the last record we found.
 */
public class LastRecordListener implements RecordListener {

    private Record lastRecord = null;

    private ClientRequest lastClientRequest = null;

    private RecordListener delegate = null;

    public LastRecordListener( RecordListener delegate ) {
        this.delegate = delegate;
    }

    @Override
    public void onRecord( ClientRequest request, StructReader key, StructReader value ) {

        if ( lastRecord == null )
            lastRecord = new Record();

        lastRecord.setKey( key );
        lastRecord.setValue( value );

        lastClientRequest = request;

        if ( delegate != null )
            delegate.onRecord( request, key, value );

    }

    public Record getLastRecord() {
        return lastRecord;
    }

    public ClientRequest getLastClientRequest() {
        return lastClientRequest;
    }

}