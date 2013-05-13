package peregrine.io.sstable;

import peregrine.Record;
import peregrine.StructReader;

/**
 * Regular record listener which forwards events to the target record
 * listener but also keeps track of the last record we found.
 */
public class LastRecordListener implements RecordListener {

    private Record last = null;

    private RecordListener delegate = null;

    public LastRecordListener( RecordListener delegate ) {
        this.delegate = delegate;
    }

    @Override
    public void onRecord( StructReader key, StructReader value ) {

        if ( last == null )
            last = new Record();

        last.setKey( key );
        last.setValue( value );

        if ( delegate != null )
            delegate.onRecord( key, value );

    }

    public Record getLast() {
        return last;
    }

}