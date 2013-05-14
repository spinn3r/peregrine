package peregrine.io.sstable;

import peregrine.client.ScanRequest;

import java.io.IOException;

/**
 * Algorithm that actually executes a Scan against an SSTable.  Decoupling this
 * from the underlying SSTable allows us to execute a query without having to
 * worry about the underlying SSTable format.
*/
public class Scanner {

    private SSTableReader sstable;

    public Scanner(SSTableReader sstable) {
        this.sstable = sstable;
    }

    public void scan( ScanRequest scanRequest, RecordListener listener ) throws IOException {

        ScanBackendRequest scanBackendRequest = new ScanBackendRequest( scanRequest.getClient(), scanRequest );

        // FIXME: we can't use JUST seekTo to jump to the DefaultChunkReader
        // position because the block will be decompressed temporarily during
        // seekTo and THEN we're going to read it again.  We should probably
        // keep at least ONE block in memory at a time to improve performance of
        // scanRequest either that or figure out a way to partially push scanRequest code into
        // the DefaultChunkReader and then have it resume across blocks when
        // changing to a new reader when we roll over.

        // position us to the starting key if necessary.
        if ( scanRequest.getStart() != null ) {

            GetBackendRequest getBackendRequest
                    = new GetBackendRequest( scanRequest.getClient(), scanRequest.getStart().key() );

            // seek to the start and return if we dont' find it.
            if ( sstable.seekTo( getBackendRequest ) == null ) {
                return;
            }

            // if it isn't inclusive skip over it.
            if ( scanRequest.getStart().isInclusive() == false ) {

                if ( sstable.hasNext() ) {
                    sstable.next();
                } else {
                    return;
                }

            }

        } else if ( sstable.hasNext() ) {

            // there is no start key so start at the beginning of the chunk
            // reader.
            sstable.next();

        } else {
            // no start key and this DefaultChunkReader is empty.
            return;
        }

        int found = 0;
        boolean finished = false;

        while( true ) {

            // respect the limit on the number of items to return.
            if ( found >= scanRequest.getLimit() ) {
                return;
            }

            if ( scanRequest.getEnd() != null ) {

                if ( sstable.key().equals( scanRequest.getEnd().key() ) ) {

                    if ( scanRequest.getEnd().isInclusive() == false ) {
                        return;
                    } else {
                        // emit the last key and then return.
                        finished = true;
                    }

                }

            }

            listener.onRecord( scanBackendRequest, sstable.key(), sstable.value() );
            ++found;

            if ( sstable.hasNext() && finished == false ) {
                sstable.next();
            } else {
                return;
            }

        }

    }


}
