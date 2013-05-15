/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package peregrine.io.sstable;

import peregrine.client.ScanRequest;
import peregrine.worker.clientd.requests.ScanBackendRequest;

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

        // FIXME: migrate to using the backend executor for this code.

        // position us to the starting key if necessary.
        if ( scanRequest.getStart() != null ) {

            peregrine.worker.clientd.requests.GetBackendRequest getBackendRequest
                    = new peregrine.worker.clientd.requests.GetBackendRequest( scanRequest.getClient(), scanRequest.getStart().key() );

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
