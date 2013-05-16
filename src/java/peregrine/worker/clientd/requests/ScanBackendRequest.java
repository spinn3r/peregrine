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

package peregrine.worker.clientd.requests;

import peregrine.StructReader;
import peregrine.client.ScanRequest;
import peregrine.sort.StrictStructReaderComparator;

/**
 *
 */
public class ScanBackendRequest extends BackendRequest implements RequestSizeable {

    private ScanRequest scanRequest = null;

    private int found = 0;

    private StrictStructReaderComparator comparator = new StrictStructReaderComparator();

    public ScanBackendRequest(ClientBackendRequest client, ScanRequest scanRequest) {
        super(client);

        if ( scanRequest.getStart() != null ) {
            setSeekKey( scanRequest.getStart().key() );
        }

        this.scanRequest = scanRequest;
    }

    public ScanRequest getScanRequest() {
        return scanRequest;
    }

    @Override
    public boolean visit(StructReader key, StructReader value) {

        boolean result = false;

        //once we have found the FIRST key we no longer need to compare
        //the start record any longer.
        if ( found == 0 ) {

            int cmp = comparator.compare( getSeekKey(), key );

            // if we are on the start key
            if ( cmp == 0 ) {

                //FIXME: we call setSeekKey BUT we don't create an actual START ... so we will NPE.

                if ( scanRequest.getStart().isInclusive() ) {
                    result = true;
                } else {
                    result = false;
                }

            } else if ( cmp > 0 ) {
                result = true;
            } else {
                result = false;
            }

        }

        //take into consideration the end key.  If we have gone past the end key
        //or are ON the end key we need to find out.
        if ( scanRequest.getEnd() != null ) {

            int cmp = comparator.compare( getSeekKey(), scanRequest.getEnd().key() );

            if ( cmp == 0 ) {
                setComplete(true);
                result = scanRequest.getEnd().isInclusive();
            } else if ( cmp > 0 ) {
                setComplete(true);
                result = false;
            }

        }

        if ( result ) {

            ++found;

            if ( found >= scanRequest.getLimit() ) {
                setComplete( true );
            }

        }

        return result;

    }

    /**
     * Technically the Scan doesn't need to use a start key because we look at
     * a table as a sequence of keys but from an implementation perspective we DO
     * need an actual seek key AND start key.
     */
    public void setImplicitStartKey( StructReader key ) {

        setSeekKey( key );

        if ( scanRequest.getStart() == null ) {
            scanRequest.setStart( key, true );
        }

    }

    @Override
    public int size() {
        return getScanRequest().getLimit();
    }

}
