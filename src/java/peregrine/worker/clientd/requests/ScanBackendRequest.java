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

        // true if this key is >= the seekKey.
        int cmp = comparator.compare( getSeekKey(), key );

        boolean result = false;

        // if we are on the start key
        if ( cmp == 0 ) {

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

        if ( true ) {

            ++found;

            if ( found >= scanRequest.getLimit() ) {
                setComplete( true );
            }

        }

        return result;

    }

    @Override
    public int size() {
        return getScanRequest().getLimit();
    }

}
