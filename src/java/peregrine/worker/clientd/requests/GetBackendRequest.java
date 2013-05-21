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
import peregrine.sort.StrictStructReaderComparator;

/**
 * Represents a request to the backend to GET a key.  This represents an
 * individual request for an item and not ALL the items in a GET client
 * request which may be numerous.
 */
public class GetBackendRequest extends BackendRequest implements RequestSizeable {

    private StructReader key;

    private StrictStructReaderComparator comparator = new StrictStructReaderComparator();

    public GetBackendRequest(ClientBackendRequest clientBackendRequest, StructReader key) {
        super( clientBackendRequest, key );
        this.key = key;
    }

    public StructReader getKey() {
        return key;
    }

    @Override
    public void visit(StructReader key, StructReader value) {

        int cmp = comparator.compare( this.key, key );

        if ( cmp == 0 ) {
            // we are complete and e found the right key.
            setComplete(true);
            setFound(true);
        } else if ( cmp < 0 ) {
            // we are complete but this key wasn't found
            setComplete(true);
            setFound(false);
        }

    }

    @Override
    public int size() {
        return 1;
    }

}
