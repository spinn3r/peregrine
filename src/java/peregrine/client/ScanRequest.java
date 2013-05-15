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

package peregrine.client;

import peregrine.*;
import peregrine.worker.clientd.requests.ClientBackendRequest;

/**
 * A scan basically represents an interval (in set theory) for requests.  It can
 * either be open on either end, inclusive, or finite.   One can specify a start
 * key and end key and then read all keys in a given range.
 */
public class ScanRequest {

    private Bound start = null;

    private Bound end = null;

    private int limit = 10;

    private ClientRequestMeta clientRequestMeta = new ClientRequestMeta();

    // noarg constructor.  ClientBackendRequest is needed but set it later.
    public ScanRequest() { }

    public void setStart( StructReader key, boolean inclusive ) {
        this.start = new Bound( key, inclusive );
    }

    public void setStart(Bound start) {
        this.start = start;
    }

    public Bound getStart() {
        return start;
    }

    public void setEnd( StructReader key, boolean inclusive ) {
        this.end = new Bound( key, inclusive );
    }

    public void setEnd(Bound end) {
        this.end = end;
    }

    public Bound getEnd() {
        return end;
    }

    /**
     * Specify the limit for this request.  
     */
    public int getLimit() { 
        return this.limit;
    }

    public void setLimit( int limit ) { 
        this.limit = limit;
    }

    public ClientRequestMeta getClientRequestMeta() {
        return clientRequestMeta;
    }

    public void setClientRequestMeta(ClientRequestMeta clientRequestMeta) {
        this.clientRequestMeta = clientRequestMeta;
    }

    protected Bound newBound( StructReader key, boolean inclusive ) {
        return new Bound( key, inclusive );
    }

    /**
     * Represents the ends of an interval (either inclusive or exclusive).
     */
    public class Bound {

        private StructReader key;
        private boolean inclusive;

        public Bound( StructReader key, boolean inclusive ) {
            this.key = key;
            this.inclusive = inclusive;
        }

        public StructReader key() {
            return key;
        }

        public boolean isInclusive() {
            return inclusive;
        }
        
    }
    
}