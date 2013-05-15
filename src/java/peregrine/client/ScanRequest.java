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
import peregrine.worker.clientd.requests.ClientRequest;

/**
 * A scan basically represents an interval (in set theory) for requests.  It can
 * either be open on either end, inclusive, or finite.   One can specify a start
 * key and end key and then read all keys in a given range.
 */
public class ScanRequest {

    private Bound start = null;

    private Bound end = null;

    private int limit = 10;

    private ClientRequest client;

    // noarg constructor.  ClientRequest is needed but set it later.
    public ScanRequest() { }

    public ScanRequest(ClientRequest client) {
        this.client = client;
    }

    public void setStart( StructReader key, boolean inclusive ) {
        this.start = new Bound( key, inclusive );
    }

    public Bound getStart() {
        return start;
    }

    public void setEnd( StructReader key, boolean inclusive ) {
        this.end = new Bound( key, inclusive );
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

    public ClientRequest getClient() {
        return client;
    }

    public void setClient(ClientRequest client) {
        this.client = client;
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