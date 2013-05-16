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

/**
 *
 * A request for a set of keys or a scan request against the backend database from
 * the client.  This is provided as it allows us to keep track of whether request
 * was sent to the client (complete) and to get metadata about the client request.
 */
public abstract class BackendRequest implements Comparable<BackendRequest>, RequestSizeable {

    private ClientBackendRequest client = null;

    private boolean complete = false;

    private StructReader seekKey;

    protected BackendRequest(ClientBackendRequest client) {
        this.client = client;
    }

    protected BackendRequest(ClientBackendRequest client, StructReader seekKey ) {
        this.client = client;
        this.seekKey = seekKey;
    }

    public ClientBackendRequest getClient() {
        return client;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    /**
     *  Visit the given key/value pair and emit it if we return true.  We also
     *  may need to set this request to complete if this serves the request.
     *  For reading and individual key this is complete if the key request
     *  matches the key we are serving.  For scan requests we have to keep
     *  scanning until we hit the limit.
     *
     *  @return true if the key serves our request and we should emit the value.
     */
    public abstract boolean visit( StructReader key, StructReader value );

    /**
     * The seekKey represents the entry that we're looking for with this
     * backend request.  For GET requests it's just the key for that entry.
     * For SCAN it's a bit different because we need to find that entry and
     * then change the seekKey if we suspend the request and then resume.
     */
    public StructReader getSeekKey() {
        return seekKey;
    }

    public void setSeekKey(StructReader seekKey) {
        this.seekKey = seekKey;
    }

    @Override
    public int compareTo(BackendRequest backendRequest) {
        return getSeekKey().compareTo( backendRequest.getSeekKey() );
    }

}
