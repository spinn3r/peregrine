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

import peregrine.client.ScanRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * Create SCAN backend requests.
 */
public class ScanBackendRequestFactory implements BackendRequestFactory {

    private ScanRequest scanRequest;

    private int size = 0;

    public ScanBackendRequestFactory(ScanRequest scanRequest) {
        this.scanRequest = scanRequest;
        this.size = scanRequest.getLimit();
    }

    @Override
    public List<BackendRequest> getBackendRequests(ClientBackendRequest clientBackendRequest) {

        ScanBackendRequest scanBackendRequest = new ScanBackendRequest(clientBackendRequest, scanRequest );

        List<BackendRequest> backendRequests
                = new ArrayList<BackendRequest>( 1 );

        backendRequests.add( scanBackendRequest );

        return backendRequests;

    }

    @Override
    public int size() {
        return size;
    }
}
