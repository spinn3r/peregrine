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
import peregrine.client.GetRequest;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class GetBackendRequestFactory implements BackendRequestFactory {

    private GetRequest request;

    private int size = 0;

    public GetBackendRequestFactory(GetRequest request) {
        this.request = request;
        this.size = request.getKeys().size();
    }

    @Override
    public List<BackendRequest> getBackendRequests(ClientBackendRequest clientBackendRequest) {

        List<BackendRequest> backendRequests
                = new ArrayList<BackendRequest>( request.getKeys().size() );

        for( StructReader key : request.getKeys() ) {

            GetBackendRequest backendRequest
                    = new GetBackendRequest(clientBackendRequest, key );

            backendRequests.add( backendRequest );

        }

        return backendRequests;

    }

    @Override
    public int size() {
        return size;
    }
}
