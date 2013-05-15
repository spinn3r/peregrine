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

/**
 *
 */
public class ScanBackendRequest extends BackendRequest {

    private ScanRequest scanRequest = null;

    //FIXME: ok this is going to be problematic becuase SCAN goes to the
    //beginning of the table but GET goes to a specific key.  If a SCAN does
    //not have a first key we could read the first key from the SSTable interface
    //I think.
    public ScanBackendRequest(ClientRequest client, ScanRequest scanRequest) {
        super(client, scanRequest.getStart().key());
        this.scanRequest = scanRequest;
    }

    public ScanRequest getScanRequest() {
        return scanRequest;
    }

}
