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

import peregrine.config.Config;

import java.io.IOException;

/**
 * <p> Run a scan request for the given san request.
 */
public class ScanClient extends Client {

    public ScanClient( Config config, Connection connection ) {
        super( config, connection );
    }

    /**
     * Execute the get request.
     */
    public void exec( ScanRequest request ) throws IOException {

        // create an HTTP client and submit the request.
        String url = new ScanRequestURLEncoder().encode(connection, request);

        exec( url );

    }

}