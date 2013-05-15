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

package peregrine.io.sstable;

import peregrine.Record;
import peregrine.StructReader;

/**
 * Regular record listener which forwards events to the target record
 * listener but also keeps track of the last record we found.
 */
public class LastRecordListener implements RecordListener {

    private Record lastRecord = null;

    private ClientRequest lastClientRequest = null;

    private RecordListener delegate = null;

    public LastRecordListener( RecordListener delegate ) {
        this.delegate = delegate;
    }

    @Override
    public void onRecord( BackendRequest request, StructReader key, StructReader value ) {

        if ( lastRecord == null )
            lastRecord = new Record();

        lastRecord.setKey( key );
        lastRecord.setValue( value );

        lastClientRequest = request.getClient();

        if ( delegate != null )
            delegate.onRecord( request, key, value );

    }

    public Record getLastRecord() {
        return lastRecord;
    }

    public ClientRequest getLastClientRequest() {
        return lastClientRequest;
    }

}