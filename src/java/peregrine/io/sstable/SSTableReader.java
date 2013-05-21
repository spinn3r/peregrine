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
import peregrine.io.SequenceReader;
import peregrine.worker.clientd.requests.BackendRequest;

import java.io.IOException;
import java.util.List;

/**
 *
 * Interface for describing an SSTable.  Both the memtable and disk tables have
 * to implement this interface.
 */
public interface SSTableReader extends SequenceReader {

    /**
     * <p> Seek to a given key (or keys) by using the SSTable index.  For disk
     * based indexes we use the meta block information.  For Memtable we can
     * just seek directly to the key in memory and read it from the internal
     * memory index.
     *
     * <p> The key() and value() method, when we match, must return the
     * <b>last</b> key we found via seekTo().
     */
    public boolean seekTo( List<BackendRequest> requests, RecordListener listener ) throws IOException;

    /**
     * Convenience method for working with a single key.
     */
    public Record seekTo( BackendRequest request ) throws IOException;

    /**
     *
     * @return The first key in this table.
     */
    public StructReader getFirstKey();

    /**
     * Return the count of records in this table.
     */
    public int count();

}