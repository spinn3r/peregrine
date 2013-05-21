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

package peregrine.io.table;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.partition.LocalPartitionReader;
import peregrine.io.sstable.SSTableReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A cache of tables which opens and maintains a table for the duration of
 * pending requests.  We then transparently close tables that are no longer
 * being referenced.
 */
public class TableCache {

    private Config config;

    private Map<TableKey,SSTableReader> delegate = new HashMap<TableKey, SSTableReader>();

    public TableCache(Config config) {
        this.config = config;
    }

    public SSTableReader getTable( Partition partition, String source ) throws IOException {

        TableKey key = new TableKey( partition, source );
        SSTableReader result = delegate.get( key );

        if ( result == null ) {
            result = newTable(partition, source);
            delegate.put( key, result );
        }

        return result;

    }

    private SSTableReader newTable( Partition partition, String source ) throws IOException {
        return new LocalPartitionReader( config, partition, source );
    }

    class TableKey {

        protected Partition partition;

        protected String source;

        TableKey(Partition partition, String source) {
            this.partition = partition;
            this.source = source;
        }

        public boolean equals( Object obj ) {

            TableKey tk = (TableKey)obj;

            return partition.getId() == tk.partition.getId() &&
                   source.equals( tk.source );

        }

    }

}
