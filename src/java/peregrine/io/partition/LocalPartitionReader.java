/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.io.partition;

import java.io.*;
import java.math.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;
import peregrine.io.sstable.*;
import peregrine.os.*;
import peregrine.rpc.*;
import peregrine.task.*;
import peregrine.sort.*;

/**
 * Read data from a partition from local storage from the given file with a set
 * of chunks.
 */
public class LocalPartitionReader extends BaseJobInput
    implements SSTableReader, SequenceReader, JobInput {

    // the path this file was opened with.
    private String path = null;

    // ALL chunks we have for this reader.
    private List<DefaultChunkReader> chunkReaders = new ArrayList();

    // iterator to handle 
    private Iterator<DefaultChunkReader> iterator = null;

    // the current chunk reader we are on.
    private DefaultChunkReader chunkReader = null;

    // true when we have another record to read
    private boolean hasNext = false;

    // the partition we're reading from.
    private Partition partition;

    // the current chunk we are working with.
    private ChunkReference chunkRef;

    // the count of all records across all chunks for the given file.
    private int count = -1;

    // the index of data blocks for seekTo and scan support.
    protected TreeMap<StructReader,DataBlockReference> dataBlockLookup = null;

    // the current key we are working with.
    private StructReader key = null;

    // the current value we are working with
    private StructReader value = null;
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path ) throws IOException {
        
        this( config, partition, path, new ArrayList() );
        
    }

    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path,
                                 ChunkStreamListener listener ) throws IOException {

        this( config, partition, path, new ArrayList() );

        addListener( listener );
        
    }
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path,
                                 List<ChunkStreamListener> listeners ) throws IOException {

        this.partition = partition;
        this.chunkReaders = LocalPartition.getChunkReaders( config, partition, path );
        this.iterator = chunkReaders.iterator();
        this.path = path;

        this.chunkRef = new ChunkReference( partition, path );

        addListeners( listeners );

        // update the count by looking at all the chunks and returning the number
        // of key/value pairs they contain.
        for( ChunkReader reader : chunkReaders ) {
            count += reader.count();
        }

        dataBlockLookup = buildDataBlockIndex();
        
    }

    private TreeMap<StructReader,DataBlockReference> buildDataBlockIndex() {

        TreeMap<StructReader,DataBlockReference> result = new TreeMap( new StrictStructReaderComparator() );

        // build the index of the ChunkReaders.

        byte[] lastKey = null;
        
        for ( int idx = 0; idx < chunkReaders.size(); ++idx ) {

            DefaultChunkReader current = chunkReaders.get( idx );
            
            for( DataBlock db : current.getDataBlocks() ) {

                DataBlockReference ref = new DataBlockReference();
                ref.reader = current;
                ref.idx = idx;
                ref.dataBlock = db;

                result.put( StructReaders.wrap( db.firstKey ), ref );
            }

            if ( current.getTrailer().getCount() > 0 ) {
                lastKey = current.getFileInfo().getLastKey();
            }
            
        }

        if ( lastKey != null ) {
        
            BigInteger ptr = new BigInteger( lastKey );
            ptr = ptr.add( BigInteger.valueOf( 1 ) );
            
            byte[] pd = ptr.toByteArray();
            
            // NOTE: BigInteger is conservative with padding so we have
            // to add any byte padding back in.
            if ( pd.length < lastKey.length ) {
                
                byte[] tmp = new byte[lastKey.length];
                System.arraycopy( pd, 0, tmp, lastKey.length - pd.length, pd.length );
                pd = tmp;
                
            }
            
            result.put( new StructReader( pd ), null );

        }
            
        return result;
                    
    }

    /**
     * Find the data block reference that could potentially hold the given key
     * so that I can find it within the given DefaultChunkReader.
     */
    protected DataBlockReference findDataBlockReference( StructReader key ) {
        
        Map.Entry<StructReader,DataBlockReference> entry = dataBlockLookup.floorEntry(key);

        if ( entry == null )
            return null;

        return entry.getValue();

    }

    /**
     * Find a given record with a specific key.
     */
    @Override
    public Record seekTo( StructReader key ) throws IOException {

        this.key = null;
        this.value = null;
        
        DataBlockReference ref = findDataBlockReference( key );
        
        if ( ref == null )
            return null;

        chunkReader = ref.reader;

        // set the iterator to ALL chunkReader including the current one and
        // continue forward.
        iterator = chunkReaders.subList( ref.idx, chunkReaders.size() ).iterator();

        // update the chunk ref that we're working with.
        chunkRef = new ChunkReference( partition, ref.idx );
        
        Record result = ref.reader.seekTo( key, ref.dataBlock );

        if ( result != null ) {
            this.key = result.getKey();
            this.value = result.getValue();
        }

        return result;
        
    }

    @Override
    public void scan( Scan scan, ScanListener listener ) throws IOException {

        // position us to the starting key if necessary.
        if ( scan.getStart() != null ) {

            // seek to the start and return if we dont' find it.
            if ( seekTo( scan.getStart().key() ) == null ) {
                return;
            }

            // if it isn't inclusive skip over it.
            if ( scan.getStart().isInclusive() == false ) {

                if ( hasNext() ) {
                    next();
                } else {
                    return;
                }

            }

        } else if ( hasNext() ) {

            // there is no start key so start at the beginning of the chunk
            // reader.
            next();
            
        } else {
            // no start key and this DefaultChunkReader is empty.
            return;
        }

        int found = 0;
        boolean finished = false;
        
        while( true ) {

            // respect the limit on the number of items to return.
            if ( found >= scan.getLimit() ) {
                return;
            }

            if ( scan.getEnd() != null ) {

                if ( key().equals( scan.getEnd().key() ) ) {

                    if ( scan.getEnd().isInclusive() == false ) {
                        return;
                    } else {
                        // emit the last key and then return.
                        finished = true;
                    }
                    
                }
                
            }

            listener.onRecord( key(), value() );
            ++found;

            if ( hasNext() && finished == false ) {
                next();
            } else {
                return;
            } 

        }

    }

    public List<DefaultChunkReader> getDefaultChunkReaders() {
        return chunkReaders;
    }
    
    @Override
    public boolean hasNext() throws IOException {

        if ( chunkReader != null )
            hasNext = chunkReader.hasNext();

        if ( hasNext == false ) {

            fireOnChunkEnd( chunkRef );
            
            if ( iterator.hasNext() ) {

                chunkRef.incr();
                
                chunkReader = iterator.next();

                fireOnChunk( chunkRef );
                
                hasNext = chunkReader.hasNext();

            } else {
                hasNext = false;
            }

        }

        return hasNext;
        
    }

    @Override
    public void next() throws IOException {
       	chunkReader.next();    	

        this.key = chunkReader.key();
        this.value = chunkReader.value();
        
    }
    
    @Override
    public StructReader key() throws IOException {
        return key;
    }

    @Override
    public StructReader value() throws IOException {
        return value;
    }

    @Override
    public void close() throws IOException {

        //TODO: migrate to using IdempotentCloser ...
        
        if ( chunkReader != null ) {
            fireOnChunkEnd( chunkRef );
        }

        new Closer( chunkReaders ).close();
        
    }

    @Override
    public String toString() {

        int offset = 0;

        if ( chunkReader != null )
            offset = chunkReader.index();

        Message message = new Message();

        message.put( "path",       path );
        message.put( "partition",  partition.getId() );
        message.put( "chunkRef",   chunkRef );
        message.put( "offset",     offset );

        return message.toString();

    }

    @Override
    public int count() {
        return count;
    }

    // contains a data block and a chunk reference and all the metadata needed
    // to find a data block and seekTo the position of a key and optionally
    // scan.
    class DataBlockReference {

        // keeps a pointer to the chunk this data block is stored 
        protected DefaultChunkReader reader = null;

        // the position in the main chunk reader list so that we can call
        // subList and get a new iterator for this and all subsequent chunks.
        protected int idx = -1;

        // the data block for this reference.  Used so that we can call seekTo
        // within the actual chunk.
        protected DataBlock dataBlock = null;
        
    }
    
}
