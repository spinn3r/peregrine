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
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.worker.clientd.requests.BackendRequest;
import peregrine.worker.clientd.requests.ScanBackendRequest;

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
    private int count = 0;

    // the index of data blocks for seekTo and scan support.
    protected TreeMap<StructReader,DataBlockReference> dataBlockLookup = null;

    // the current key we are working with.
    private StructReader key = null;

    // the current value we are working with
    private StructReader value = null;

    // the first key in the stream of keys.  Used for scan operation when a
    // scan isn't given a start key.
    private StructReader firstKey = null;

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

                if ( firstKey == null )
                    firstKey = StructReaders.wrap(db.getFirstKey());

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
     * Get the first key in the stream of key/value pairs.
     */
    @Override
    public StructReader getFirstKey() {
        return firstKey;
    }

    @Override
    public Record seekTo( BackendRequest request ) throws IOException {

        List<BackendRequest> requests = new ArrayList();
        requests.add( request );

        LastRecordListener lastRecordListener = new LastRecordListener( null );

        seekTo( requests, lastRecordListener );

        if ( lastRecordListener.getLastRecord() != null ) {
            return lastRecordListener.getLastRecord();
        }

        return null;
        
    }

    @Override
    public boolean seekTo( List<BackendRequest> requests, RecordListener listener ) throws IOException {

        this.key = null;
        this.value = null;

        // the list must be sorted before we service it so that we can
        // access keys on the same block without going backwards and
        // accessing the same block again.  A seek is involved with fetching
        // a block as is optionally decompression and that's expensive.  By
        // first sorting the set we avoid both expensive operations.
        Collections.sort( requests );

        //FIXME: if we weren't able to find this key in this block it is not
        //going to be in the next block so mark them complete. (at least for GET
        //requests).

        Map<DataBlockReference,List<BackendRequest>> dataBlockLookup = new TreeMap();

        LastRecordListener lastRecordListener = new LastRecordListener( listener );
        
        for( BackendRequest request : requests ) {

            //FIXME skip suspended clients...

            if ( request instanceof ScanBackendRequest ) {

                ScanBackendRequest scanBackendRequest = (ScanBackendRequest)request;

                // We have to set the seek key as the first entry in this SSTable
                // so that we can seekTo it similar to the way GetBackendRequest
                // works.
                if ( scanBackendRequest.getSeekKey() == null ) {
                    StructReader firstKey = getFirstKey();
                    scanBackendRequest.setImplicitStartKey( firstKey );
                }

            }

            StructReader key = request.getSeekKey();

            DataBlockReference ref = findDataBlockReference( key );

            // this key isn't indexed so no need to check.
            if ( ref == null )
                continue;

            List<BackendRequest> requestsForReference = dataBlockLookup.get( ref );

            if ( requestsForReference == null ) {
                requestsForReference = new ArrayList();
                dataBlockLookup.put(ref, requestsForReference);
            }

            requestsForReference.add( request );
            
        }

        for( DataBlockReference ref : dataBlockLookup.keySet() ) {

            chunkReader = ref.reader;

            // set the iterator to ALL chunkReader including the current one and
            // continue forward.
            iterator = chunkReaders.subList( ref.idx, chunkReaders.size() ).iterator();

            // update the chunk ref that we're working with.
            chunkRef = new ChunkReference( partition, ref.idx );

            List<BackendRequest> refKeys = dataBlockLookup.get( ref );

            //FIXME: should this return any entries that are incomplete (and for
            // further processing).  This is going to be needed for scan requests
            // that use more than one block.
            List<BackendRequest> incomplete =
                    ref.reader.seekTo( refKeys, ref.dataBlock, lastRecordListener );

            if ( ref.idx == dataBlockLookup.size() - 1 ) {

                for( BackendRequest request : incomplete ) {
                    request.setComplete(true);
                }

            }

        }

        if ( lastRecordListener.getLastRecord() != null ) {

            Record last = lastRecordListener.getLastRecord();
            
            this.key = last.getKey();
            this.value = last.getValue();

            return true;
            
        } else {
            return false;
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
    class DataBlockReference implements Comparable<DataBlockReference> {

        // keeps a pointer to the chunk this data block is stored 
        protected DefaultChunkReader reader = null;

        // the position in the main chunk reader list so that we can call
        // subList and get a new iterator for this and all subsequent chunks.
        protected int idx = -1;

        // the data block for this reference.  Used so that we can call seekTo
        // within the actual chunk.
        protected DataBlock dataBlock = null;

        // the order doesn't really matter as long as it's deterministic.
        public int compareTo( DataBlockReference db ) {
            return hashCode() - db.hashCode();
        }
        
    }
    
}
