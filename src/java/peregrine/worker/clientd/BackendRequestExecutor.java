
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

package peregrine.worker.clientd;

import com.spinn3r.log5j.Logger;
import peregrine.StructReader;
import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.SequenceWriter;
import peregrine.io.chunk.DefaultChunkWriter;
import peregrine.io.partition.LocalPartitionReader;
import peregrine.worker.clientd.requests.BackendRequest;
import peregrine.worker.clientd.requests.ClientBackendRequest;
import peregrine.io.sstable.RecordListener;
import peregrine.io.util.Closer;
import peregrine.util.netty.NonBlockingChannelBufferWritable;
import peregrine.worker.clientd.requests.ScanBackendRequest;

import java.io.IOException;
import java.util.*;

/**
 * Listens to the queue of items submitted by clients and executes them in a
 * background thread.
 *
 */
public class BackendRequestExecutor implements Runnable {

    protected static final Logger log = Logger.getLogger();

    private Config config;

    private BackendRequestQueue queue;

    private PartitionIndex partitionIndex = null;

    // all known clients.
    private Set<ClientBackendRequest> clientIndex = null;

    // all executing scan requests.
    private List<ScanBackendRequest> scanBackendRequests = null;

    public BackendRequestExecutor(Config config, BackendRequestQueue queue) {
        this.config = config;
        this.queue = queue;
    }

    @Override
    public void run() {

        while( true ) {

            // http://mechanical-sympathy.blogspot.com/2011/10/smart-batching.html
            //
            // we have group 'commit' of requests for intelligent/smart
            // batching so that if we come BACK to the queue and it's full with
            // GET or SCAN requests that we can elide them so that blocks only need
            // to be decompressed for ALL inbound requests.

            List<BackendRequest> requests = new ArrayList( BackendRequestQueue.LIMIT );

            queue.drainTo(requests);

            // the list must be sorted before we service it so that we can
            // access keys on the same block without going backwards and
            // accessing the same block again.  A seek is involved with fetching
            // a block as is optionally decompression and that's expensive.  By
            // first sorting the set we avoid both expensive operations.
            Collections.sort( requests );

            // FIXME: make SURE that when we are handling adjacent keys
            // correctly and that when two clients each fetch the same key that
            // we handle that use case.

            handle(requests);

        }

    }

    /**
     * Create a multi-level index of partitions -> files -> keys
     */
    private void createIndex( List<BackendRequest> requests ) {

        partitionIndex = new PartitionIndex();
        clientIndex = new HashSet<ClientBackendRequest>();

        for( BackendRequest current : requests ) {

            ClientBackendRequest clientBackendRequest = current.getClient();

            // keep track of every client...
            clientIndex.add(clientBackendRequest);

            SourceIndex sourceIndex = partitionIndex.fetch(clientBackendRequest.getPartition());
            List<BackendRequest> list = sourceIndex.fetch( clientBackendRequest.getSource());
            list.add( current );

        }

    }

    class PartitionIndex extends FetchIndex<Partition,SourceIndex> {

        @Override
        public SourceIndex newEntry() {
            return new SourceIndex();
        }
    }

    class SourceIndex extends FetchIndex<String,List<BackendRequest>> {

        @Override
        public List<BackendRequest> newEntry() {
            return new ArrayList<BackendRequest>();
        }

    }

    abstract class FetchIndex<K,V> {

        private Map<K,V> delegate = new HashMap();

        /**
         * Fetch the given key from this index, create a new entry if it isn't
         * already present.
         */
        public V fetch( K key ) {

            V entry = delegate.get( key );

            if ( entry == null ) {
                entry = newEntry();
                delegate.put( key, entry );
            }

            return entry;

        }

        public abstract V newEntry();

        public  Set<Map.Entry<K,V>> entries() {
            return delegate.entrySet();
        }

    }

    public void handle( List<BackendRequest> requests ) {

        //FIXME: don't let clients fetch teh same key multiple times.  this is
        //silly but where should this go?  The client?  The server?

        // FIXME: what happens if we have two entries for the same key... we
        // should de-dup them but we have to be careful because two clients
        // could request the SAME key and we need to be careful and return it
        // correctly.  I could write a unit test for this but they would need to
        // come from different requests of course.

        // FIXME:if a key/value are BIGGER than the send buffer then we are
        // fucked and I think we will block?  What happens there? I need to
        // review the NIO netty code to see what it would do but I think it
        // goes into a queue.

        // we now need an index of partitions to scan , and then sources in those
        // partitions to scan and the keys in those files to scan.  Create the
        // index...

        createIndex( requests );

        // we have to first go through each partition, then each source, etc.
        for( Map.Entry<Partition,SourceIndex> partitionEntry : partitionIndex.entries() ) {

            Partition partition = partitionEntry.getKey();
            SourceIndex sourceIndex = partitionEntry.getValue();

            for( Map.Entry<String,List<BackendRequest>> pathEntry : sourceIndex.entries() ) {

                String source = pathEntry.getKey();
                List<BackendRequest> backendRequests = pathEntry.getValue();

                handle( partition, source, backendRequests );

            }

        }

        //FIXME: we can only re-enqueue items that are not suspended.  We do
        //NOT want to attempt to re-enqueue them, seek to the keys, decompress
        //the block, ONLY to find that the channel is not ready for writing and
        //that the client is suspended.  We need to re-enqueue based on whether
        //the writer is ready for writes.

        // now look at the requests and see which ones were NOT completed
        // and re-enqueue them... these are lagged clients so I don't think
        // we necessarily need to put them at the head of the queue.  We need to
        // look at all entries where the client is not CANCELLED and where the
        // entry is not complete

        List<BackendRequest> incomplete = new ArrayList<BackendRequest>();

        for( BackendRequest current : requests ) {

            if ( current.getClient().isCancelled() ) {
                continue;
            }

            if ( System.currentTimeMillis() - current.getClient().getReceived() > config.getNetWriteTimeout() ) {
                // don't re-enqueue this request because the client has lagged
                // for far too long.  Technically there is no need to send the
                // last HTTP chunk because they're lagged and they probably
                // won't receive it anyway.
                continue;
            }

            if ( current.isComplete() == false )
                incomplete.add( current );

        }

        // add them back into the queue.
        queue.add( incomplete );

    }

    /**
     * Handle an individual file no an specific partition.
     */
    public void handle( Partition partition, String source, List<BackendRequest> requests ) {

        try {

            LocalPartitionReader reader = null;

            try {

                //FIXME: replace List<BackendRequests> with the
                //ReadyBackendRequestIterator so that all future uses of this
                //system just magically skip these requests.

                log.info( "Handling %s on %s ", source, partition );

                // FIXME: should we support a table cache here... I don't think
                // it's really necessary.  The main issue is reading and
                // constructing block information and arguably reading from the
                // filesystem dentries.

                reader = new LocalPartitionReader( config, partition, source );

                if ( reader.count() <= 0 ) {
                    // we're done here.  There's nothing in this table.  In
                    // theory this is redundant however, it eliminates bugs
                    // working with an empty SSTable which is an edge case.

                    //FIXME we have to mark ALL the requests in this table as
                    //complete EVEN if there are no requests presents so we
                    //do not attempt to execute them again.
                    //
                    // FIXME: we don't close the channels here!!! which means
                    // that the last chunk isn't written.  probably best to
                    // push this code down and just act like we missed all the
                    // keys.

                    return;
                }

                // create a SequenceWriter for every client so that we have an
                // output channel.

                for( ClientBackendRequest clientBackendRequest : clientIndex ) {

                    NonBlockingChannelBufferWritable writable =
                            new NonBlockingChannelBufferWritable( clientBackendRequest.getChannel() );

                    // FIXME: we need to set a mode here for the DefaultChunkWriter to
                    // include a CRC32 in the minimal form so that the entire record is
                    // checked for checksum.  For starters we need it for the wire
                    // protocol but we ALSO need it to detect if we failed to service
                    // the request.
                    DefaultChunkWriter writer = new DefaultChunkWriter( config , writable );

                    clientBackendRequest.setSequenceWriter( writer );

                }

                reader.seekTo( requests, new RecordListener() {

                    @Override
                    public void onRecord( BackendRequest backendRequest, StructReader key, StructReader value ) {

                        ClientBackendRequest clientBackendRequest = backendRequest.getClient();

                        try {

                            // the client will automatically come back from being
                            // suspended once the buffer is drained.
                            if ( clientBackendRequest.isSuspended() ) {
                                return;
                            }

                            SequenceWriter writer = clientBackendRequest.getSequenceWriter();

                            writer.write( key, value );

                            //FIXME: we DO need a write future here because if
                            // this write suspends the client then we need to
                            // resume it once it comes back and the buffer is
                            // drained.  I think I could do this by setting
                            // suspended=false from this completion and then
                            // when I re-examine the resulting incomplete items
                            // at the end of the executor then either I can add
                            // them directly there or have the write listener do
                            // it. This should probably be called ResumeListener
                            // and a dedicated class.


                            // flush after every key write.  This allows us to serve
                            // requests with lower latency.  These go into the TCP
                            // send buffer immediately and sent ASAP.  Keeping
                            // them around in memory is just pointless
                            writer.flush();

                            // mark this key as complete so that I can move on
                            // to other keys to serve after we execute the batch.
                            // Both a scan AND a fetch may need to be suspended
                            // and when we resume we have to look at which
                            // requests are complete.
                            backendRequest.setComplete(true);

                        } catch ( IOException e ) {

                            // mark this request as failed (cancel it) so that all
                            // future requests are simply skipped for this entry.

                            clientBackendRequest.setCancelled(true);

                            log.error("Could not write to client: ", e);

                        }

                    }

                } );

                // NOTE: the client writer should be closed BEFORE the reader so
                // that we can read all the values.  If we do the reverse we we will
                // segfault.

                for( ClientBackendRequest clientBackendRequest : clientIndex ) {

                    try {

                        SequenceWriter writer = clientBackendRequest.getSequenceWriter();

                        writer.flush();
                        writer.close();

                    } catch ( IOException e ) {
                        log.error( "Unable to handle client: ", e );
                    }

                }

            } finally {
                new Closer( reader ).close();
            }

        } catch ( IOException e ) {

            // we can't notify clients that the request failed because it might
            // be already being served and further they were given an HTTP 200 OK
            // however the CRC of the response will fail.

            log.error( String.format( "Could not handle %s on %s", source, partition ), e );

        }

    }

}
