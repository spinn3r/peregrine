package peregrine.io.driver.cassandra;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.TokenRange;

import org.apache.thrift.TException;

import com.spinn3r.log5j.Logger;

import peregrine.util.*;



/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one ColumnFamily.
 *
 * At minimum, you need to set the CF and predicate (description of columns to extract from each row)
 * in your Hadoop job Configuration.  The ConfigHelper class is provided to make this
 * simple:
 *   ConfigHelper.setColumnFamily
 *   ConfigHelper.setSlicePredicate
 *
 * You can also configure the number of rows per InputSplit with
 *   ConfigHelper.setInputSplitSize
 * This should be "as big as possible, but no bigger."  Each InputSplit is read from Cassandra
 * with multiple get_slice_range queries, and the per-call overhead of get_slice_range is high,
 * so larger split sizes are better -- but if it is too large, you will run out of memory.
 *
 * The default split size is 64k rows.
 */
public class TestCassandra 
{

    private static final Logger log = Logger.getLogger();

    public static final String MAPRED_TASK_ID = "mapred.task.id";
    // The simple fact that we need this is because the old Hadoop API wants us to "write"
    // to the key and value whereas the new asks for it.
    // I choose 8kb as the default max key size (instanciated only once), but you can
    // override it in your jobConf with this setting.
    public static final String CASSANDRA_HADOOP_MAX_KEY_SIZE = "cassandra.hadoop.max_key_size";
    public static final int    CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT = 8192;

    private String keyspace;
    private String cfName;

    private static void validateConfiguration(StructMap conf)
    {
        if (ConfigHelper.getInputKeyspace(conf) == null || ConfigHelper.getInputColumnFamily(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setColumnFamily()");
        }
        if (ConfigHelper.getInputSlicePredicate(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the predicate with setPredicate");
        }
    }

    public List<ColumnFamilySplit> getSplits(StructMap conf) throws IOException
    {

        validateConfiguration(conf);

        // cannonical ranges and nodes holding replicas
        List<TokenRange> masterRangeNodes = getRangeMap(conf);

        keyspace = ConfigHelper.getInputKeyspace(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);

        // cannonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = Executors.newCachedThreadPool();
        List<ColumnFamilySplit> splits = new ArrayList<ColumnFamilySplit>();

        try
        {
            List<Future<List<ColumnFamilySplit>>> splitfutures = new ArrayList<Future<List<ColumnFamilySplit>>>();
            KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(conf);
            IPartitioner partitioner = null;
            Range jobRange = null;
            if (jobKeyRange != null)
            {
                partitioner = ConfigHelper.getPartitioner(conf);
                assert partitioner.preservesOrder() : "ConfigHelper.setInputKeyRange(..) can only be used with a order preserving paritioner";
                assert jobKeyRange.start_key == null : "only start_token supported";
                assert jobKeyRange.end_key == null : "only end_token supported";
                jobRange = new Range(partitioner.getTokenFactory().fromString(jobKeyRange.start_token),
                                     partitioner.getTokenFactory().fromString(jobKeyRange.end_token),
                                     partitioner);
            }

            for (TokenRange range : masterRangeNodes)
            {
                if (jobRange == null)
                {
                    // for each range, pick a live owner and ask it to compute bite-sized splits
                    splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                }
                else
                {
                    Range dhtRange = new Range(partitioner.getTokenFactory().fromString(range.start_token),
                                               partitioner.getTokenFactory().fromString(range.end_token),
                                               partitioner);

                    if (dhtRange.intersects(jobRange))
                    {
                        for (Range intersection: dhtRange.intersectionWith(jobRange))
                        {
                            range.start_token = partitioner.getTokenFactory().toString(intersection.left);
                            range.end_token = partitioner.getTokenFactory().toString(intersection.right);
                            // for each range, pick a live owner and ask it to compute bite-sized splits
                            splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                        }
                    }
                }
            }

            // wait until we have all the results back
            for (Future<List<ColumnFamilySplit>> futureInputSplits : splitfutures)
            {
                try
                {
                    splits.addAll(futureInputSplits.get());
                }
                catch (Exception e)
                {
                    throw new IOException("Could not get input splits", e);
                }
            }
        }
        finally
        {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    /**
     * Gets a token range and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    class SplitCallable implements Callable<List<ColumnFamilySplit>>
    {

        private final TokenRange range;
        private final StructMap conf;

        public SplitCallable(TokenRange tr, StructMap conf)
        {
            this.range = tr;
            this.conf = conf;
        }

        public List<ColumnFamilySplit> call() throws Exception
        {
            ArrayList<ColumnFamilySplit> splits = new ArrayList<ColumnFamilySplit>();
            List<String> tokens = getSubSplits(keyspace, cfName, range, conf);
            assert range.rpc_endpoints.size() == range.endpoints.size() : "rpc_endpoints size must match endpoints size";
            // turn the sub-ranges into InputSplits
            String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);
            // hadoop needs hostname, not ip
            int endpointIndex = 0;
            for (String endpoint: range.rpc_endpoints)
            {
                String endpoint_address = endpoint;
		        if (endpoint_address == null || endpoint_address.equals("0.0.0.0"))
			        endpoint_address = range.endpoints.get(endpointIndex);
		        endpoints[endpointIndex++] = InetAddress.getByName(endpoint_address).getHostName();
            }

            for (int i = 1; i < tokens.size(); i++)
            {
                ColumnFamilySplit split = new ColumnFamilySplit(tokens.get(i - 1), tokens.get(i), endpoints);
                log.debug("adding " + split);
                splits.add(split);
            }
            return splits;
        }
    }

    private List<String> getSubSplits(String keyspace, String cfName, TokenRange range, StructMap conf) throws IOException
    {
        int splitsize = ConfigHelper.getInputSplitSize(conf);
        for (int i = 0; i < range.rpc_endpoints.size(); i++)
        {
            String host = range.rpc_endpoints.get(i);
            
            if (host == null || host.equals("0.0.0.0"))
                host = range.endpoints.get(i);
                        
            try
            {
                Cassandra.Client client = ConfigHelper.createConnection(host, ConfigHelper.getRpcPort(conf), true);
                client.set_keyspace(keyspace);
                return client.describe_splits(cfName, range.start_token, range.end_token, splitsize);
            }
            catch (IOException e)
            {
                log.debug("failed connect to endpoint " + host, e);
            }
            catch (TException e)
            {
                throw new RuntimeException(e);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException(e);
            }
        }
        throw new IOException("failed connecting to all endpoints " + Arrays.asList(range.endpoints));
        
    }


    private List<TokenRange> getRangeMap(StructMap conf) throws IOException
    {
        Cassandra.Client client = ConfigHelper.getClientFromAddressList(conf);

        List<TokenRange> map;
        try
        {
            map = client.describe_ring(ConfigHelper.getInputKeyspace(conf));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        return map;
    }



}
