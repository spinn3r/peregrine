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

package peregrine.metrics;

import peregrine.metrics.impl.CounterMetric;
import peregrine.metrics.impl.Metric;

/**
 * Metrics about a specific region we are hosting.
 */
public class RegionMetrics {

    // clients that have been suspended and not keeping up with content.
    public CounterMetric suspendedClients = new CounterMetric( "worker.suspendedClients" );

    public CounterMetric getRequests = new CounterMetric( "region.getRequests" );

    public CounterMetric scanRequests = new CounterMetric( "region.scanRequests" );

    // the number of blocks read from disk.  Technically we don't read the
    // whole block off disk as we quit once we've read all keys.
    public CounterMetric blocksRead = new CounterMetric( "region.blocksRead" );

    public CounterMetric blocksWritten = new CounterMetric( "region.blocksWritten" );

}
