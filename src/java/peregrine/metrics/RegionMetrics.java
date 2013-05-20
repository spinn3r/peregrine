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
    private Metric suspendedClients = new CounterMetric( "worker.suspendedClients" );

    private Metric getRequests = new CounterMetric( "region.getRequests" );

    private Metric scanRequests = new CounterMetric( "region.scanRequests" );

    private Metric blocksRead = new CounterMetric( "region.blocksRead" );

}
