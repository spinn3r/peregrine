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

package peregrine.metrics.impl;

/**
 * Provides a gauge metric.  This would be a value that keeps changing similar
 * to free MB in a system or disk space.
 */
public class GaugeMetric extends Metric {

    public GaugeMetric(String name) {
        super(name);
    }

    private void set( long value ) {
        this.value = value;
    }

}
