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

package peregrine.client;

import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import peregrine.config.Partition;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parse out the partition and source for all client request URLs and keep the
 * information around for metadata usage.
 */
public class ClientRequestMeta {

    private static Pattern PATH_REGEX =
            Pattern.compile( "/([0-9_]+)/client-rpc/(GET|SCAN|MUTATE)" );

    private Partition partition = null;

    private String requestType = null;

    private String source = null;

    //FIXME: migrate ALL this to Connection?

    public boolean parse( String url ) {

        Matcher matcher = PATH_REGEX.matcher( url );

        if ( matcher.find() == false ) {
            return false;
        }

        partition = new Partition( Integer.parseInt( matcher.group( 1 ) ) );
        requestType = matcher.group(2);

        QueryStringDecoder decoder = new QueryStringDecoder( url );

        source = decoder.getParameters().get( "source" ).get( 0 );

        return true;

    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public String getRequestType() {
        return requestType;
    }

    public void setRequestType(String requestType) {
        this.requestType = requestType;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

}
