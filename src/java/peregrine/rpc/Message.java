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
package peregrine.rpc;

import java.io.*;
import java.util.*;

import peregrine.io.*;
import peregrine.util.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.handler.codec.http.*;

/**
 * A message sent between two hosts.
 */
public class Message extends StructMap implements MessageSerializable {

    public Message() { }

    public Message( String data ) {

        if ( data == null ) return;
        
        Map<String,List<String>> decoded
            = new QueryStringDecoder( data ).getParameters();

        for( String key : decoded.keySet() ) {
            put( key , decoded.get( key ).get(0) );
        }

    }

    public Message( Object... args ) {
        for( int i = 0; i < args.length; ++i ) {
            put( args[i].toString(), args[i + 1].toString() );
            ++i;
        }
    }
    
    public Message( Map map ) {
        super( map );
    }
    
    @Override
    public String toString() {
        
        QueryStringEncoder encoder = new QueryStringEncoder( "" );

        for( String key : keys ) {

            Object value = delegate.get( key );

            if( value != null ) {
                encoder.addParam( key, value.toString() );
            }
            
        }

        return encoder.toString();

    }

    /**
     * Print a message which a human could read.
     */
    public String toDebugString() {

        StringBuilder buff = new StringBuilder();
        
        for( String key : keys ) {

            Object value = delegate.get( key );

            buff.append( String.format( "%15s = %s\n", key, value ) );
            
        }

        if ( buff.length() == 0 )
            return "";
        
        return buff.toString().substring( 0, buff.length() - 1 ) ;

    }
    
    public Message getMessage( String key ) {
        return new Message( getString( key ) );
    }
    
    @Override
    public Message toMessage() {
        return this;
    }

    @Override
    public void fromMessage( Message message ) {
        init( message.toMap() );
    }

    public ChannelBuffer toChannelBuffer() {
        return ChannelBuffers.wrappedBuffer( toString().getBytes() );
    }
}
