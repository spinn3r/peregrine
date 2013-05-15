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
package peregrine.util;

import java.util.*;
import java.text.*;

/**
 * Various utility methods for working with dates.
 */
public class Dates {

    private static TimeZone UTC = TimeZone.getTimeZone("UTC");
    
    public static String toISO8601( long date ) {
        return toISO8601( new Date( date ) );
    }

    public static String toISO8601( Date date ) {

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(UTC);
        return df.format(date);
        
    }

}

