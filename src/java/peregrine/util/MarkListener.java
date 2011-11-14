package peregrine.util;

import java.util.*;
import java.util.concurrent.*;

/**
 * Used to listen to a MarkSet or MarkMap for changes.
 */
public interface MarkListener<T> {

    public void updated( T entry, Status status );

    public enum Status {

        MARKED(),
        CLEARED(),
        
    }

}