package peregrine.reduce;

import java.util.*;

public interface SortListener {

    public void onFinalValue( byte[] key, List<byte[]> values );
    
}