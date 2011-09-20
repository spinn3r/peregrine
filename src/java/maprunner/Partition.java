
package maprunner;

import java.io.*;
import java.util.*;

public class Partition {

    private static int sequence = 0;

    protected int id = 0;
    
    public Partition( int id ) {
        this.id = id;
    }

    public boolean equals( Object obj ) {
        return id == ((Host)obj).id;
    }

    public int hashCode() {
        return id;
    }

    public int getId() {
        return id;
    }

    public String toString() {
        return "partition:" + id ;
    }
    
}