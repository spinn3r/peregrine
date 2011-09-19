
package maprunner;

import java.io.*;
import java.util.*;

public class Host {

    private static int sequence = 0;

    protected int id = 0;
    protected String name = null;
    
    public Host( String name ) {
        this.name = name;
        this.id = sequence++;
    }

    public boolean equals( Object obj ) {
        return id == ((Host)obj).id;
    }

    public int hashCode() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public String toString() {
        return String.format( "%s:%s", name, id );
    }
    
}