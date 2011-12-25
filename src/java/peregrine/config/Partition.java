
package peregrine.config;

/**
 * Represents a partition by identifier.
 */
public class Partition {

    protected int id = 0;
    
    public Partition( int id ) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public boolean equals( Object obj ) {
    	    	
    	if ( obj != null && obj instanceof Partition )
    	    return id == ((Partition)obj).id;
        
    	return false;
    	
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return String.format( "partition:%08d", id  );
    }
    
}