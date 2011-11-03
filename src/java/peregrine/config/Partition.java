
package peregrine.config;

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
        return id == ((Partition)obj).id;
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