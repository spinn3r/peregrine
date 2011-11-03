
package peregrine.reduce;

public interface SortEntryFactory  {
    
    public SortEntry newSortEntry( byte[] key, byte[] value );

}
