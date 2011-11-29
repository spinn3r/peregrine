package peregrine;

import peregrine.map.BaseMapper;
import peregrine.values.*;

public class Mapper extends BaseMapper {

    public void map( StructReader key, StructReader value ) {
        emit( key, value );
    }

}
