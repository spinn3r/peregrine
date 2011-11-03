package peregrine;

import peregrine.map.BaseMapper;

public class Mapper extends BaseMapper {

    public void map( byte[] key, byte[] value ) {
        emit( key, value );
    }

}
