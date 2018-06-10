package com.timeyang.athena.utill.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author yangck
 */
@FunctionalInterface
public interface RowMapper<T> {
    /**
     * @param rs     ResultSet
     * @param rowNum Support for the getRow method is optional for ResultSets with a result set type of TYPE_FORWARD_ONLY. If not supported, -1 is passed
     * @return Object
     */
    T mapRow(ResultSet rs, int rowNum) throws SQLException;
}
