package com.omegasixcloud.database;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by amcde on 9/23/2017.
 */
public class QueryParameter {

    private Object value;
    private Integer type;

    public QueryParameter(Object value, Integer sqlType)
    {
        this.value = value;
        type = sqlType;
    }

    public void setValueOnPreparedStatement(PreparedStatement ps, int paramNumber) throws SQLException
    {
        if(null == value)
        {
            ps.setNull(paramNumber, type);
            return;
        }

        switch(type)
        {
            case Types.BIT:
                ps.setBoolean(paramNumber, (boolean)value);
                return;
            case Types.DOUBLE:
                ps.setDouble(paramNumber, (double)value);
                return;
            case Types.INTEGER:
                ps.setInt(paramNumber, (int)value);
                return;
            case Types.BIGINT:
                ps.setLong(paramNumber, (long)value);
                return;
            case Types.DECIMAL:
                ps.setBigDecimal(paramNumber, (BigDecimal)value);
                return;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.OTHER: // Assume JSON columns
            default:
                ps.setString(paramNumber, (String)value);
        }
    }
}
