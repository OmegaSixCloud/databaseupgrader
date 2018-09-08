package com.omegasixcloud.database;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Created by amcde on 9/23/2017.
 */
public class DBConnection {

    private static BasicDataSource dataSource;
    private static String dbUrl;
    private static String dbUsername;
    private static String dbPassword;

    public static void setDBCredentials(String url, String username, String password)
    {
        dbUrl = url;
        dbUsername = username;
        dbPassword = password;
    }

    private static BasicDataSource getDataSource() throws SQLException
    {
        if(dbUrl == null || dbUsername == null || dbPassword == null)
        {
            throw new SQLException("DB credentials have not been set");
        }

        if(dataSource == null)
        {
            BasicDataSource ds = new BasicDataSource();
            ds.setUrl(dbUrl);
            ds.setUsername(dbUsername);
            ds.setPassword(dbPassword);

            ds.setInitialSize(2);
            ds.setMinIdle(2);
            ds.setMaxIdle(50);
            ds.setMaxTotal(50);

            ds.setMaxWaitMillis(10000);

            ds.setDefaultAutoCommit(false);

            dataSource = ds;
        }
        return dataSource;
    }

    public static Connection getDBConnection() throws SQLException
    {
        return getDataSource().getConnection();
    }

    public static <T> List<T> queryWithParameters(
            String sql,
            List<QueryParameter> parameters,
            Function<ResultSet, Object> resultFunc) throws SQLException
    {
        try(Connection conn = getDBConnection();
            PreparedStatement ps = conn.prepareStatement(sql, Statement.NO_GENERATED_KEYS))
        {
            for(int i = 0; i < parameters.size(); i++)
            {
                parameters.get(i).setValueOnPreparedStatement(ps, i + 1);
            }

            ResultSet rs = ps.executeQuery();
            if(!rs.isBeforeFirst())
            {
                return new ArrayList<>();
            }

            List<T> resultList = new ArrayList<>();

            while(rs.next())
            {
                T obj = (T)resultFunc.apply(rs);
                if(obj != null)
                {
                    resultList.add(obj);
                }
            }

            return resultList;

        } catch (SQLException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    public static Long queryWithParametersGetGeneratedKey(
            String sql,
            List<QueryParameter> parameters) throws SQLException
    {
        try(Connection conn = getDBConnection();
            PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS))
        {
            for(int i = 0; i < parameters.size(); i++)
            {
                parameters.get(i).setValueOnPreparedStatement(ps, i + 1);
            }

            ResultSet rs = ps.executeQuery();
            if(!rs.isBeforeFirst())
            {
                return null;
            }

            while(rs.next())
            {
                return rs.getLong(1);
            }

            return null;

        } catch (SQLException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    public static void executeWithParams(String sql, List<QueryParameter> parameters) throws SQLException
    {
        try(Connection conn = getDBConnection();
            PreparedStatement ps = conn.prepareStatement(sql))
        {
            for(int i = 0; i < parameters.size(); i++)
            {
                parameters.get(i).setValueOnPreparedStatement(ps, i + 1);
            }
            ps.execute();
            conn.commit();

        } catch (SQLException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    public static void executeUpdateWithParams(String sql, List<QueryParameter> parameters) throws SQLException
    {
        try(Connection conn = getDBConnection();
            PreparedStatement ps = conn.prepareStatement(sql))
        {
            for(int i = 0; i < parameters.size(); i++)
            {
                parameters.get(i).setValueOnPreparedStatement(ps, i + 1);
            }
            ps.executeUpdate();
            conn.commit();

        } catch (SQLException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    public static Long executeUpdateReturnGeneratedKey(
            String sql,
            List<QueryParameter> parameters) throws SQLException
    {
        try(Connection conn = getDBConnection();
            PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS))
        {
            for(int i = 0; i < parameters.size(); i++)
            {
                parameters.get(i).setValueOnPreparedStatement(ps, i + 1);
            }

            ps.executeUpdate();
            conn.commit();

            try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                }
                else {
                    throw new SQLException("Item price insert failed, no ID obtained.");
                }
            }
        } catch (SQLException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    public static void executeBatch(String sql,QueryBatch batch) throws SQLException
    {
        try(Connection conn = getDBConnection();
            PreparedStatement ps = conn.prepareStatement(sql))
        {
            for(List<QueryParameter> qpList : batch.getBatch())
            {
                for(int i = 0; i < qpList.size(); i++)
                {
                    qpList.get(i).setValueOnPreparedStatement(ps, i + 1);
                }
                ps.addBatch();
            }

            ps.executeBatch();
            conn.commit();

        } catch (SQLException e)
        {
            e.printStackTrace();
            throw e;
        }
    }
}
