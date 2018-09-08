package com.omegasixcloud.database;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by amcde on 9/24/2017.
 */
public class QueryBatch {

    private List<List<QueryParameter>> _batch;

    public QueryBatch()
    {
        _batch = new ArrayList<>();
    }

    public void addBatch(List<QueryParameter> paramList)
    {
        _batch.add(paramList);
    }

    public List<List<QueryParameter>> getBatch()
    {
        return _batch;
    }

    public boolean empty()
    {
        return _batch.size() <= 0;
    }
}
