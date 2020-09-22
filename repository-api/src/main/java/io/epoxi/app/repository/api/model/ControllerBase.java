package io.epoxi.app.repository.api.model;

import java.util.List;



public abstract class ControllerBase {
    
    public ControllerBase()
    {
    }

    /**
     * @param list the list to limit
     * @param skip If 0 is passed, no records are skipped
     * @param limit If 0 is passed, no records are skipped
     * @return the specified list, limited by skip and limit
     */
    protected List<?> limitList(List<?> list, Integer skip, Integer limit)
    {
        if (list.size() <= skip) return list;

        int end = skip + limit ;

        //If no limit was passed, return it all
        if (limit==0) end = list.size() - skip;

        if (list.size() <= end) end = list.size();
        
        return list.subList(skip, end);
    }
   
    public enum TrashOperation {
        UNDELETE, DELETE
    }

    public enum IngestionOperation {
        START, SHUTDOWN, SHUTDOWN_NOW
    }

    public enum IngestionSyncOperation {
        PAUSE, RESUME, SHUTDOWN, SHUTDOWN_NOW
    }
    
}