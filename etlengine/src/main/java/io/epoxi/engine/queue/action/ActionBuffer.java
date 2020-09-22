package io.epoxi.engine.queue.action;

import io.epoxi.cloud.logging.AppLog;
import io.epoxi.repository.model.Ingestion.ReplicationType;
import io.epoxi.repository.model.KeysetId;

import java.lang.invoke.MethodHandles;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

public class ActionBuffer extends AbstractList<Action>{

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    private ReplicationType updateType;
    private KeysetId keysetId;

    //Fields used in the running of the action buffer
    private final List<Action> actions = new ArrayList<>();
    private Integer maxSize = 1;

    @Override
    public void clear()
    {
        actions.clear();
    }

    public Boolean isFull()
    {
        return (actions.size() >= maxSize);
    }

    public Integer getMaxSize() {
        return maxSize;
    }

    public Integer getSize() {
        return actions.size();
    }

    public void setMaxSize(Integer maxSize) {
        this.maxSize = maxSize;
    }

    public ReplicationType getUpdateType() {
        return updateType;
    }

    public void setUpdateType(ReplicationType updateType) {
        this.updateType = updateType;
    }

    public KeysetId getKeysetId() {
        return keysetId;
    }

    public void setKeysetId(KeysetId keysetId) {
        this.keysetId = keysetId;
    }

    @Override
    public boolean add(Action action) {
        if (keysetId == null) keysetId = action.getKeysetId();
        if (updateType == null) updateType = action.getUpdateType();

        if(keysetId.toString().equals(action.getKeysetId().toString()) && updateType.equals(action.getUpdateType()))
        {
            actions.add(action);
            logger.atDebug().log("Action added to pipeline buffer");
            return true;
        }
        else
        {
            logger.atWarn()
                .addKeyValue("keysetId", keysetId)
                .addKeyValue("action.keysetId", action.getKeysetId())
                .log("Action was not added to the buffer because the keysetId or UpdateType of the action to add does not match the keyset or UpdateType of other actions previously added");

        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return actions.remove(o);
    }

    @Override
    public Action get(int index) {
       return actions.get(index);
    }

    @Override
    public void add(int index, Action action)
    {
        actions.add(action);
    }

    @Override
    public int size() {
        return actions.size();
    }

}
