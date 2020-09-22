package io.epoxi.engine;

import io.epoxi.engine.etl.ETLStep;
import io.epoxi.engine.queue.action.Action;
import io.epoxi.repository.TestDataFactory;
import io.epoxi.repository.model.Ingestion;
import io.epoxi.repository.model.KeysetId;
import io.epoxi.repository.model.StepType;
import io.epoxi.repository.event.ETLTimestamp;
import io.epoxi.repository.event.Event;

import java.util.ArrayList;
import java.util.List;

public class EngineTestDataFactory extends TestDataFactory {

    public EngineTestDataFactory(String accountName) {
        super(accountName);
    }

    public EngineTestDataFactory(Long accountId) {
        super(accountId);
    }

    public Event getTestEvent() {
        KeysetId keysetId = KeysetId.of("TestProject", "testTable");
        return getTestEvent(keysetId);
    }

    public Event getTestEvent(KeysetId keysetId) {
        return new Event(keysetId);
    }

    public Action getTestAction(StepType EtlStepType) {
        Event event = getTestEvent();
        return getTestAction(EtlStepType, event);
    }

    public Action getTestAction(StepType stepType, Event event) {
        return new Action(stepType, event);
    }

    public List<KeysetId> getTestKeysets() {
        List<KeysetId> list = new ArrayList<>();

        list.add(getKeyset("Firm_Data_Entity_Type"));
        list.add(getKeyset("Firm_Data_Entity"));

        return list;
    }

    private KeysetId getKeyset(String name) {
        return KeysetId.of(getFirstProject().getName(), name);
    }

    public ETLTimestamp getTransformTestTimestamp() {
        return ETLTimestamp.fromNanos(1599100000000000L); // Sept 2, 2020 (and some seconds)
    }

    public Action getAction(StepType stepType, KeysetId keysetId) {
		return getAction (stepType, keysetId, ETLTimestamp.now());
    }

    public Action getAction(StepType stepType, KeysetId keysetId, ETLTimestamp timestamp) {
        Event event = new Event(keysetId, timestamp);
        return new Action(stepType, event);
    }

     /***
     * Returns an ETLStep with a single buffered action
     * @param stepType  The stepType of the ETLStep to create
     * @param keysetId The keysetId to assign to the ETLStep
     * @return a new ETLStep
     */
    public ETLStep getEtlStep(StepType stepType, KeysetId keysetId)
    {
        return getEtlStep(stepType, keysetId, ETLTimestamp.now());
    }

    /***
     * Returns an ETLStep with a single buffered action
     * @param stepType The stepType of the ETLStep to create
     * @param keysetId The keysetId to assign to the ETLStep
     * @return a new ETLStep
     */
    public ETLStep getEtlStep(StepType stepType, KeysetId keysetId, ETLTimestamp timestamp) {
		//Get the ingestion from the keysetId;
        Action action = getAction (stepType, keysetId, timestamp);
        Ingestion ingestion = getTestIngestion(keysetId);

        ETLStep step = new ETLStep(ingestion, stepType);
        step.bufferAction(action);

        return step;
	}

}
