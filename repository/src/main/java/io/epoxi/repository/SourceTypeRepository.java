package io.epoxi.repository;

import io.epoxi.repository.model.SourceType;
import io.epoxi.repository.model.StepType;
import io.epoxi.repository.modelbase.EngineRepository;
import io.epoxi.repository.modelbase.ObjectifyRegistry;

import java.util.List;

public class SourceTypeRepository extends EngineRepository<SourceType> {

	public SourceTypeRepository() {
		super(SourceType.class);
	}

	public List<SourceType> search(StepType stepType) {
        
		return  ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().load().type(SourceType.class).filter("stepType =", stepType).list());
	}



	
}