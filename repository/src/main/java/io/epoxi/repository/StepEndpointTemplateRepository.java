package io.epoxi.repository;

import io.epoxi.repository.model.StepEndpointTemplate;
import io.epoxi.repository.model.StepType;
import io.epoxi.repository.modelbase.EngineRepository;
import io.epoxi.repository.modelbase.ObjectifyRegistry;

import java.util.List;

public class StepEndpointTemplateRepository extends EngineRepository<StepEndpointTemplate> {

	public StepEndpointTemplateRepository() {
		super(StepEndpointTemplate.class);
	}

	public List<StepEndpointTemplate> search(StepType stepType) {
        
		return ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().load().type(StepEndpointTemplate.class).filter("stepType =", stepType).list() );
		
	}



	
}