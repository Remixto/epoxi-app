package io.epoxi.repository;

import io.epoxi.repository.model.Ingestion;
import io.epoxi.repository.model.KeysetId;
import io.epoxi.repository.model.Project;
import io.epoxi.repository.modelbase.EngineRepository;
import io.epoxi.repository.modelbase.MemberRepository;

public class IngestionRepository extends MemberRepository<Ingestion> {
    
    protected IngestionRepository(EngineRepository<?> parentRepository) {
        super(Ingestion.class, parentRepository);
    }

	protected IngestionRepository(MemberRepository<?> parentRepository) {
        super(Ingestion.class, parentRepository);
    }
    
    public static Ingestion getIngestion(KeysetId keysetId)
	{
		IngestionRepository repository = IngestionRepository.of(keysetId.getProjectName());
        return  repository.get(keysetId.getKeysetName());
    }
    
    public static IngestionRepository of(MemberRepository<?> parentRepository)
	{
        return new IngestionRepository(parentRepository);
    }

    public static IngestionRepository of(String projectName)
	{
        AccountRepository accountRepository = new AccountRepository();
        Project project = accountRepository.getProject(projectName);
        return accountRepository.getIngestionRepository(project.getId());
    }
}