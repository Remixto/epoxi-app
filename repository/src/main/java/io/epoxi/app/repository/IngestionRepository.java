package io.epoxi.app.repository;

import io.epoxi.app.repository.model.Ingestion;
import io.epoxi.app.repository.model.KeysetId;
import io.epoxi.app.repository.model.Project;
import io.epoxi.app.repository.modelbase.EngineRepository;
import io.epoxi.app.repository.modelbase.MemberRepository;

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