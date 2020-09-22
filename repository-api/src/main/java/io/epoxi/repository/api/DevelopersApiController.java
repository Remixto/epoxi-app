package io.epoxi.repository.api;

import com.google.api.server.spi.ServiceException;
import com.google.api.server.spi.response.*;
import io.epoxi.cloud.pubsub.PubsubException;
import io.epoxi.repository.api.model.DevelopersApi;
import io.epoxi.repository.api.model.ControllerBase;
import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.IngestionRepository;
import io.epoxi.repository.IngestionSyncRepository;
import io.epoxi.repository.model.*;
import io.epoxi.repository.modelbase.MemberRepository;

import java.util.List;

public class DevelopersApiController extends ControllerBase implements DevelopersApi {

	private final AccountRepository accountRepository;

	public DevelopersApiController(String accountName) {
		super(); // Init the ApiBase
		this.accountRepository = AccountRepository.of(accountName);
	}

	public DevelopersApiController(Long accountId) {
		super(); // Init the ApiBase
		this.accountRepository = AccountRepository.of(accountId);
	}

	public void addProject(Project project) {

		MemberRepository<Project> projectRepository = accountRepository.getProjectRepository();
		projectRepository.add(project);
	}

	public void addSource(Source source) {
		MemberRepository<Source> sourceRepository = accountRepository.getSourceRepository();
		sourceRepository.add(source);
	}

	public void addTarget(Target target) {
		MemberRepository<Target> targetRepository = accountRepository.getTargetRepository();
		targetRepository.add(target);
	}

	public void addIngestion(Ingestion ingestion) {
		
		MemberRepository<Ingestion> ingestionRepository = accountRepository.getIngestionRepository(ingestion.getProjectId());
		ingestionRepository.add(ingestion);
	}

	public void addIngestionSync(IngestionSync ingestionSync) throws ServiceException {

		try {
			IngestionSyncRepository ingestionSyncRepository = accountRepository.getIngestionSyncRepository();			
			ingestionSyncRepository.add(ingestionSync);

		} catch (PubsubException ex) {
			if (ingestionSync.getSchedule()==null) {
				throw getEndpointException("Sync cloud not started.",
						"No queueing service was available to queue the ingestion.", ex);
			} else {
				throw getEndpointException("Sync cloud not scheduled.",
						"No schedule service was available to schedule the ingestion.", ex);
			}
		}
	}	

	public void patchIngestion(Long id, IngestionOperation operation) {

		MemberRepository<Ingestion> ingestionRepository = accountRepository.getIngestionRepository();	
		Ingestion ingestion = ingestionRepository.get(id);

		IngestionSyncRepository ingestionSyncRepository = accountRepository.getIngestionSyncRepository();	

		switch(operation)
        {
            case START :
				ingestionSyncRepository.add(ingestion);
                break;           
			case SHUTDOWN :
			case SHUTDOWN_NOW :
				throw new UnsupportedOperationException();  //TODO fix this
		}
	}

    public void patchIngestionSync(Long id, IngestionSyncOperation operation) {

        switch(operation)
		{
            case PAUSE :
                pauseIngestionSync(id);
                break;
            case RESUME :
				resumeIngestionSync(id);
				break;
			case SHUTDOWN :
				interruptIngestionSync(id, false);
				break;
			case SHUTDOWN_NOW :
				interruptIngestionSync(id, true);
                break;
        }
    }

	public void addStream(Stream stream) {
		MemberRepository<Stream> streamRepository = accountRepository.getStreamRepository();
		streamRepository.add(stream);
	}


	public void deleteProject(Long id) {
	
		deleteProject(id, false);
	}

	public void deleteProject(Long id, Boolean permanent) {

		MemberRepository<Project> projectRepository = accountRepository.getProjectRepository();
		projectRepository.delete(id, permanent);
	}

	public void deleteSource(Long id) {
	
		deleteSource(id, false);
	}

	public void deleteSource(Long id, Boolean permanent) {
		MemberRepository<Source> sourceRepository = accountRepository.getSourceRepository();
		sourceRepository.delete(id, permanent);
	}

	public void deleteTarget(Long id) {
	
		deleteTarget(id, false);
	}

	public void deleteTarget(Long id, Boolean permanent) {
		MemberRepository<Target> targetRepository = accountRepository.getTargetRepository();
		targetRepository.delete(id, permanent);
	}

	public void deleteIngestion(Long id) {
	
		deleteIngestion(id, false);
	}

	public void deleteIngestion(Long id, Boolean permanent) {
		MemberRepository<Ingestion> ingestionRepository = accountRepository.getIngestionRepository();
		ingestionRepository.delete(id, permanent);
	}

	public void deleteIngestionSync(Long id) {
		IngestionSyncRepository ingestionSyncRepository = accountRepository.getIngestionSyncRepository();
		ingestionSyncRepository.delete(id, true);
	}

	public void deleteStream(Long id) {
	
		deleteStream(id, false);
	}

	public void deleteStream(Long id, Boolean permanent) {
		MemberRepository<Stream> streamRepository = accountRepository.getStreamRepository();
		streamRepository.delete(id, permanent);
	}

	public Account getAccount() {
		return accountRepository.getAccount();
	}

	public Project getProject(Long id) {

		MemberRepository<Project> projectRepository = accountRepository.getProjectRepository();
		return projectRepository.get(id);
	}

	public Source getSource(Long id) {
		MemberRepository<Source> sourceRepository = accountRepository.getSourceRepository();
		return sourceRepository.get(id);
	}

	public Target getTarget(Long id) {
		MemberRepository<Target> targetRepository = accountRepository.getTargetRepository();
		return targetRepository.get(id);
	}

	public Ingestion getIngestion(Long id) {
		MemberRepository<Ingestion> ingestionRepository = accountRepository.getIngestionRepository();
		return ingestionRepository.get(id);
	}

	public IngestionSync getIngestionSync(Long id) {
		IngestionSyncRepository ingestionSyncRepository = accountRepository.getIngestionSyncRepository();
		return ingestionSyncRepository.get(id);
	}

	public Stream getStream(Long id) {
		MemberRepository<Stream> streamRepository = accountRepository.getStreamRepository();
		return streamRepository.get(id);
	}

	public Project getProject(String name) {

		MemberRepository<Project> projectRepository = accountRepository.getProjectRepository();
		return projectRepository.get(name);
	}

	public Source getSource(String name) {
		MemberRepository<Source> sourceRepository = accountRepository.getSourceRepository();
		return sourceRepository.get(name);
	}

	public Source getSource(String name, Long projectId) {
		MemberRepository<Source> sourceRepository = accountRepository.getSourceRepository(projectId);
		return sourceRepository.get(name);
	}

	public Target getTarget(String name) {
		MemberRepository<Target> targetRepository = accountRepository.getTargetRepository();
		return targetRepository.get(name);
	}

	public Target getTarget(String name, Long projectId) {
		MemberRepository<Target> targetRepository = accountRepository.getTargetRepository(projectId);
		return targetRepository.get(name);
	}

	public Stream getStream(String name) {
		MemberRepository<Stream> streamRepository = accountRepository.getStreamRepository();
		return streamRepository.get(name);
	}

	public Stream getStream(String name, Long projectId) {
		MemberRepository<Stream> streamRepository = accountRepository.getStreamRepository(projectId);
		return streamRepository.get(name);
	}

	public Ingestion getIngestion(String name, Long projectId) {
		MemberRepository<Ingestion> ingestionRepository = accountRepository.getIngestionRepository(projectId);
		return ingestionRepository.get(name);
	}

	public IngestionSync getIngestionSync(String name, Long projectId) {
		IngestionSyncRepository ingestionSyncRepository = accountRepository.getIngestionSyncRepository(projectId);
		return ingestionSyncRepository.get(name);
	}

	@SuppressWarnings("unchecked")
	public List<Project> searchProject(String searchPattern, String name, Integer skip, Integer limit) {

		MemberRepository<Project> repository = accountRepository.getProjectRepository();
		
		List<Project> list;
		if(name!=null)
		{
			list = repository.searchName(name);
		}
		else
		{
			list = repository.searchPattern(searchPattern);
		}

		return (List<Project>) limitList(list, skip, limit);
	}

	@SuppressWarnings("unchecked")
	public List<Source> searchSource(String searchPattern, String name, Long projectId, Integer skip, Integer limit) {
		MemberRepository<Source> repository = accountRepository.getSourceRepository(projectId);
		
		List<Source> list;
		if(name!=null)
		{
			list = repository.searchName(name);
		}
		else
		{
			list = repository.searchPattern(searchPattern);
		}		

		return (List<Source>) limitList(list, skip, limit);
	}

	@SuppressWarnings("unchecked")
	public List<Target> searchTarget(String searchPattern, String name, Long projectId, Integer skip, Integer limit) {
		MemberRepository<Target> repository = accountRepository.getTargetRepository(projectId);
		
		List<Target> list;
		if(name!=null)
		{
			list = repository.searchName(name);
		}
		else
		{
			list = repository.searchPattern(searchPattern);
		}

		return (List<Target>) limitList(list, skip, limit);
	}

	@SuppressWarnings("unchecked")
	public List<Ingestion> searchIngestion(String searchPattern, String name, Long projectId, Integer skip, Integer limit) {

		IngestionRepository repository = accountRepository.getIngestionRepository(projectId);	

		List<Ingestion> list;

		if(name!=null)
		{
			list = repository.searchName(name);
		}
		else
		{
			list = repository.searchPattern(searchPattern);
		}		

		return (List<Ingestion>) limitList(list, skip, limit);
	}

	@SuppressWarnings("unchecked")
	public List<Stream> searchStream(String searchPattern, String name, Long projectId, Integer skip, Integer limit) {

		MemberRepository<Stream> repository = accountRepository.getStreamRepository(projectId);
		List<Stream> list;

		if(name!=null)
		{
			list = repository.searchName(name);
		}
		else
		{
			list = repository.searchPattern(searchPattern);
		}

		return (List<Stream>) limitList(list, skip, limit);
	}

	public ServiceException getEndpointException(String msg, String reason, PubsubException ex) {
		ServiceException returnEx = null;
		switch (ex.getStatusCode()) {
			case OK:
				break;
			case ALREADY_EXISTS:
			case UNKNOWN:
			case INVALID_ARGUMENT:
				returnEx = new BadRequestException(msg, reason, ex.getCause());
				break;
			case UNAUTHENTICATED:
			case PERMISSION_DENIED:
				returnEx = new UnauthorizedException(msg, reason, ex.getCause());
				break;
			case DATA_LOSS:
				returnEx = new ForbiddenException(msg, reason, ex.getCause());
				break;
			case NOT_FOUND:
				returnEx = new NotFoundException(msg, reason, ex.getCause());
				break;
			case OUT_OF_RANGE:
				returnEx = new ConflictException(msg, reason, ex.getCause());
				break;
			case ABORTED:
			case CANCELLED:
			case RESOURCE_EXHAUSTED:
			case UNIMPLEMENTED:
			case INTERNAL:
			case FAILED_PRECONDITION:
				returnEx = new InternalServerErrorException(msg, reason, ex.getCause());
				break;
			case DEADLINE_EXCEEDED:
			case UNAVAILABLE:
				returnEx = new ServiceUnavailableException(msg, reason, ex.getCause());
				break;
		}

		return returnEx;
	}

	//#region Trash methods

    public Ingestion getDeletedIngestion(Long id) {
		MemberRepository<Ingestion> ingestionRepository = accountRepository.getIngestionRepository();
		return ingestionRepository.get(id, true);
    }

    public void patchIngestion(Long id, TrashOperation operation) {

		if (operation == TrashOperation.UNDELETE) {
			undeleteIngestion(id);
		} else if (operation == TrashOperation.DELETE) {
			deleteIngestion(id, true);
		}
    }

    public void undeleteIngestion(Long id) {
        throw new UnsupportedOperationException(String.format("Cannot undelete %s. Undeletes are not yet implemented by the API", id));  //TODO fix this
    }

    //#endregion


	//#region IngestionSync operations
	public void pauseIngestionSync(Long id) {
		IngestionSyncRepository ingestionSyncRepository = accountRepository.getIngestionSyncRepository();
		ingestionSyncRepository.pauseIngestionSchedule(id);
	}
	
	public void resumeIngestionSync(Long id) {
        IngestionSyncRepository ingestionSyncRepository = accountRepository.getIngestionSyncRepository();
		ingestionSyncRepository.resumeIngestionSchedule(id);
	}
	
	public void interruptIngestionSync(Long id, Boolean now) {
		IngestionSyncRepository ingestionSyncRepository = accountRepository.getIngestionSyncRepository();
		ingestionSyncRepository.shutdownIngestionSchedule(id, now);
	}
	
	public IngestionSyncProgress getIngestionSyncProgress(Long id) {
		IngestionSyncRepository ingestionSyncRepository = accountRepository.getIngestionSyncRepository();
		return ingestionSyncRepository.progress(id);       
    }

	//#endregion
	
}