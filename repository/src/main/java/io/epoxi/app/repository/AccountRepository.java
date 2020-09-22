package io.epoxi.app.repository;

import com.googlecode.objectify.Key;
import io.epoxi.app.repository.model.*;
import io.epoxi.app.repository.modelbase.MemberRepository;
import io.epoxi.app.repository.exception.NotFoundException;
import io.epoxi.repository.model.*;
import io.epoxi.app.repository.modelbase.EngineRepository;
import io.epoxi.app.repository.modelbase.ObjectifyRegistry;
import io.epoxi.app.repository.modelbase.RefBuilder;

import javax.xml.bind.annotation.XmlEnumValue;

/**
 * Account repository can be instantiated with our without an AccountId.  
 * When instantiated by the adminApi, it does not need an api.
 * When instantiated by the devApi, it must contain an account Id.
 * 
 */
public class AccountRepository extends EngineRepository<Account> {

	public AccountRepository()
	{
		super(Account.class, null);	
	}

	private AccountRepository (Long accountId)
	{	
		super(Account.class, accountId);
	}

	public Long getAccountId() {

		if(getId() == null) throw new IllegalArgumentException("AccountId not set.");
		return getId();
	}

	public Account getAccount() {

		if(getId() == null) throw new IllegalArgumentException("Account not set.");
		return get(getId());
	}

	/**
	 * Gets a project.  If the project is found, and the accountId of the 
	 * AccountRepository is not set, it will be set to the accountID of the project
	 * @param projectName  The name of the project to find
	 * @return A Project object based on the name
	 */
	public Project getProject(String projectName)
	{
		//This will not be retrieved via the repository, 
		//as we don't know the AccountId at this point, 
		//and this is needed by the repository to complete the request
		Project project = ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().load().type(Project.class).filter("name = ", projectName).first().now());

		if (project == null) throw new NotFoundException("IngestionRepository cannot be created. Cannot get Project from projectName", projectName);
		if (getId()==null) setId(project.getAccountId());
		return project;
    }

	/**
	 * Gets a project.  If the project is found, and the accountId of the 
	 * AccountRepository is not set, it will be set to the accountID of the project
	 * @param projectId  The Id of the project to find
	 * @return A project object
	 */
	public Project getProject(Long projectId)
	{
		Project project = getEngineProjectRepository().get(projectId);
		
		if (project == null) throw new NotFoundException("IngestionRepository cannot be created. Cannot get Project from projectId.", projectId);
		if (getId()==null) setId(project.getAccountId());
		return project;
	}

	public MemberRepository<?> getChildRepository(ChildMember child) {

		switch(child)
		{
			case SOURCE:
				return getSourceRepository(null);
			case TARGET:
				return getTargetRepository(null);
			case STREAM:
				return new MemberRepository<>(Stream.class, this);
			case PROJECT:
				return new MemberRepository<>(Project.class, this);
			case INGESTION:
				return getIngestionRepository(null);
			case INGESTION_SYNC:
				return new MemberRepository<>(IngestionSync.class, this);
		}

		throw new IllegalArgumentException();
	}
	
	public MemberRepository<Project> getEngineProjectRepository() {
		
		return new MemberRepository<>(Project.class);
	}

	public ProjectRepository getProjectRepository() {

		if(getId() == null) throw new IllegalAccessError("Account not set.  Call getEngineProjectRepository() instead");
		
		return new ProjectRepository(this);
	}

	public MemberRepository<Source> getSourceRepository() {

		return getSourceRepository(null);
	}

	public MemberRepository<Target> getTargetRepository() {

		return getTargetRepository(null);
	}

	public MemberRepository<Stream> getStreamRepository() {

		return getStreamRepository(null);
	}

	public MemberRepository<Ingestion> getIngestionRepository() {

		return getIngestionRepository(null);
	}

	public MemberRepository<Source> getSourceRepository(Long projectId) {

		if(getId() == null) throw new IllegalAccessError("Cannot get SourceRepository. AccountId not set");

		if (projectId ==null)
			return new MemberRepository<>(Source.class, this);
		else
			return getProjectRepository().getSourceRepository(projectId);
	}


	public MemberRepository<Target> getTargetRepository(Long projectId) {

		if(getId() == null ) throw new IllegalAccessError("Cannot get TargetRepository. AccountId not set");

		if (projectId ==null)
			return new MemberRepository<>(Target.class, this);
		else
			return getProjectRepository().getTargetRepository(projectId);
	}

	public IngestionRepository getIngestionRepository(Long projectId) {

		if(getId() == null) throw new IllegalAccessError("Cannot get IngestionRepository. AccountId not set");

		if (projectId == null)
			return new IngestionRepository(this);
		else
			return getProjectRepository().getIngestionRepository(projectId);
	}

	public IngestionSyncRepository getIngestionSyncRepository() {

		if(getId() == null) throw new IllegalAccessError("Cannot get IngestionSyncRepository. AccountId not set");
		return new IngestionSyncRepository(this);
	}
	
	public IngestionSyncRepository getIngestionSyncRepository(Long projectId) {

		if(getId() == null) throw new IllegalAccessError("Cannot get IngestionSyncRepository. AccountId not set");

		if (projectId == null)
			return new IngestionSyncRepository(this);
		else
			return getProjectRepository().getIngestionSyncRepository(projectId);
	}

	public MemberRepository<Stream> getStreamRepository(Long projectId) {

		if (projectId ==null)
			return new MemberRepository<>(Stream.class, this);
		else
			return getProjectRepository().getStreamRepository(projectId);
	}

	public MessageQueueRepository getMessageQueueRepository() {

		//This repository can be accessed without an account, as it is not account specific
		return new MessageQueueRepository();
	}

	public StepEndpointTemplateRepository getStepEndpointTemplateRepository() {

		//This repository can be accessed without an account, as it is not account specific
		return new StepEndpointTemplateRepository();
	}

	public SourceTypeRepository getSourceTypeRepository() {
		//This repository can be accessed without an account, as it is not account specific
		return new SourceTypeRepository();
	}

	public void purge() {

		//Purge all child objects of the account
		for(AccountRepository.ChildMember item : AccountRepository.ChildMember.values())
		{
			MemberRepository<?> child = this.getChildRepository(item);
			purge(child.memberKeys(true));
		}

		//Finally, purge the account itself
		Key<Account> key = new RefBuilder<Account>().createRef(Account.class, getAccountId()).getKey();
		purge(key);
	}

	public static AccountRepository of (Long accountId)
	{
		return new AccountRepository(accountId);
	}

	public static AccountRepository of(String accountName)
	{
		AccountRepository repository = new AccountRepository(null);
		Account account = repository.get(accountName);

		if(account == null) throw new NotFoundException("AccountName not found", accountName);

		Long accountId = account.getId();
		return new AccountRepository(accountId);
	}



	public enum ChildMember {

		@XmlEnumValue("PROJECT") PROJECT("PROJECT"),
		@XmlEnumValue("SOURCE")	SOURCE("SOURCE"),
		@XmlEnumValue("TARGET")	TARGET("TARGET"),
		@XmlEnumValue("STREAM")	STREAM("STREAM"),
		@XmlEnumValue("INGESTION")	INGESTION("INGESTION"),
		@XmlEnumValue("INGESTION_SYNC")	INGESTION_SYNC("INGESTION_SYNC");

		private final String value;

		ChildMember(String v) {
			value = v;
		}

		public String value() {
			return value;
		}

		@Override
		public String toString() {
			return String.valueOf(value);
		}

		public static AccountRepository.ChildMember fromValue(String v) {
			for (AccountRepository.ChildMember b : AccountRepository.ChildMember.values()) {
				if (String.valueOf(b.value).equals(v)) {
					return b;
				}
			}
			return null;
		}
	}

}