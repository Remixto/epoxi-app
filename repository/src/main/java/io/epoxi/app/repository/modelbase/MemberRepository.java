package io.epoxi.app.repository.modelbase;

import com.googlecode.objectify.Key;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.cmd.Query;

import com.googlecode.objectify.cmd.QueryKeys;
import io.epoxi.app.repository.model.Project;
import io.epoxi.app.repository.model.Account;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class MemberRepository<T extends AccountMember<T>> extends EngineRepository<T>{
	
	private final Boolean isMultiGenerationalMember;

	public MemberRepository(Class<T> type)
	{
		super(type);		
		isMultiGenerationalMember = false;
	}

    public MemberRepository(Class<T> type, EngineRepository<?> parentRepository)
	{
		this(type, parentRepository, true);
	}	

	public MemberRepository(Class<T> type, EngineRepository<?> parentRepository, Boolean isMultiGenerationalMember)
	{
		super(type, parentRepository.getId(), parentRepository);		
		this.isMultiGenerationalMember = isMultiGenerationalMember;
	}
	
	@Override
	public synchronized void add(T obj) {
		
		// if (obj.getAccountId() == null) 
		// {			
		// 	obj.setAccount(getAccountId());
		// }
		super.add(obj);
	}

	@Override
	public List<T> members() {
        
		return search(null, null, false, isMultiGenerationalMember);
	}

	@Override
	public List<Key<T>>  memberKeys() {

		return memberKeys( false);
	}

	@Override
	public List<Key<T>> memberKeys(Boolean searchDeletedItems) {

		return Objects.requireNonNull(searchIds(searchDeletedItems, isMultiGenerationalMember)).list();
	}

	@Override
	public T get(Long id) {

		T obj = super.get(id);
		Long accountId = getAccountId();

		//If the MemberRepository has an AccountId set, then use it to confirm that the item returned is contained within the account
		if (accountId !=null && obj !=null && Boolean.FALSE.equals(accountId.equals(obj.getAccountId())))
		{
			obj = null;
			}
	
       
		return obj;
	}
	
	@Override
	public List<T> searchPattern(String searchPattern) {
		return search(null, searchPattern, false, isMultiGenerationalMember);
	}

	@Override
	public List<T> searchName(String name) {
		return search(name, null, false, isMultiGenerationalMember);
	}

	/**
	 * Search for members at multiple levels.
	 * @param searchPattern The pattern to search for.  Pattern can include Regular Expressions
	 * @param performMultiGenerationSearch If this is passed as true, results for that specific project will be returned.
	 * 	Results whose parent is Account will be ignored.  This parameter is only only applied to members whose parent
	 *  could either be an Account or Project
	 * @return A list of matching members
	 */
	private List<T> search(String name, String searchPattern, boolean searchDeletedItems, boolean performMultiGenerationSearch)
	{
		List<T> members = new ArrayList<>();
		
		if (getParentRepository()== null)
		{
			members.addAll(search(name, searchPattern, getAccountId(), "AccountRepository", searchDeletedItems));				
		}
		else if (Boolean.TRUE.equals(performMultiGenerationSearch))
		{
			List<T> parentMembers = search(name, searchPattern, getParentRepository().getId(), getParentRepository().getClass().getSimpleName(), searchDeletedItems);
			members.addAll(parentMembers);
		}

		return members;
	}
	
	private List<T> search(String name, String searchPattern, Long currentId, String parentTypeName, Boolean searchDeletedItems) {		

		if (currentId == null) throw new IllegalArgumentException("Member repository required a parentId, that was not passed");

		List<T> members = ObjectifyRegistry.run(() -> {
			Query<T> query = ObjectifyRegistry.ofy().load().type(type).filter("isDeleted = ", searchDeletedItems);
			
			Ref<?> ref;
			
			switch (parentTypeName)
			{
				case "AccountRepository": 
					ref = new RefBuilder<Account>().createRef(Account.class, currentId);
					query = query.filter("account =", ref);
					break;
				case "ProjectRepository":
					ref = new RefBuilder<Project>().createRef(Project.class, currentId);
					query = query.filter("project =", ref);
					break;
				default:
					break;
			}			
			
			if(name!=null) query = query.filter("name =", name);
			return query.list();

		});
		
		if (searchPattern!=null)
		{
			String pattern = getRegexPattern(searchPattern);
			List<T> validList = members.stream()
				.filter(x -> (x.getName()!=null))		//Eliminate where names are null.  We won't match on these
				.filter(x -> x.getName().matches(pattern))
				.collect(Collectors.toList());
	
				members = validList;
		}

		return members;
	}

	/**
	 * Search for members Ids at multiple levels.
	 * @param performMultiGenerationSearch If this is passed as true, results for that specific project will be returned.
	 * 	Results whose parent is Account will be ignored.  This parameter is only only applied to members whose parent
	 *  could either be an Account or Project
	 * @return A list of member Ids
	 */
	private QueryKeys<T> searchIds(Boolean searchDeletedItems, Boolean performMultiGenerationSearch)
	{
		List<T> members = new ArrayList<>();

		if (getParentRepository()== null)
		{
			return searchIds(getAccountId(), "AccountRepository", searchDeletedItems);
		}
		else if (Boolean.TRUE.equals(performMultiGenerationSearch))
		{
			return searchIds(getParentRepository().getId(), getParentRepository().getClass().getSimpleName(), searchDeletedItems);
		}

		return null;
	}

	private QueryKeys<T> searchIds(Long currentId, String parentTypeName, boolean searchDeletedItems) {

		return  ObjectifyRegistry.run(() -> {

			Query<T> query = ObjectifyRegistry.ofy().load().type(type).filter("isDeleted = ", searchDeletedItems);

			Ref<?> ref;

			switch (parentTypeName)
			{
				case "AccountRepository":
					ref = new RefBuilder<Account>().createRef(Account.class, currentId);
					query = query.filter("account =", ref);
					break;
				case "ProjectRepository":
					ref = new RefBuilder<Project>().createRef(Project.class, currentId);
					query = query.filter("project =", ref);
					break;
				default:
					break;
			}

			return query.keys();

		});
	}


	private Long getAccountId()
	{
		Long accountId = null;

		EngineRepository<?> parent = getParentRepository();

		while(parent != null)
		{
			if(parent.getParentRepository() == null)
			{
				accountId = getAccountId(parent);
				break;
			}
			else
			{
				parent = parent.getParentRepository();
			}
		}
		
		return accountId;
	}

	private Long getAccountId(EngineRepository<?> engineRepository)
	{
		return engineRepository.getId();
	}
}