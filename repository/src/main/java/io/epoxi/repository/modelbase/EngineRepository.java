package io.epoxi.repository.modelbase;

import com.google.cloud.Timestamp;
import com.googlecode.objectify.Key;
import com.googlecode.objectify.NotFoundException;
import com.googlecode.objectify.cmd.Query;
import com.googlecode.objectify.cmd.QueryKeys;
import io.epoxi.repository.exception.DuplicateMemberException;
import io.epoxi.repository.exception.ForeignKeyException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.stream.Collectors;

public class EngineRepository<T extends EngineMember<T>> {

	protected final Class<T> type;
	
	@Getter(AccessLevel.PROTECTED) @Setter(AccessLevel.PROTECTED)
	private Long id;

	@Getter
	private final EngineRepository<?> parentRepository;

	public EngineRepository(Class<T> type)
	{
		this(type, null, null);
	}
	
	public EngineRepository(Class<T> type, Long id)
	{
		this(type, id, null);
	}

	public EngineRepository(Class<T> type, Long id, EngineRepository<?> parentRepository)
	{
		this.type = type;
		this.id = id;
		this.parentRepository = parentRepository;
	}    
	
	/**
	 * Adds a new Object to the Datastore.  
	 * If the object does not have an id (implying it is new), and an object already
	 * exists with this name, a DuplicateMemberException will be thrown.
	 * To overwrite a object, please ensure you specify the object's Id
	 * @param obj The object that will be saved to the data store
	 */
	public synchronized void add(T obj) {

		obj.validOrThrow();		

		if (obj.getId()==null && obj.getName() != null)
		{
			EngineMember<T> existing = get(obj.getName());
			if (existing !=null)
			{
				String msg = String.format("%s already exists with a name of '%s'", type.getSimpleName(), obj.getName());
				throw new DuplicateMemberException(msg, obj.getName());
			}
		}

		if(obj.dateCreated ==null) obj.dateCreated = Timestamp.now().toString();
		obj.isDeleted = false;
		obj.dateDeleted = null;		
		obj.dateLastModified = Timestamp.now().toString();

		ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().save().entity(obj).now());
	}

	public List<T> members() {
        
		return members(false);
	}

	public List<T> members(Boolean searchDeletedItems) {
        
		return search(null, null, searchDeletedItems);
	}

	public List<Key<T>>  memberKeys() {

		return memberKeys( false);
	}

	public List<Key<T>> memberKeys(Boolean searchDeletedItems) {

		return searchIds(searchDeletedItems).list();
	}

	public T get(String name) {
		T obj = null;
		List<T> list = searchName(name);		//this will be overridden by memberRepository if needed.
		if (!list.isEmpty() && list.get(0).getName().equals(name)) obj = list.get(0);
		return obj;
	}

	public T get(Long id) {
       
		return get(id, false);
	}

	public T get(Long id, Boolean searchDeletedItems) {

		T obj = ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().load().type(type).id(id).now());			
	
		if (obj != null && !searchDeletedItems && Boolean.TRUE.equals(obj.isDeleted) )
		{
			obj = null;
		}
       
		return obj;
	}

	public List<T> searchName(String name) {
        
		return search(name, null, false);
	}

	public List<T> searchPattern(String searchPattern) {

		return search(null, searchPattern, false);
	}

	public List<T> searchPattern(String name, String searchPattern) {

		return search(name, searchPattern, false);
	}

	private QueryKeys<T> searchIds(Boolean searchDeletedItems) {

		return  ObjectifyRegistry.run(() -> {

			Query<T> query = ObjectifyRegistry.ofy().load().type(type).filter("isDeleted = ", searchDeletedItems);

			return query.keys();

		});
	}

	private List<T> search(String name, String searchPattern, Boolean searchDeletedItems) {		

		List<T> members = ObjectifyRegistry.run(() -> {			
			
			Query<T> query = ObjectifyRegistry.ofy().load().type(type).filter("isDeleted = ", searchDeletedItems);

			if(name !=null) query = query.filter("name =", name);

			return query.list();

		});	
		
		if (searchPattern!=null)
		{
			String pattern = getRegexPattern(searchPattern);
			members = members.stream()
				.filter(x -> (x.getName()!=null))		//Eliminate where names are null.  We won't match on these
				.filter(x -> !x.getName().matches(pattern))
				.collect(Collectors.toList());
		}		

		//if we are returning a ResultProxy, it is because ofy could not deserialize the result into a list of objects of type= type.  
		//If that happens, it is because there is problems with the way the objects fields are written.  Check there for a fix to ensure proper deserialization.
		return members;
	}

	/**
	 * Performs a cascading soft delete
	 */
	public void delete(T obj) {
        
        delete(obj, false, false);
	}

	/**
	 * Performs a cascading delete
	 * @param permanent if true, the item will be hard deleted.  If false, the item will be marked as deleted (soft delete)
	 */
	public void delete(T obj, Boolean permanent) {
        
        delete(obj, permanent, false);
	}

	/**
	 * Performs a cascading soft delete
	 */
	public void delete(Long id) {

		delete(id, false);
	}

	/**
	 * Performs a cascading delete
	 * @param permanent if true, the item will be hard deleted.  If false, the item will be marked as deleted (soft delete)
	 */
	public void delete(Long id, Boolean permanent) {

		delete(get(id), permanent, false);
	}

	/**
	 * Performs a delete by key. This is useful in the case that the object cannot be deserialized from the datastore
	 * due to corruption.
	 * WARNING: Purge does not check for key constraints, nor perform a cascading delete.  As such, it should
	 * be used only as a last resort in cases where a regular delete is not possible.
	 */
	public static void purge(List<? extends Key<?>> keys) {

		Key<?>[] keysAry = keys.toArray(new Key<?>[keys.size()]);
		purge(keysAry);
	}

	/**
	 * Performs a delete by key. This is useful in the case that the object cannot be deserialized from the datastore
	 * due to corruption.
	 * WARNING: Purge does not check for key constraints, nor perform a cascading delete.  As such, it should
	 * be used only as a last resort in cases where a regular delete is not possible.
	 */
	public static void purge(Key<?>... keys) {

		ObjectifyRegistry.transact(() -> {
			//Perform permanante delete on the object (and cascade delete on children)
			ObjectifyRegistry.ofy().delete().keys(keys).now();
			return null;
		});
	}

	/**
	 * Performs a cascading delete
	 * @param member the member to delete
	 * @param permanent if true, the item will be hard deleted.  If false, the item will be marked as deleted (soft delete)
	 * @param ignoreDisallowConstraints if set to true, cascading deletes will occur without checking for DISALLOW Foreign Key Constraints.
	 */
	private static synchronized void delete(EngineMember<?> member, Boolean permanent, Boolean ignoreDisallowConstraints) {

		//Check for the existence of the object.  Only objects that exist can be deleted.
		if (member == null) throw new NotFoundException();

		if (Boolean.TRUE.equals(member.isDeleted) && Boolean.FALSE.equals(permanent)) throw new IllegalArgumentException("The object set for soft delete has already marked as deleted.");

		//Check foreign key constraints
		if (Boolean.FALSE.equals(ignoreDisallowConstraints)) validateForeignKeyConstraints(member);
		
		//Delete the found object
		if (Boolean.TRUE.equals(permanent))
		{
			ObjectifyRegistry.transact(() -> {
				//Perform permanante delete on the object (and cascade delete on children)
				ObjectifyRegistry.ofy().delete().type(member.getRepository().type).id(member.getId()).now();
				cascadeDelete(member, true);

				return null;

			});
		}
		else
		{
			member.isDeleted = true;
			member.dateDeleted = Timestamp.now().toString();
			ObjectifyRegistry.transact(() -> {
				//Save the object with the new deleted flag  (and cascade delete on children)
				ObjectifyRegistry.ofy().save().entity(member).now();
				cascadeDelete(member, permanent);

				return null;
			});
		}		
	}

	/**
	 * Throws an error if the specified object is referenced by another object 
	 * holding a foreign key constraint that would prevent a delete action of the primary object.
	 * @param objectToDelete  The object being tested to confirm a delete action is possible.
	 * 			From this object, the model will be checked for any objects that hold foreign key constraints
	 * 			that prevent the deletion of this this object to check for foreign keys.  
	 * 			If foreign keys are found they will be checked to see if they hold a reference to the the object being checked.
	 */
	private static void validateForeignKeyConstraints(EngineMember<?> objectToDelete) {

		//Find the type of the object being deleted.  Get all the registered constraints on this object
		List<ForeignKeyConstraint> constraints = getConstraints(objectToDelete);
		if (constraints == null) return;

		constraints = constraints
						.stream()
						.filter(x -> x.getAction().equals(ForeignKey.ConstraintAction.DISALLOW))
						.collect(Collectors.toList());

		if (constraints.isEmpty()) return;

		//Loop through and test each constraint against objects in the datastore.
		//If a constraint is found, then an error will be thrown.
		for(ForeignKeyConstraint constraint : constraints)
		{
			//Get the value for the filter object, either an Id, or a Objectify Ref
			final Object filterDeleteObjectValue;

			if (Boolean.TRUE.equals(constraint.fieldTypeUsesRef()))
			{
				filterDeleteObjectValue = objectToDelete.toRef();
			}
			else
			{
				filterDeleteObjectValue = objectToDelete.getId();
			}

			//Get a count of the constraint violations
			Integer numResults = ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().load().type(constraint.getFieldContainer()).filter(constraint.getFilterPath(), filterDeleteObjectValue).count());
	
			if (numResults > 0) 
			{
				String msg = String.format("Cannot delete %s with id '%s' because it participates within an %s", objectToDelete.getClass().getSimpleName(), objectToDelete.getId(), constraint.getFieldContainer().getSimpleName());
				throw new ForeignKeyException(msg);
			}
		}
	 }
	 
	 /**
	 * Delete any objects that are children of the object being deleted.  
	 * This will prevent the orphaning of children.
	 * @param objectToDelete  The object being deleted.
	 */
	 private static void cascadeDelete(EngineMember<?> objectToDelete, Boolean permanent) {

		//Find the type of the object being deleted.  Get all the registered cascades on this object
		List<ForeignKeyConstraint> cascades = getConstraints(objectToDelete);
		if (cascades == null) return;

		cascades = cascades
					.stream()
					.filter(x -> x.getAction().equals(ForeignKey.ConstraintAction.CASCADE))
					.collect(Collectors.toList());
			
		if (cascades.isEmpty()) return;

		//Loop through and test each constraint against objects in the datastore.
		//If a constraint is found, then an error will be thrown.
		for(ForeignKeyConstraint cascade : cascades)
		{
			//Get the value for the filter object, either an Id, or a Objectify Ref
			final Object filterDeleteObjectValue;			 

			if (Boolean.TRUE.equals(cascade.fieldTypeUsesRef()))
			{
				filterDeleteObjectValue = objectToDelete.toRef();
			}
			else
			{
				filterDeleteObjectValue = objectToDelete.getId();
			}

			//Get a count of the constraint violations
			Class<?> itemType = cascade.getFieldContainer();
			List<?> items = ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().load().type(cascade.getFieldContainer()).filter(cascade.getFilterPath(), filterDeleteObjectValue).list());

			if (items.isEmpty()) continue;

			//Loop through the items returned and delete them
			items.stream()
					.map(x -> (EngineMember<?>)itemType.cast(x))
					.forEach(s-> deleteChild(s, permanent));
		}
	 }

	 private static void deleteChild(EngineMember<?> objectToDelete, Boolean permanent)
	 {
		 objectToDelete.getRepository().delete(objectToDelete, permanent, true);
	 }

	 private static List<ForeignKeyConstraint> getConstraints(EngineMember<?> objectToDelete)
	 {
		 //Find the type of the object being deleted.  Get all the registered cascades on this object			
		Class<?> deleteObjectType = objectToDelete.getClass();
		return ObjectifyRegistry.getForeignConstraintFields().get(deleteObjectType.getName());
	 }

	/**
	 * Performs an undelete for the object previous marked as deleted.  
	 * dateLastModified will be updated for any undeleted object.  dateCreated is not altered during undelete.
	 * This method will not undelete child objects deleted as a result of a "delete cascade" when the delete method was initially called on the object
	 * @param id The id of the item to undelete
	 */
	public synchronized void undelete(Long id) {
		
		T obj = get(id);
        if (obj == null) throw new NotFoundException();		
		
		add(obj);		
	}

	protected String getRegexPattern(String searchPattern)
    {
        String pattern = searchPattern;
        if (!pattern.startsWith("--r"))
        {
            pattern = String.format(".*%s.*", pattern);
        }
        return pattern;
    }
	
}