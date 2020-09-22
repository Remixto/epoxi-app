package io.epoxi.app.repository.modelbase;

import com.googlecode.objectify.Objectify;
import com.googlecode.objectify.ObjectifyService;
import com.googlecode.objectify.Work;
import io.epoxi.app.repository.model.*;
import io.epoxi.app.repository.model.Queue;
import io.epoxi.repository.model.*;
import lombok.Getter;

import java.util.*;

public class ObjectifyRegistry {

	private ObjectifyRegistry()
	{}

	@Getter
	private final static Map<String, List<ForeignKeyConstraint>> foreignConstraintFields = new HashMap<>();

	static {
		register(Account.class);
        register(Metadata.class);
		register(Project.class);		
		register(Schema.class);
		register(Source.class);
		register(SourceType.class);
		register(Stream.class);
		register(Target.class);
		register(Ingestion.class);
		register(IngestionSync.class);
		register(MessageQueue.class);
		register(StepEndpointTemplate.class);
		register(Field.class);
		register(Queue.class);		
	}

	public static Objectify ofy() {			
		return ObjectifyService.ofy();		
	}

	public static <R> R run(final Work<R> work) {			
		return ObjectifyService.run(work);		
	}

	public static <R> R transact(final Work<R> work) {			
		return ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().transact(work));
	}

	private static void register(Class<?> clazz)
	{
		registerForeignKeys(clazz);
		ObjectifyService.register(clazz);	
	}

	private static void registerForeignKeys(Class<?> clazz)
	{	
		Class<?> fieldType;

		List<java.lang.reflect.Field> fields = new ArrayList<>();
		fields.addAll(Arrays.asList(clazz.getSuperclass().getDeclaredFields()));
		fields.addAll(Arrays.asList(clazz.getDeclaredFields()));

		for (java.lang.reflect.Field field : fields) {
			if (field.isAnnotationPresent(ForeignKey.class)) {
			
				//Determine the field type of the field.  
				//If the field is Iterable, then use the inner field type of the iterable value.  
				//Else simply use the type of the field
				
				ForeignKeyConstraint constraint = new ForeignKeyConstraint(field, clazz);
				fieldType = constraint.getFieldType();

				//Get the map entry that matches the field type. If one doesn't exist, create it and add it to the map
				List<ForeignKeyConstraint> constraints = foreignConstraintFields.get(fieldType.getName());
				if (constraints==null)
				{
					constraints = new ArrayList<>();
					constraints.add(constraint);
					foreignConstraintFields.put(fieldType.getName(), constraints);
				}
				else
				{
					constraints.add(constraint);
				}
			}
		}
	}	
}