package io.epoxi.app.repository.modelbase;

import java.lang.reflect.Field;
import java.util.Collection;

import com.googlecode.objectify.Ref;

public class ForeignKeyConstraint {    


    public ForeignKeyConstraint(Field field, Class<?> declaringClass)
    {
        this.field = field;
        this.declaringClass = declaringClass;
    }            
            
    public String getFilterPath()
    {
        if (Boolean.TRUE.equals(isIterableField()))
        {
            return String.format("%s.%s = ", getFieldName(), getFieldType().getSimpleName());
        }
        else
        {
            return String.format("%s = ", getFieldName());
        }
        
    }

    public String getFieldName()
    {
        return field.getName();
    }

    public Boolean fieldTypeUsesRef()
    {
        return field.getType().equals(Ref.class);
    }

    public ForeignKey.ConstraintAction getAction()
    {
        return field.getAnnotation(ForeignKey.class).action();
    }

    public Class<?> getFieldType()
    {
        return field.getAnnotation(ForeignKey.class).value();
    }

    public Class<?> getFieldContainer()
    {
        return declaringClass;
    }

    public Boolean isIterableField() {
		for (Class<?> interfaceClass: field.getType().getInterfaces()) { 
			if(interfaceClass.equals(Collection.class)) 
			{
				return true;
			}						
		}
		return false;
      }
      
      private final Field field;
      private final Class<?> declaringClass;


}