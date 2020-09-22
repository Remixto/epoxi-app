package io.epoxi.app.repository.modelbase;

import com.googlecode.objectify.Key;
import com.googlecode.objectify.Ref;

public class RefBuilder<T> {
   
	public RefBuilder()
	{
	}

    public Ref<T> createRef(Key<T> key) {
		return ObjectifyRegistry.run(() -> Ref.create(key));	
	}

	public Ref<T> createRef(Class<T> clazz, Long id) {
		Key<T> key = Key.create(clazz, id);
		return createRef(key);
	}
    
    
}