package io.epoxi.repository.modelbase;

import com.googlecode.objectify.Ref;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RefList<T> extends ArrayList<Ref<T>> {

	/**
     *
     */
    private static final long serialVersionUID = 8454991015874681492L;

    public List<Long> toIdList() {

        return this.stream()
                                .map(x -> x.getKey().getId())
                                .collect(Collectors.toList());
	}

}
