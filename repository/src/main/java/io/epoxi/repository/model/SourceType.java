package io.epoxi.repository.model;

import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.modelbase.EngineMember;
import io.epoxi.repository.modelbase.EngineRepository;
import io.swagger.annotations.ApiModel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.googlecode.objectify.annotation.Entity;

/**
 * a type of source that can be consumed. Examples of a sourceType could be a
 * REST api, a database, a CSV file or a spreadsheet.
 **/
@Entity
@ApiModel(description = "a type of source that can be consumed.  Examples of a sourceType could be a REST api, a database, a CSV file or a spreadsheet.")
public class SourceType extends EngineMember<SourceType> {

    private SourceType (){
        super(); 
    }

    private  SourceType (String name)
    {
        super(name);
    }

    @Override
    public String toString() {
        return EngineMember.toString(this);
    }

    public Boolean isSqlQueryable()
    {
        return (getName().equals("BQ"));
    }   

    public Source createSource(Project project, String name)
    {
        return createSource(project, name, null);
    }

    public Source createSource(Project project, String name, Metadata properties)
    {
        return Source.newBuilder()
                            .withName(name)
                            .withSourceType(this)
                            .setProject(project)
                            .setProperties(properties)
                            .build();
    }

    public static SourceType of (String name)
    {
        return new SourceType(name);
    }

    @Override
    @JsonIgnore
    public EngineRepository<?> getRepository() {
        return new AccountRepository().getSourceTypeRepository();
    }
}
