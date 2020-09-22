package io.epoxi.app.repository.model;

import io.epoxi.app.repository.AccountRepository;
import io.epoxi.app.repository.modelbase.EngineMember;
import io.epoxi.app.repository.modelbase.EngineRepository;
import io.epoxi.app.repository.modelbase.ProjectMember;
import io.epoxi.app.repository.modelbase.RefBuilder;
import io.swagger.annotations.ApiModel;

import io.swagger.annotations.ApiModelProperty;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigquery.TableId;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Entity;

import io.epoxi.app.repository.Config;


/**
 * details about the specific type and location of the data to be ingested.
 **/
@Entity
@ApiModel(description = "details about the specific type and location of the data to be ingested.")
public class Source extends ProjectMember<Source> {

    private Source (){
        super();
    }

    private Source (String name, SourceType sourceType)
    {
        this.name = name;
        this.sourceType = sourceType;
    }  

    @ApiModelProperty(required = true, value = "The type of source that data will be extracted from.  The value must match a list of Sources supported by Epoxi. See http://epoxi.io/[TODO] for details")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("sourceType")}))
    @Setter
    private SourceType sourceType = SourceType.of("BQ");

    @ApiModelProperty(value = "The properties of the source. Valid properties are : config (a json object used by taps), catalog (a json object used by taps), and tableId (used when the source is a database tableId managed by the etlEngine). tableId must be specified in the form [projectId].datasetId.tableName where projectId is optional. If projectId is not specified, the project Id to which the source belongs will be assumed. In the case that the source does not specify a project Id, it must be included in the tableId property")
    @Getter(onMethod=@__({@JsonProperty("properties")}))
    @Setter
    private Metadata properties = null;


    @Override
    public String toString() {
        return EngineMember.toString(this);
    }

    /**
     * When the SourceType is sql queryable, return the source table. 
     * When the SourceType is not sql queryable, return the cache table 
     * that will hold the data streamed from the source     
     */
    @JsonIgnore
    public TableId getTableId() {           
        
        return TableId.of(getProject().getName(), Config.SOURCE_DATASET_NAME, String.format("%s%s", Config.SOURCE_VIEW_PREFIX, getName()));
    }

    @Override
    @JsonIgnore
    public EngineRepository<?> getRepository() {

        AccountRepository accountRepository = AccountRepository.of(getAccountId());
        if (getProjectId() ==null)
            return accountRepository.getChildRepository(AccountRepository.ChildMember.SOURCE);
        else
            return accountRepository.getSourceRepository(getProjectId());
    }

    public static Ref<Source> createRef(Long id) {
        RefBuilder<Source> builder = new RefBuilder<>();
        return builder.createRef(Source.class, id);		
    }    

    public static Builder newBuilder() { 
       
        return name -> sourceType -> new OptionalsBuilder().setName(name).setSourceType(sourceType);       
    }

    
    /******************************************
     * Builder.  A functional interface with a single method (withName) that starts the build.
     */

    @FunctionalInterface
    public interface Builder {

        SourceBuilder withName(final String name);

        interface SourceBuilder
        {
            OptionalsBuilder withSourceType(final SourceType sourceType);
        }
    }
   
    public static class OptionalsBuilder
    {   
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String name;
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) SourceType sourceType;
        
        @Accessors(chain = true) @NonNull @Setter Project project;
        @Accessors(chain = true) @NonNull @Setter() Metadata properties;        
        
        public Source build(){

            Source obj = new Source(name, sourceType);
            if (project !=null) obj.setProject(project); 
            obj.setProperties(properties);           

            return obj;
        }		
    }
}
