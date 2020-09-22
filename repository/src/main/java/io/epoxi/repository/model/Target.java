package io.epoxi.repository.model;

import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.modelbase.EngineMember;
import io.epoxi.repository.modelbase.EngineRepository;
import io.epoxi.repository.modelbase.ProjectMember;
import io.epoxi.repository.modelbase.RefBuilder;
import io.swagger.annotations.ApiModel;

import io.swagger.annotations.ApiModelProperty;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import io.epoxi.repository.Config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigquery.TableId;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Entity;

/**
 * details about the specific location (typically a database) where the data
 * will reside after ingestion.
 **/
@Entity
@ApiModel(description = "details about the specific location (typically a database) where the data will reside after ingestion.")
public class Target extends ProjectMember<Target> {

    private Target (){
        super();         
    }

    private Target (String name, String keyFieldId)
    {
        this.name = name;
        this.keyFieldId = keyFieldId;     
    }    

    @ApiModelProperty(value = "Extended properties for the target")
    @Getter(onMethod=@__({@JsonProperty("properties")}))
    @Setter 
    /*
     * Extended properties for the target
     */
    private Metadata properties = null;

    @ApiModelProperty(value = "the name of the field that defines the primary key of the target.  When not specified, keyFieldId will be set to the name of the table appended with a suffix of '_Key'")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("keyFieldId")}))
    @Setter 
    /*
     * the name of the field that defines the primary key of the target. When not specified, keyFieldId will be set to the name of the table appended with a suffix of '_Key'"
     */
    private String keyFieldId;


    @Override
    public String toString() {
        return EngineMember.toString(this);
    }

    @JsonIgnore
    public TableId getTableId() {
        return TableId.of(getProject().getName(), Config.TARGET_DATASET_NAME, getName()); 
    }
    
    public static Ref<Target> createRef(Long id) {
		return new RefBuilder<Target>().createRef(Target.class, id);
	}

    @Override
    @JsonIgnore
    public EngineRepository<?> getRepository() {

        AccountRepository accountRepository = AccountRepository.of(getAccountId());
        if (getProjectId() ==null)
            return accountRepository.getChildRepository(AccountRepository.ChildMember.TARGET);
        else
            return accountRepository.getTargetRepository(getProjectId());
    }


    public static Builder newBuilder() { 
       
        return name -> new OptionalsBuilder().setName(name);       
    }

    
    /******************************************
     * Builder.  A functional interface with a single method (withName) that starts the build.
     */

    @FunctionalInterface
    public interface Builder {

        OptionalsBuilder withName(final String name);
    }
   
    public static class OptionalsBuilder
    {   
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String name;
         

        @Accessors(chain = true) @NonNull @Setter Project project;
        @Accessors(chain = true) @NonNull @Setter String keyFieldId;    

        public Target build(){

            if (this.keyFieldId == null) this.keyFieldId  = String.format("%s_Key", name); 

            Target obj = new Target(name, keyFieldId);        
            if (project !=null) obj.setProject(project);        

            return obj;
        }		
    }
}
