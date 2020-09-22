package io.epoxi.repository.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Index;
import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.exception.NotFoundException;
import io.epoxi.repository.modelbase.*;
import io.epoxi.util.sqlbuilder.types.RowField;
import io.epoxi.util.sqlbuilder.types.SqlFieldMap;
import io.epoxi.util.sqlbuilder.types.SqlTable;
import io.epoxi.repository.modelbase.*;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Optional;

/**
 * details about a particular data set that exists within a source that can be
 * ingested. Streams specify a source from where the data can be obtained, as
 * well as a view that specifies what fields are consumed as well as how they
 * be presented in the target.
 **/
@Entity
@ApiModel(description = "details about a particular data set that exists within a source that can be ingested.  Streams specify a source from where the data can be obtained, as well as a view that specifies what fields are consumed as well as how they be presented in the target.")
public class Stream extends ProjectMember<Stream> {

    private Stream (){
        super();
    }

    private Stream (Long sourceId)
    {
        this();
        setSource(sourceId);
    }

    private Stream (String name, Long sourceId)
    {
        this();
        this.name = name;
        setSource(sourceId);
    }
    
    @ApiModelProperty(required = true, value = "the source object that holds the data returned by the stream")
    @ForeignKey(Source.class)
    @Index
    @NonNull
    /*
     * the source object that holds the data returned by the stream
     */
    private Ref<Source> source;

    @ApiModelProperty(value = "the schema that defines the fields returned within the stream")
    /*
     * the schema that defines the fields returned within the stream
     */
    private Schema schema = null;

    @ApiModelProperty(value = "the name of the field that will contain the unique hash of key fields.  This name should match the keyFieldId of the Ingestion.target")
    @NonNull 
    /*
     * the name of the field that will contain the unique hash of key fields.  This name should match the keyFieldId of the Ingestion.target.
     * @return
     */
    @Getter @Setter
    private String hashKeyFieldId;


    @ApiModelProperty(value = "when a value is specified, the stream will be returned as a single field called nestField, and where each field in the stream is a subfield")
    /*
     * when a value is specified, the stream will be returned as a single field called nestField, and where each field in the stream is a subfield
     * @return
     */
    @Getter @Setter
    private String nestFieldId; 
  


    /**
     * Get source
     * 
     * @return source
     **/
    @JsonProperty("source")
    public Source getSource()
    {        
        return source.get();
    }

    public void setSource(@NonNull Source source) {
        setSource(source.getId());
    }

    @JsonIgnore
    public void setSource(@NonNull Long sourceId) {       
        this.source = new RefBuilder<Source>().createRef(Source.class, sourceId);
    }

    public Stream source(Long sourceId) {
        setSource(sourceId);
        return this;
    }

    /**
     * Get schema
     * 
     * @return schema
     **/
    @JsonProperty("schema")
    public Optional<Schema> getSchema() {
        return Optional.ofNullable(schema);
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public void setProject(@NonNull Project project) {   
             
        super.setProject(project.getId());
    }
    
    @Override
    public void setProject(@NonNull Long projectId) {          
        super.setProject(projectId);
    }



    public static Ref<Stream> createRef(Long id) {
		return new RefBuilder<Stream>().createRef(Stream.class, id);
	}

    @Override
    public String toString() {
        return EngineMember.toString(this);
    }

   

    /**
     * When the source is a sql table, we can return the fields as a Sql Field Map.
     * The map returned will contain all fields in the source.  If a schema exists,
     * the fields will be filtered to only those that exist in the schema 
     * If the source type is invalid, the method will error
     * @return The SqlFieldMap for the Stream
     */
    @JsonIgnore
    public SqlFieldMap<String, RowField> getSqlFieldMap()
    {
        if (Boolean.FALSE.equals(getSource().getSourceType().isSqlQueryable()))
        {
            String msg = String.format("Cannot build SqlField map for a source whose type '%s' is not queryable.", getSource().getSourceType().getName());
            throw new IllegalStateException(msg);
        }

        //Get the source table (or throw if not exists)
        SqlTable table = SqlTable.of(getSource().getTableId());
        if (table==null) throw new NotFoundException(String.format("Cannot get SqlFieldMap from table '%s'. The table does not exist", getSource().getTableId()), getSource().getTableId());


        //If we have specified a list of fields to return grab them
        List<String> fieldPaths = null;
        if (getSchema().isPresent())
        {
            fieldPaths = getSchema().orElseThrow().getFieldPaths();
        }

        //If we have no valid schema, return table fields, else limit the map by the schema
        SqlFieldMap<String, RowField> rowFields;
        if (fieldPaths==null || fieldPaths.isEmpty())
        {
            rowFields = table.getFields();
        }
        else
        {
             //Examine the table and find the SqlFields that match those in the stream, based on the Path field
            SqlFieldMap<String, RowField> tableFields = table.getFields(fieldPaths);        

            //Loop through tableFields and build a final list.
            //For fields that have been where name and path are different (an alias), use the alias as the key in rowFields
            rowFields = new SqlFieldMap<>();
    
            for(Field field : schema.getFields())
            {       
                RowField currentField = tableFields.get(field.getPath());
                if (currentField == null) continue;
    
                if (!field.getPath().equals(field.getName()))
                {                                
                    currentField = currentField.clone(field.getName());
                }
               
                rowFields.put(currentField.getName(), currentField);            
            }
        }

        return rowFields;
    }

    public Boolean isAnonymous()
    {
        return (name == null);
    }    

    @Override
    @JsonIgnore
    public EngineRepository<?> getRepository() {

        AccountRepository accountRepository = AccountRepository.of(getAccountId());
        if (getProjectId() ==null)
            return accountRepository.getChildRepository(AccountRepository.ChildMember.STREAM);
        else
            return accountRepository.getStreamRepository(getProjectId());    
    }

    public static AnonymousBuilder newAnonymousBuilder() { 
       
        return source -> new OptionalsBuilder().setSource(source);       
    }

    public static NamedBuilder newNamedBuilder() { 
       
        return name -> source -> new OptionalsBuilder().setName(name).setSource(source);       
    }

    
    /******************************************
     * Builder.  A functional interface with a single method (withName) that starts the build.
     */

    @FunctionalInterface
    public interface AnonymousBuilder {      
        OptionalsBuilder fromSource(final Long sourceId);
    }

    @FunctionalInterface
    public interface NamedBuilder {
      
        SourceBuilder withName(final String name);

        interface SourceBuilder
        {
            OptionalsBuilder fromSource(final Long sourceId);
        }
    }

    public static class OptionalsBuilder
    {     
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) Long source;
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String name;
        
        @Accessors(chain = true) @NonNull @Setter String hashKeyFieldId;
        @Accessors(chain = true) @NonNull @Setter Project project;
        @Accessors(chain = true) @Setter Schema schema;        
        @Accessors(chain = true) @Setter String nestFieldId; 

        public Stream build(){

            Stream obj = new Stream(name, source);
            
            if (hashKeyFieldId==null) hashKeyFieldId = String.format("%s_Key", obj.getSource().getName());
            obj.setHashKeyFieldId(hashKeyFieldId);

            if (project !=null) obj.setProject(project);
            obj.setSchema(schema);
            obj.setNestFieldId(nestFieldId); 

            return obj;
        }		
    }

}
