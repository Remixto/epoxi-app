package io.epoxi.repository.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.pubsub.v1.PubsubMessage;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Index;
import io.epoxi.cloud.pubsub.Writable;
import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.modelbase.*;
import io.epoxi.repository.validation.NameNotNullCondition;
import io.epoxi.repository.validation.ProjectNotNullCondition;
import io.epoxi.repository.event.Event;
import io.epoxi.repository.modelbase.*;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * details about the specific ingestion of data from a stream attached to a
 * source.
 **/
@Entity
@ApiModel(description = "details about the specific ingestion of data from a stream attached to a source.")
public class Ingestion extends ProjectMember<Ingestion> implements Writable {

    private Ingestion() {
        validator.add(new NameNotNullCondition<>(this));
        validator.add(new ProjectNotNullCondition<>(this));
    }

    private Ingestion(String name) {
        this();
        this.name = name;
    }

    @ApiModelProperty(required = true, value = "an array of the streams that will comprise the data to be ingested.  At least one stream is required.  If additional streams are specified, they will be unioned to the stream output in the order they appear in the array.  If duplicate fields are found in each stream, data in earlier fields will be overwritten by data found in later streams.")
    @ForeignKey(Stream.class)
    @Index
    @NonNull
    /*
     * an array of the streams that will comprise the data to be ingested. At least
     * one stream is required. If additional streams are specified, they will be
     * unioned to the stream output in the order they appear in the array. If
     * duplicate fields are found in each stream, data in earlier fields will be
     * overwritten by data found in later streams.
     */
    private final List<Ref<Stream>> streams = new ArrayList<>();

    @ApiModelProperty(required = true, value = "the id of a target where streamed data will be stored")
    @ForeignKey(Target.class)
    @Index   
    @NonNull 
    /*
     * the id of a target where streamed data will be stored
     */
    private Ref<Target> target;   


    @ApiModelProperty(required = true, value = "the type of ingestion required. At the current time, all ingestions are of type Standard.")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("ingestionType")}))
    @Setter
    /*
     * the type of ingestion required. At the current time, all ingestions are of
     * type Standard.
     */
    private IngestionType ingestionType;


    @ApiModelProperty(value = "a hint as to the order of ingestion for a given source.  Ingestions with lower numbers have priority.")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("weight")}))
    @Setter
     /*
     * a hint as to the order of ingestion for a given source. Ingestions with lower
     * numbers have priority.
     */
    private Integer weight;
 

    @ApiModelProperty(value = "determines a threshold of permitted deletes from the target table based on incoming data from the source.  When the threshold is exceeded, the ingestion will not complete and instead generate an error. If no value is set, the default is 1")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("deleteThreshold")}))
    @Setter  
    /*
     * determines a threshold of permitted deletes from the target table based on
     * incoming data from the source. When the threshold is exceeded, the ingestion
     * will not complete and instead generate an error. If no value is set, the default is 1
     */
    private Float deleteThreshold;
    

    @ApiModelProperty(value = "a hint that specifies whether the source contain a full, partial or delete only set of rows")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("updateType")}))
    @Setter 
    /*
     * a hint that specifies whether the source contain a full, partial or delete only set of rows
     */
    private ReplicationType updateType;


    @ApiModelProperty(value = "determines the way rows are created within the target.  TypeI will 1) add new values, 2) overwrite existing values. TypeII will 1) append new and existing data as new rows and 2) mark previous existing rows as expired.  Deleted rows will be marked as expired for all scdTypes.")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("scdType")}))
    @Setter
    /*
     * determines the way rows are created within the target. TypeI will 1) add new
     * values, 2) overwrite existing values. TypeII will 1) append new and existing
     * data as new rows and 2) mark previous existing rows as expired. Deleted rows
     * will be marked as expired for all scdTypes.
     */
    private ScdType scdType;

    /**
     * an array of the streams that will comprise the data to be ingested. At least
     * one stream is required. If additional streams are specified, they will be
     * unioned to the stream output in the order they appear in the array. If
     * duplicate fields are found in each stream, data in earlier fields will be
     * overwritten by data found in later streams.
     * 
     * @return streams
     **/
    @JsonProperty("streams")
    public synchronized List<Stream> getStreams() {

        List<Stream> items = new ArrayList<>();
        for(Ref<Stream> ref : streams)
        {
            items.add(ref.get());
        }   
        return items;
    }

    public synchronized void setStreams(Stream... streams) {
       
        this.streams.clear();
        for(Stream stream : streams)
        {
            addStreamsItem(stream);   
        }
    }    

    @JsonIgnore
    public synchronized void setStreams(Long... streamIds) {
       
        streams.clear();
        for(Long id : streamIds)
        {
            addStreamsItem(id);   
        }
    }    
    
    public Ingestion addStreamsItem(@NonNull Long streamId) {
        
        Ref<Stream> ref = Stream.createRef(streamId);
        this.streams.add(ref);
        return this;
    }

    public Ingestion addStreamsItem(@NonNull Stream stream) {
        
        return addStreamsItem(stream.getId());
    }
    
    public Stream getStreamsItem(@NonNull Long streamId) {
        
        Stream foundStream = null;
        for(Ref<Stream> ref : streams)
        {
            if (ref.getKey().getId() == streamId)
            {
                foundStream = ref.get();
                break;
            }       
        }   
        return foundStream;
    }
    
    public Stream getStreamsItem(@NonNull String name) {
        
        Stream foundStream = null;
        for(Ref<Stream> ref : streams)
        {
            if (ref.getKey().getName().equals(name))
            {
                foundStream = ref.get();
                break;
            }
        }   
        return foundStream;
    }

    /**
     * Get target
     * 
     * @return target
     **/
    @JsonProperty("target")
    public Target getTarget() { 
        
        if (target==null)
        {
            //return a default value
            return Target.newBuilder()
            .withName(name)
            .setProject(getProject())
            .build();
        }
        return target.get();       
    }

    public void setTarget(@NonNull Target target) { 

        this.target = Target.createRef(target.getId());       
     }

    @JsonIgnore
    public void setTarget(@NonNull Long targetId) { 

       this.target = Target.createRef(targetId);
    }   

    @Override
    public String toString() {
        return EngineMember.toString(this);
    }

    @JsonIgnore
    public PubsubMessage toMessage()
    {
        KeysetId keysetId = getKeysetId();
        Event event = new Event(keysetId);

        return event.toMessage();
    }

    @Override
    public void setProject(@NonNull Project project) {   
             
        super.setProject(project.getId());
    }
    
    @Override
    public void setProject(@NonNull Long projectId) {          
        super.setProject(projectId);
    }

    /*
    Get the source data, which will always be the source of the first stream (by design)
    */
    public Source getSource()
    {
        if (streams.isEmpty()) return null;
        return streams.get(0).get().getSource();
    } 

    @JsonIgnore
    public KeysetId getKeysetId()
    {
        return KeysetId.of(getProject().getName(), name);
    }   

    public String getKeyFieldId()  //TODO:  write a source query generation script that hashes the keyFields together and return that under an alias that matches the return String here.
    {       
        return target.get().getKeyFieldId();
    }

    public static Ref<Ingestion> createRef(Long id) {
		return new RefBuilder<Ingestion>().createRef(Ingestion.class, id);
	}
    
    @Override
    @JsonIgnore
    public MemberRepository<?> getRepository() {
        AccountRepository accountRepository = AccountRepository.of(this.getAccountId());
        return accountRepository.getIngestionRepository(getProjectId());
    }   

    public static Builder newBuilder() { 
       
       return name -> streams -> target -> new OptionalsBuilder().setName(name).setStreamIds(streams).setTargetId(target);
    }

    /******************************************
     * Builder.  A functional interface with a single method (withName) that starts the build.
     */

    @FunctionalInterface
    public interface Builder {
      
        StreamBuilder withName(String name);

        interface StreamBuilder {
            TargetBuilder fromStreams(final List<Long> streamIds);
        }

        interface TargetBuilder {
            OptionalsBuilder toTarget(final Long targetId);
        }
    }

    public static class OptionalsBuilder
    {     
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String name;
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) List<Long> streamIds;
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) Long targetId; 

        @Accessors(chain = true) @NonNull @Setter Project project;
        @Accessors(chain = true) @NonNull @Setter() IngestionType ingestionType = IngestionType.STANDARD;
        @Accessors(chain = true) @NonNull @Setter() Integer weight = 100;
        @Accessors(chain = true) @NonNull @Setter() ScdType scdType = ScdType.TYPE_I;
        @Accessors(chain = true) @NonNull @Setter() ReplicationType updateType = ReplicationType.PARTIAL;
        @Accessors(chain = true) @NonNull @Setter() Float deleteThreshold = 1F;

        public Ingestion build(){

            Ingestion obj = new Ingestion(name);
            obj.setStreams(streamIds.toArray(new Long[streamIds.size()]));
            obj.setTarget(targetId);
            if (project !=null) obj.setProject(project);            
            obj.setIngestionType(ingestionType);
            obj.setWeight(weight);
            obj.setScdType(scdType);
            obj.setUpdateType(updateType);
            obj.setDeleteThreshold(deleteThreshold);

            return obj;
        }		
    }

    @XmlType(name = "IngestionType") @XmlEnum()
    public enum IngestionType {

        @XmlEnumValue("standard") STANDARD("standard");

        private final String value;

        IngestionType(String v) {
            value = v;
        }

        public String value() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public static IngestionType fromValue(String v) {
            for (IngestionType b : IngestionType.values()) {
                if (String.valueOf(b.value).equals(v)) {
                    return b;
                }
            }
            return null;
        }
    }

    @XmlType(name = "UpdateType") @XmlEnum()
    public enum ReplicationType {

        @XmlEnumValue("FULL")  FULL("FULL"),
        @XmlEnumValue("DELETE") DELETE("DELETE"),
        @XmlEnumValue("PARTIAL") PARTIAL("PARTIAL");

        private final String value;

        ReplicationType(String v) {
            value = v;
        }

        public String value() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public static ReplicationType fromValue(String v) {
            for (ReplicationType b : ReplicationType.values()) {
                if (String.valueOf(b.value).equals(v)) {
                    return b;
                }
            }
            return null;
        }
    }

    @XmlType(name = "ScdType") @XmlEnum()
    public enum ScdType {

        @XmlEnumValue("type_I") TYPE_I("type_I"),
        @XmlEnumValue("type_II") TYPE_II("type_II");

        private final String value;

        ScdType(String v) {
            value = v;
        }

        public String value() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public static ScdType fromValue(String v) {
            for (ScdType b : ScdType.values()) {
                if (String.valueOf(b.value).equals(v)) {
                    return b;
                }
            }
            return null;
        }
    }

      
}
