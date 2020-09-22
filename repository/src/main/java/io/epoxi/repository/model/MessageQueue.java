package io.epoxi.repository.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Index;
import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.modelbase.EngineMember;
import io.epoxi.repository.modelbase.EngineRepository;
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
import java.util.Comparator;

/**
 * config info that associates a queue with a specific queueType and StepType
 **/
@Entity
@ApiModel(description = "config info that associates a queue with a specific queueType and StepType")
public class MessageQueue extends EngineMember<MessageQueue> {

  private MessageQueue() {
    super();
  }

  private MessageQueue(String name) {
    super(name);  
  }

  @ApiModelProperty(value = "the purpose of the queue")
  @Index
  @NonNull
  @Getter(onMethod=@__({@JsonProperty("queueType")}))
  @Setter
  /*
   * the purpose of the queue
   */
  private QueueType queueType = null;


  @ApiModelProperty(value = "the StepType of the queue")
  @Index
  @NonNull
  @Getter(onMethod=@__({@JsonProperty("stepType")}))
  @Setter
  /*
   * the StepType of the queue
   */
  private StepType stepType = null;


  @ApiModelProperty(value = "the pubsub queue")
  @NonNull
  @Getter(onMethod=@__({@JsonProperty("queue")}))
  @Setter
  /*
   * the pubsub queue
   */
  private Queue queue = null;


  @ApiModelProperty(value = "a weight that specifies the order of processing.  Queues with lower weight will be processed first")
  @Index
  @NonNull
  @Getter(onMethod=@__({@JsonProperty("weight")}))
  @Setter  
  /*
   * a weight that specifies the order of processing.  
   * Queues with lower weight will be processed first
   */
  private Integer weight = 100;

  @Override
  public String toString() {
    return EngineMember.toString(this);
  }

  @Override
  @JsonIgnore
  public EngineRepository<?> getRepository() {
      return new AccountRepository().getMessageQueueRepository();
  }

  public static class SortByWeight implements Comparator<MessageQueue>
  { 
      // Used for sorting in ascending order of 
      // roll name 
      public int compare(MessageQueue a, MessageQueue b) 
      { 
          return a.weight - b.weight; 
      }
  }

  @XmlType(name = "QueueType") @XmlEnum()
  public enum QueueType {

      @XmlEnumValue("IN") IN("IN"),
      @XmlEnumValue("OUT") OUT("OUT"),
      @XmlEnumValue("ERROR") ERROR("ERROR");

      private final String value;

      QueueType(String v) {
        value = v;
      }

      public String value() {
        return value;
      }

      @Override
      public String toString() {
        return String.valueOf(value);
      }

      public static QueueType fromValue(String v) {
        for (QueueType b : QueueType.values()) {
          if (String.valueOf(b.value).equals(v)) {
            return b;
          }
        }
        return null;
      }
  }

  public static Builder newBuilder() { 
       
    return name -> queueType -> stepType -> queuePath -> new OptionalsBuilder()
                                                                        .setName(name)
                                                                        .setQueueType(queueType)
                                                                        .setStepType(stepType)
                                                                        .setQueuePath(queuePath);      
  }


  /******************************************
   * Builder.  A functional interface with a single method (withName) that starts the build.
   */

  @FunctionalInterface
  public interface Builder {

    QueueTypeBuilder withName(final String name);

    interface QueueTypeBuilder
    {
      StepTypeBuilder withQueueType(final QueueType queueType);
    }

    interface StepTypeBuilder
    {
      QueuePathBuilder withStepType(final StepType stepType);
    }

    interface QueuePathBuilder
    {
        OptionalsBuilder withQueuePath(final String queuePath);
    }
  }

  public static class OptionalsBuilder
  {   
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String name;
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) QueueType queueType;
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) StepType stepType;
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String queuePath;

      @Accessors(chain = true) @NonNull @Setter Integer weight;
      public MessageQueue build(){

          MessageQueue obj = new MessageQueue(name);        
          obj.setQueueType(queueType); 
          obj.setStepType(stepType);      
          obj.setQueue(Queue.of(queuePath));
          if (weight !=null) obj.setWeight(weight);
      
          return obj;
      }		
  }
   
}

