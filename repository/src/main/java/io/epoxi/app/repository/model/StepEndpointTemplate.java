package io.epoxi.app.repository.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Index;
import io.epoxi.app.repository.AccountRepository;
import io.epoxi.app.repository.modelbase.EngineMember;
import io.epoxi.app.repository.modelbase.EngineRepository;
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

/**
* config info that describes an endpoint template for a specific stepType
 **/
@Entity
@ApiModel(description = "config info that describes an endpoint template for a specific stepType")
public class StepEndpointTemplate  extends EngineMember<StepEndpointTemplate> {
  
  private StepEndpointTemplate (){
    super(); 
  }

  private StepEndpointTemplate (String name, EndpointType endpointType, StepType stepType, String tableId)
  {
    super(name); 
    this.endpointType = endpointType;
    this.stepType = stepType;
    this.tableId = tableId;   
  }

  @ApiModelProperty(required=true, value = "the endpoint of the pipeline")
  @NonNull
  @Getter(onMethod=@__({@JsonProperty("endpointType")}))
  @Setter 
  /*
    * the endpoint of the pipeline
   */
   private EndpointType endpointType;

 
  @ApiModelProperty(required=true, value = "the StepType of the Pipeline")
  @Index
  @NonNull
  @Getter(onMethod=@__({@JsonProperty("stepType")}))
  @Setter 
  /*
   * the StepType of the Pipeline
  */
  private StepType stepType;


  @ApiModelProperty(required=true, value = "the table that define the endpoint of the pipeline")
  @NonNull
  @Getter(onMethod=@__({@JsonProperty("tableId")}))
  @Setter 
  /*
   * the table that define the endpoint of the pipeline
  */
  private String tableId;


  @ApiModelProperty(value = "the keyField defining the data that will be passed on the pipeline")
  @Getter(onMethod=@__({@JsonProperty("keyFieldId")}))
  @Setter 
  /*
   * the keyField defining the data that will be passed on the pipeline
  */
  private String keyFieldId = null;



  @Override
  public String toString() {
    return EngineMember.toString(this);
  }

  @Override
  @JsonIgnore
  public EngineRepository<?> getRepository() {
      return new AccountRepository().getStepEndpointTemplateRepository();
  }
  

  @XmlType(name="EndpointType") @XmlEnum()
  public enum EndpointType {

    @XmlEnumValue("SOURCE") SOURCE("SOURCE"),
    @XmlEnumValue("TARGET") TARGET("TARGET");


      private final String value;

      EndpointType (String v) {
          value = v;
      }

      public String value() {
          return value;
      }

      @Override
      public String toString() {
          return String.valueOf(value);
      }

      public static EndpointType fromValue(String v) {
          for (EndpointType b : EndpointType.values()) {
              if (String.valueOf(b.value).equals(v)) {
                  return b;
              }
          }
          return null;
      }
  }

  public static Builder newBuilder() { 
       
    return name -> endpointType -> stepType -> tableId -> new OptionalsBuilder()
                                                                        .setName(name)
                                                                        .setEndpointType(endpointType)
                                                                        .setStepType(stepType)
                                                                        .setTableId(tableId);       
  } 


  /******************************************
   * Builder.  A functional interface with a single method (withName) that starts the build.
   */

  @FunctionalInterface
  public interface Builder {

    EndpointTypeBuilder withName(final String name);

      interface EndpointTypeBuilder
      {
        StepTypeBuilder withEndpointType(final EndpointType endpointType);
      }

      interface StepTypeBuilder
      {
        TableIdBuilder withStepType(final StepType stepType);
      }

      interface TableIdBuilder
      {
          OptionalsBuilder withTableId(final String tableId);
      }
  }

  public static class OptionalsBuilder
  {   
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String name;
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) EndpointType endpointType;
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) StepType stepType;
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String tableId;

      @Accessors(chain = true) @Setter String keyFieldId;

      public StepEndpointTemplate build(){

        StepEndpointTemplate obj = new StepEndpointTemplate(name, endpointType, stepType, tableId);   
        
        obj.setKeyFieldId(keyFieldId);
    
        return obj;
      }		
  }
}

