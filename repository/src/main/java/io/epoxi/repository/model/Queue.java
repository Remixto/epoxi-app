package io.epoxi.repository.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Index;
import io.epoxi.repository.modelbase.EngineMember;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Optional;

/**
* info about a Pubsub queue
 **/
@Entity
@ApiModel(description = "info about a pubsub queue")
public class Queue  {

  private Queue ()
  {}

  private Queue (String path)
  {
    this.path = Optional.ofNullable(path).orElseThrow();
  }

  @ApiModelProperty(value = "the Id of the queue")
  @Getter(onMethod=@__({@JsonProperty("id")}))
  @Setter
  @Id private Long id = null;

  @ApiModelProperty(value = "the path of the Pubsub queue")
  @Index
  @NonNull
  @Getter(onMethod=@__({@JsonProperty("path")}))
  @Setter
  private String path;

  @Override
  public String toString() {
    return EngineMember.toString(this);
  }

  public static Queue of (String path)
  {
    return new Queue(path);
  }

}

