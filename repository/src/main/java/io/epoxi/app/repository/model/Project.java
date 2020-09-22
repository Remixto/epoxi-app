package io.epoxi.app.repository.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Entity;
import io.epoxi.app.repository.AccountRepository;
import io.epoxi.app.repository.modelbase.AccountMember;
import io.epoxi.app.repository.modelbase.EngineMember;
import io.epoxi.app.repository.modelbase.EngineRepository;
import io.epoxi.app.repository.modelbase.RefBuilder;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
/**
 * A project is a collection of sources, targets and ingestions that are used to
 * achieve the specific dataset(s) required.
 **/
@Entity
@ApiModel(description = "A project is a collection of sources, targets and ingestions that are used to achieve the specific dataset(s) required.")
public class Project extends AccountMember<Project> {

    private Project()
    {
        super();
    }

    private Project (Long accountId, String name, TargetType targetType)
    {
        super(name);
        this.setAccount(accountId);
        this.targetType = Optional.ofNullable(targetType).orElseThrow();
    }

    /**
     * the metadata of the project
     **/
    @Getter(onMethod=@__({@JsonProperty("metadata")}))
    @Setter
    @NonNull
    private Map<String, Metadata> metadata = new HashMap<>();

    @ApiModelProperty(required = true, value = "the target type of the project.  All ingestions targets must confirm to this target type.")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("targetType")}))
    @Setter
    /*
     * the target type of the project.  All ingestions targets must confirm to this target type.
     */
    private TargetType targetType = TargetType.BIGQUERY;

    public Project addMetadataItem(Metadata metadataItem) {
        metadata.put(metadataItem.getKey(), metadataItem);
        return this;
    }

    public Project removeMetadataItem(String key) {
        metadata.remove(key);
        return this;
    }

    @Override
    public String toString() {
        return EngineMember.toString(this);
    }

    @Override
    @JsonIgnore
    public EngineRepository<?> getRepository() {

        AccountRepository accountRepository = AccountRepository.of(getAccountId());
        return accountRepository.getProjectRepository();
    }

    public static Project of(Long accountId, String name, TargetType targetType)
    {
      return new Project(accountId, name, targetType);
    }

    public static Ref<Project> createRef(Long id) {
		return new RefBuilder<Project>().createRef(Project.class, id);
	}

    @XmlType(name="TargetType") @XmlEnum()
    public enum TargetType {

        @XmlEnumValue("BigQuery") BIGQUERY("BigQuery");

        private final String value;

        TargetType (String v) {
            value = v;
        }

        public String value() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public static TargetType fromValue(String v) {
            for (TargetType b : TargetType.values()) {
                if (String.valueOf(b.value).equals(v)) {
                    return b;
                }
            }
            return null;
        }
    }

    public static Builder newBuilder() {

        return name -> accountId -> targetType -> new OptionalsBuilder().setName(name).setAccountId(accountId).setTargetType(targetType);
    }


    /******************************************
     * Builder.  A functional interface with a single method (withName) that starts the build.
     */

    @FunctionalInterface
    public interface Builder {

        AccountBuilder withName(final String name);

        interface AccountBuilder
        {
            TargetTypeBuilder withAccountId(final Long accountId);
        }

        interface TargetTypeBuilder
        {
            OptionalsBuilder withTargetType(final TargetType targetType);
        }
    }

    public static class OptionalsBuilder
    {
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String name;
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) Long accountId;
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) TargetType targetType;

        public Project build(){

            return new Project(accountId, name, targetType);

        }
    }


}
