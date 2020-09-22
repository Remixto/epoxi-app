package io.epoxi.repository.model;

import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.modelbase.EngineMember;
import io.epoxi.repository.modelbase.EngineRepository;
import io.epoxi.repository.modelbase.RefBuilder;
import io.swagger.annotations.ApiModel;

import io.swagger.annotations.ApiModelProperty;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Entity;

/**
 * a account under which projects are created. These are typically created for a
 * single company. Multiple users can be added to the account with varied
 * permissions to access projects managed by the account.
 **/
@Entity
@ApiModel(description = "an account under which projects are created.  These are typically created for a single company.  Multiple users can be added to the account with varied permissions to access projects managed by the account.")
public class Account extends EngineMember<Account> {

    private Account ()
    {
        super();
    }

    private Account (String accountName, String firstName, String lastName)
    {
        super(accountName);
        this.firstName = firstName;
        this.lastName = lastName;
    }

    @ApiModelProperty(value = "the company name associated with the account.")
    @Getter(onMethod=@__({@JsonProperty("company")}))
    @Setter
    /*
     * the company name associated with the account.
     */
    private String company = null;

    @ApiModelProperty(required = true, value = "the first name of the contact associated with the account.")

    @Getter(onMethod=@__({@JsonProperty("firstName")}))
    @NonNull
    @Setter
    /*
     * the first name of the contact associated with the account.
     */
    private String firstName;

    @ApiModelProperty(required = true, value = "the last name of the contact associated with the account.")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("lastName")}))
    @Setter
    /*
     * the last name of the contact associated with the account.
     */
    private String lastName;

    @Override
    public String toString() {
        return EngineMember.toString(this);
    }

    public static Ref<Account> createRef(Long id) {
		return new RefBuilder<Account>().createRef(Account.class, id);
	}

    @Override
    @JsonIgnore
    public EngineRepository<?> getRepository() {
        return AccountRepository.of(this.getId());
    }

    public static Builder newBuilder() {

        return accountName ->  firstName -> lastName -> new OptionalsBuilder().setAccountName(accountName).setFirstName(firstName).setLastName(lastName);
    }


    /******************************************
     * Builder.  A functional interface with a single method (withName) that starts the build.
     */

    @FunctionalInterface
    public interface Builder {

        FirstNameBuilder withAccountName(final String accountName);

        interface FirstNameBuilder
        {
            LastNameBuilder withFirstName(final String firstName);
        }

        interface LastNameBuilder
        {
            OptionalsBuilder withLastName(final String lastName);
        }
    }

    public static class OptionalsBuilder
    {
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String accountName;
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String firstName;
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String lastName;

        @Accessors(chain = true) @NonNull @Setter() String company;

        public Account build(){

            Account obj = new Account(accountName, firstName, lastName);
            obj.setCompany(company);

            return obj;
        }
    }
}
