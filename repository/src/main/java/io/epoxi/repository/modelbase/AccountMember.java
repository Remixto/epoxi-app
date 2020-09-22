package io.epoxi.repository.modelbase;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Index;
import io.epoxi.repository.model.Account;
import lombok.NonNull;

import java.util.Optional;

public abstract class AccountMember<T> extends EngineMember<T> {

    @JsonIgnore
    protected AccountMember()
    {}

    @JsonIgnore
    protected AccountMember (String name)
    {
        this.name = Optional.ofNullable(name).orElseThrow();
    }

    @ForeignKey(value=Account.class, action= ForeignKey.ConstraintAction.CASCADE)
    @Index
    protected Ref<Account> account = null;


    public Long getAccountId() {        
        return account.getKey().getId();
    }
    
    public Account getAccount() {        
        return this.account.get();
    }

    
    protected void setAccount(@NonNull Account account) {        
        setAccount(account.getId());
    }   

    @JsonIgnore
    protected void setAccount(@NonNull Long accountId) {
        this.account = Account.createRef(accountId);
    }

    @Override
    public String toString() {
        String sb = "class AccountMember {\n" +
                "    id: " + toIndentedString(id) + "\n" +
                "    name: " + toIndentedString(name) + "\n" +
                "}";
        return sb;
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private static String toIndentedString(java.lang.Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}