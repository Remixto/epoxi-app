package io.epoxi.app.repository.validation;

import java.util.Optional;

import io.epoxi.app.util.validation.ValidationCondition;
import io.epoxi.app.repository.modelbase.ProjectMember;


public class ProjectNotNullCondition<T> implements ValidationCondition {
    
    final ProjectMember<T> member;

    public ProjectNotNullCondition(ProjectMember<T> member) {  
        this.member = member;      
    }

    @Override
    public Optional<String> validate() {
        if (member.getProjectId()!=null) 
            return Optional.empty();
        else
        {
            return Optional.of(String.format("Project cannot be null for object '%s'", member.toString()));
        }
    }
}