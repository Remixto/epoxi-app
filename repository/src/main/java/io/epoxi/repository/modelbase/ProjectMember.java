package io.epoxi.repository.modelbase;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Index;
import io.epoxi.repository.model.Project;
import io.swagger.annotations.ApiModelProperty;
import lombok.NonNull;

import java.util.Optional;

public abstract class ProjectMember<T> extends AccountMember<T>  {

    @JsonIgnore
    protected ProjectMember ()
    {
    }

    @JsonIgnore
    protected ProjectMember (Long projectId)
    {        
        setProject(Optional.ofNullable(projectId).orElseThrow());           
    }

    protected ProjectMember (Long projectId, String name)
    {        
        setProject(Optional.ofNullable(projectId).orElseThrow());
        this.name = Optional.ofNullable(name).orElseThrow(); 
        
    }
   
    @ApiModelProperty(value = "the project to which the source belongs.  If not specified, this source will be shared between all projects within the account.")
    /*
     * the project to which the source belongs. If not specified, this source will
     * be shared between all projects within the account.
     */
    @ForeignKey(value=Project.class, action= ForeignKey.ConstraintAction.CASCADE)
    @Index
    protected Ref<Project> project = null;


    /**
     * the project to which the source belongs. If not specified, this source will
     * be shared between all projects within the account.
     * 
     * @return project
     **/
    @JsonProperty("project")
    public Project getProject() {
        return project.get();
    }

    protected void setProject(@NonNull Project project) {           
        this.project = Project.createRef(project.id);
        super.setAccount(project.getAccountId());
    }
    
    @JsonIgnore
    protected void setProject(@NonNull Long projectId) {
        this.project = Project.createRef(projectId);
        super.setAccount(this.project.get().getAccountId());
    }

    @Override
    public String toString() {
        return "class Source {\n" +
                "    id: " + toIndentedString(id) + "\n" +
                "    projectId: " + toIndentedString(getProjectId()) + "\n" +
                "    name: " + toIndentedString(name) + "\n" +
                "}";
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

    public Long getProjectId()
    {       
        if (project != null) return project.getKey().getId();
        return null;
        
    }
}
