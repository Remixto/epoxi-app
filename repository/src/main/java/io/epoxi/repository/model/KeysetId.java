package io.epoxi.repository.model;

import lombok.Getter;
import lombok.NonNull;

public class KeysetId {
    
    @Getter @NonNull
    private final String projectName;

    @Getter @NonNull
    private final String keysetName;
    
    private KeysetId (@NonNull String projectName, @NonNull String keysetName)
    {
        this.projectName = projectName;
        this.keysetName = keysetName;
    }

    public static KeysetId of(@NonNull String projectName, @NonNull String keysetName)
    {
        return new KeysetId(projectName, keysetName);        
    }

    @Override 
    public String toString()
    {       
        return String.format("%s.%s", projectName, keysetName);
    }

}