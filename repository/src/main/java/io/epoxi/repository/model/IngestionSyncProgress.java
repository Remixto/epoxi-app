package io.epoxi.repository.model;

import lombok.Getter;

public class IngestionSyncProgress {

    @Getter
    int completed;

    @Getter
    int total;
}