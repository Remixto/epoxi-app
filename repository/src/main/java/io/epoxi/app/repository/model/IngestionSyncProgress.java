package io.epoxi.app.repository.model;

import lombok.Getter;

public class IngestionSyncProgress {

    @Getter
    int completed;

    @Getter
    int total;
}