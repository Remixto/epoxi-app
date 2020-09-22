package io.epoxi.app.repository.validation;

import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.TestDataFactory;
import io.epoxi.app.repository.model.Ingestion;
import io.epoxi.app.repository.modelbase.EngineMember;
import io.epoxi.app.util.validation.InvalidStateException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ValidationTest {

    @BeforeAll
    public static void setUp() {

    }

    @Test
    public void ValidIngestionTest()
    {
        Ingestion validIngestion = getValidTestIngestion();
        Assertions.assertDoesNotThrow(validIngestion::validOrThrow);
    }


    @Test
    public void InvalidIngestionTest()
    {
        //Get an ingestion from the factory.  Use it to create an invalid ingestion

        Ingestion validIngestion = getValidTestIngestion();

        List<Long> streamIds = validIngestion.getStreams()
                                                    .stream()
                                                    .map(EngineMember::getId)
                                                    .collect(Collectors.toList());

        //Build a new ingestion without setting the Project
        Ingestion invalidIngestion = Ingestion.newBuilder()
                                            .withName("Test Validation")
                                            .fromStreams(streamIds)
                                            .toTarget(validIngestion.getTarget().getId())
                                            .build();


        assertThrows(InvalidStateException.class, invalidIngestion::validOrThrow);
    }

    private Ingestion getValidTestIngestion()
    {
        TestDataFactory factory = new TestDataFactory(TestConfig.engineTestAccountName);
        return  factory.createTestIngestion("Validation Test", false);
    }
}
