package io.epoxi.repository.model.modelbase;

import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.TestDataFactory;
import io.epoxi.repository.exception.ForeignKeyException;
import io.epoxi.repository.modelbase.MemberRepository;
import io.epoxi.repository.TestConfig;
import io.epoxi.repository.exception.DuplicateMemberException;
import io.epoxi.repository.model.Account;
import io.epoxi.repository.model.Ingestion;
import io.epoxi.repository.model.Project;
import io.epoxi.repository.model.Target;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class EngineRepositoryTest {

    @BeforeAll
    public static void setUp() {

    }

    @Test
    public void GetObjectsByName()
    {
        TestDataFactory factory = new TestDataFactory(TestConfig.apiTestAccountName);
        AccountRepository accountRepository = factory.getAccountRepository();

        //Assert Account
        Account account = accountRepository.getAccount();
        assertNotNull(account, "Get Account successfully executed");
        assertEquals(TestConfig.apiTestAccountName, account.getName(), "Get Account successfully returned the Account with the correct name");

        //Assert Project
        String projectName = factory.getFirstProject().getName();
        Project project = accountRepository.getProjectRepository().get(projectName);
        assertNotNull(project, "Get Account successfully executed");
        assertEquals(projectName, project.getName(), "Get Account successfully returned the Account with the correct name");
    }

    @Test
    public void GetSetTest()
    {
        //Get an ingestion
        TestDataFactory factory = new TestDataFactory(TestConfig.apiTestAccountName);
        Ingestion ingestion = factory.getTestIngestion("Test Ingestion (Get/Set Test)", true);

        //Update a value on the ingestion
        Float testValue = ingestion.getDeleteThreshold().equals(.5F) ? .25F : .5F;
        ingestion.setDeleteThreshold(testValue);

        //Save and reload the ingestion
        factory.getAccountRepository().getIngestionRepository().add(ingestion);
        ingestion = factory.getAccountRepository().getIngestionRepository().get(ingestion.getId());

        //Assert
        assertEquals(testValue, ingestion.getDeleteThreshold(), "Creation of project with same name as existing project successfully threw exception");
    }

    @Test
    public void EngineRepositoryDuplicateNameExistsErrorTest()
    {
        //Assert Account
        Account account = TestDataFactory.getTestAccount("Test Account (Duplicate Name Test)", true);
        String accountName = account.getName();
        assertThrows(DuplicateMemberException.class, () -> TestDataFactory.createTestAccount(accountName, true), "Creation of account with same name as existing account successfully threw exception");

         //Assert Project
        TestDataFactory factory = new TestDataFactory(TestConfig.apiTestAccountName);
        Project project = factory.getTestProject("Test Project (Duplicate Name Test)", true);
        String projectName = project.getName();
        assertThrows(DuplicateMemberException.class, () -> factory.createTestProject(projectName, true), "Creation of project with same name as existing project successfully threw exception");

    }

    @Test
    public void OnDeleteErrorWhenForeignKeyExistsTest()
    {
        TestDataFactory factory = new TestDataFactory(TestConfig.apiTestAccountName);
        Ingestion ingestion = factory.getTestIngestion("FK Ingestion Test", true);

        Target target = ingestion.getTarget();
        MemberRepository<Target> targetRepository = factory.getAccountRepository().getProjectRepository().getTargetRepository(target.getProjectId());

        //Assert
        Assertions.assertThrows(ForeignKeyException.class, () -> targetRepository.delete(target), "A foreign key exception was successfully thrown to prevent the deletion of the Target");
    }

    @Test
    public void OnDeleteCascadeDeleteTest()
    {
        Account account = TestDataFactory.getTestAccount("Test Account - Cascade Delete Test", true);     //This will create the account by default

        TestDataFactory factory = new TestDataFactory(account.getId());  //Get a factory based on the new test account
        Project project = factory.getTestProject("Test Project - Cascade Delete Test", true);
        Ingestion ingestion = factory.getTestIngestion("Test Project - Cascade Delete Test", true);

        //Assert that we can successfully delete an account
        AccountRepository accountRepository = factory.getAccountRepository();
        assertDoesNotThrow(() -> accountRepository.delete(account), "The account was deleted successfully");

        //Assert the project was deleted when the account was deleted.
        Project deletedProject = accountRepository.getProjectRepository().get(project.getId());
        assertNull(deletedProject, "The project associated with the account was deleted by a cascade delete of the account");

        //Assert the project was deleted when the account was deleted.
        Ingestion deletedIngestion = accountRepository.getProjectRepository().getIngestionRepository(project.getId()).get(ingestion.getId());
        assertNull(deletedIngestion, "The ingestion associated with the account's default project was deleted by a cascade delete of the account");

    }
}
