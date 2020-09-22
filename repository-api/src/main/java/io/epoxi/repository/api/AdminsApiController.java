package io.epoxi.repository.api;

import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.api.model.AdminsApi;
import io.epoxi.repository.model.Account;
import io.epoxi.repository.model.Metadata;
import io.epoxi.repository.model.Project;
import io.epoxi.repository.modelbase.MemberRepository;
import io.epoxi.repository.api.model.ControllerBase;

import java.util.List;
import java.util.Map;

public class AdminsApiController extends ControllerBase implements AdminsApi {

    private final AccountRepository accountRepository;

    public AdminsApiController()
	{
        super();  //init the ApiBase
        accountRepository = new AccountRepository();
    }

    public void addAccount(Account account) {
        accountRepository.add(account);        
    }

    public Account getAccount(Long id) {
        return accountRepository.get(id);       
    }

	public Account getAccount(String name) {

		return accountRepository.get(name);
	}

    @SuppressWarnings("unchecked")
	public List<Account> searchAccount(String searchPattern, String name, Integer skip, Integer limit) {      

        List<Account> list;

		if(name!=null)
		{
			list = accountRepository.searchName(name);
		}
		else
		{
			list = accountRepository.searchPattern(searchPattern);
        }		
        
        return (List<Account>)limitList(list, skip, limit);
    }

    public void deleteAccount(Long id) {
        deleteAccount(id, false);
    }

    public void deleteAccount(Long id, Boolean permanent) {
        accountRepository.delete(id, permanent);
    }

    public void addProjectMetadata(Long id, Metadata metadata) {
        
        MemberRepository<Project> projectRepository = accountRepository.getEngineProjectRepository();
        Project project = projectRepository.get(id);
        project.addMetadataItem(metadata);

        projectRepository.add(project);      
    }

    public void deleteProjectMetadata(Long id, String key) {
       
        MemberRepository<Project> projectRepository = accountRepository.getEngineProjectRepository();
        Project project = projectRepository.get(id);
        project.removeMetadataItem(key);

        projectRepository.add(project);
    }

    public Map<String, Metadata> getProjectMetadata(Long id) {

        MemberRepository<Project> projectRepository = accountRepository.getEngineProjectRepository();
        Project project = projectRepository.get(id);
        return  project.getMetadata();
    }

    //#region Trash methods
    public Account getDeletedAccount(Long id) {
        return accountRepository.get(id, true);
    }

    public void patchAccount(Long id, TrashOperation operation) {

        switch(operation)
        {
            case UNDELETE :
                undeleteAccount(id);
                break;
            case DELETE :
                deleteAccount(id, true);
                break;
        }
    }

    public void undeleteAccount(Long id) {
        System.out.print(id);
        throw new UnsupportedOperationException("Undeletes are not yet implemented by the API");
    }

    //#endregion
  
}