package io.epoxi.app.repository.api.model;

import io.epoxi.app.repository.api.model.ControllerBase.TrashOperation;
import io.epoxi.app.repository.model.Account;
import io.epoxi.app.repository.model.Metadata;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.*;
import java.util.List;
import java.util.Map;

/**
 * Sample Implicit Flow OAuth2 Project
 *
 * <p>This is an example of using OAuth2 Implicit Flow in a specification to describe security to your API.
 * 
 */
@Path("adminApi")
@Api(value = "/")
public interface AdminsApi  {

    /**
     * adds an account
     *
     * adds an account to the system.
     *
     */

    @POST
    @Path("/account")
    @Consumes({ "application/json" })
    @ApiOperation(value = "adds an account", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "account created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing account already exists") })
    void addAccount(Account account);

    /**
     * sets metadata for a project
     *
     * adds metadata to a project. If metadata key already exists, it will be overwritten
     *
     */
    @POST
    @Path("/project/{id}/metadata")
    @Consumes({ "application/json" })
    @ApiOperation(value = "sets metadata for a project", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "project metadata created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid") })
    void addProjectMetadata(@PathParam("id") Long id, Metadata metadata);

    /**
     * deletes account
     *
     * deletes account
     *
     */
    @DELETE
    @Path("/account/{id}")
    @ApiOperation(value = "deletes account", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "account deleted"),
        @ApiResponse(code = 400, message = "invalid input, object invalid") })
    void deleteAccount(@PathParam("id") Long id);


    /**
     * deletes metadata from a project
     *
     * deletes the project metadata object
     *
     */
    @DELETE
    @Path("/project/{id}/metadata{key}")
    @ApiOperation(value = "deletes metadata from a project", tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "project metadata deleted"),
        @ApiResponse(code = 400, message = "invalid input, object invalid") })
    void deleteProjectMetadata(@PathParam("id") Long id, @PathParam("key") String key);


     /**
     * returns an specific account based on the id
     *
     * returns an specific account based on the id
     *
     */
    @GET
    @Path("/account{id}")
    @ApiOperation(value = "returns an specific account based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "account matching id", response = Account.class) })
    Account getAccount(@PathParam("id") Long id);

     /**
     * returns an specific account based on the name
     *
     * returns an specific account based on the name
     *
     */
    @GET
    @Path("/account{name}")
    @ApiOperation(value = "returns an specific account based on the name", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "account matching name", response = Account.class) })
    Account getAccount(@PathParam("name") String name);


    /**
     * returns metadata for a project
     *
     * returns metadata for a project
     *
     */
    @GET
    @Path("/project/{id}/metadata")
    @ApiOperation(value = "returns an project", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "metadata for project", response = Metadata.class, responseContainer = "Map") })
    Map<String, Metadata> getProjectMetadata(@PathParam("id") Long id);

    /**
     * returns a list of accounts
     *
     * By passing in the appropriate options, you can search for available account in the system 
     *
     */
    @GET
    @Path("/account")
    @ApiOperation(value = "returns an array of accounts", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "search results matching criteria", response = Account.class, responseContainer = "List") })
    List<Account> searchAccount(@QueryParam("searchString")String searchString, @QueryParam("name")String name, @QueryParam("skip") @DefaultValue("0")Integer skip, @QueryParam("limit") @DefaultValue("0")Integer limit);


    //#region Trash

     /**
     * returns an specific account based on the id
     *
     * returns an specific account based on the id
     *
     */
    @GET
    @Path("trash/account{id}")
    @ApiOperation(value = "returns an specific deleted account based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "account matching id", response = Account.class) })
    Account getDeletedAccount(@PathParam("id") Long id);

     /**
     * acts on the specific ingestion specified by the id. 
     *
     * Performs the action passed via the query param, either an DELETE (permanent), or an UNDELETE
     *
     */
    @PATCH
    @Path("trash/account{id}")
    @ApiOperation(value = "acts on the specific ingestion specified by the id. Performs the action passed via the query param, either an DELETE (permanent), or an UNDELETE", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "account matching id", response = Account.class) })
    void patchAccount(@PathParam("id") Long id, @QueryParam("operation") TrashOperation operation);

    //#endregion
   
}

