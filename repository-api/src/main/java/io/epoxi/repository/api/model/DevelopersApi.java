package io.epoxi.repository.api.model;

import com.google.api.server.spi.ServiceException;
import io.epoxi.repository.api.model.ControllerBase.IngestionOperation;
import io.epoxi.repository.api.model.ControllerBase.IngestionSyncOperation;
import io.epoxi.repository.api.model.ControllerBase.TrashOperation;
import io.epoxi.repository.model.*;
import io.epoxi.repository.model.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.*;
import java.util.List;


/**
 * Sample Implicit Flow OAuth2 Project
 *
 * <p>This is an example of using OAuth2 Implicit Flow in a specification to describe security to your API.
 */
@Path("devApi")
@Api(value = "/")
public interface DevelopersApi  {

    /**
     * adds an ingestion item
     *
     * Adds an ingestion item to the system
     *
     */
    @POST
    @Path("/ingestion")
    @Consumes({ "application/json" })
    @ApiOperation(value = "adds an ingestion item", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "item created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing ingestion item already exists") })
    void addIngestion(Ingestion ingestion);

    /**
     * queue an ingestion for immediate processing
     *
     * queue an ingestion for immediate processing
     *
     */
    @PATCH
    @Path("/ingestion{id}")
    @Consumes({ "application/json" })
    @ApiOperation(value = "queue an ingestion for processing", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "item created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing ingestion item already exists") })
    void patchIngestion(@PathParam("id") Long id, @QueryParam("operation") IngestionOperation operation);

    /**
     * schedule an ingestion.
     *
     * Queue an ingestion. If not schedule is specified, the ingestion is queued for immediate processing
     */
    @POST
    @Path("/ingestionSync")
    @Consumes({ "application/json" })
    @ApiOperation(value = "start or schedule an ingestion.", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "item created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing ingestion item does not exists") })
    void addIngestionSync(IngestionSync ingestionSync) throws ServiceException;

    /**
     * acts on the specific ingestion specified by the id. 
     *
     * Performs the action passed via the query param, either an DELETE (permanent), or an UNDELETE
     *
     */
    @PATCH
    @Path("/ingestionSync{id}")
    @ApiOperation(value = "Acts on a scheduled sync", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "account matching id", response = Account.class) })
    void patchIngestionSync(@PathParam("id") Long id, @QueryParam("operation") IngestionSyncOperation operation);


    /**
     * adds an project
     *
     * adds an project to the system.
     *
     */
    @POST
    @Path("/project")
    @Consumes({ "application/json" })
    @ApiOperation(value = "adds an project", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "project created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing project already exists") })
    void addProject(Project project);

    /**
     * adds an source item
     *
     * adds an source item to the system
     */
    @POST
    @Path("/source")
    @Consumes({ "application/json" })
    @ApiOperation(value = "adds an source item", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "source item created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing source item already exists") })
    void addSource(Source source);

    /**
     * adds an target item
     *
     * Adds an target item to the system
     *
     */
    @POST
    @Path("/target")
    @Consumes({ "application/json" })
    @ApiOperation(value = "adds an target item", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "target item created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing target item already exists") })
    void addTarget(Target target);

    /**
     * adds an stream item
     *
     * Adds an stream item to the system
     *
     */
    @POST
    @Path("/stream")
    @Consumes({ "application/json" })
    @ApiOperation(value = "adds an stream item", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "stream item created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing stream item already exists") })
    void addStream(Stream target);

    /**
     * delete an ingestion item
     *
     * deletes a ingestion item
     *
     */
    @DELETE
    @Path("/ingestion{id}")
    @ApiOperation(value = "delete an ingestion item", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "ingestion item deleted"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing ingestion item already exists") })
    void deleteIngestion(@PathParam("id") Long id);

    /**
     * Stop (or dequeue) and ingestion sync.
     *
     * stop an in-process ingestion sync
     *
     */
    @DELETE
    @Path("/ingestionSync{id}")
    @ApiOperation(value = "Stop (or dequeue) and ingestion sync.", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "ingestion sync stopped"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing ingestion sync does not exists") })
    void deleteIngestionSync(@PathParam("id") Long id);

    /**
     * deletes a project
     *
     * deletes a project.
     *
     */
    @DELETE
    @Path("/project{id}")
    @ApiOperation(value = "deletes a project", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "project deleted"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing project item already exists") })
    void deleteProject(@PathParam("id") Long id);

    /**
     * adds an source item
     *
     * deletes a source item
     *
     */
    @DELETE
    @Path("/source{id}")
    @ApiOperation(value = "adds an source item", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "source item deleted"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing source item already exists") })
    void deleteSource(@PathParam("id") Long id);

    /**
     * deletes an target item
     *
     * deletes a target item from the system
     *
     */
    @DELETE
    @Path("/target{id}")
    @ApiOperation(value = "deletes an target item", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "target item deleted"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing target item already exists") })
    void deleteTarget(@PathParam("id") Long id);

     /**
     * deletes an stream item
     *
     * deletes a stream item from the system
     *
     */
    @DELETE
    @Path("/stream{id}")
    @ApiOperation(value = "deletes an stream item", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "stream item deleted"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing stream item already exists") })
    void deleteStream(@PathParam("id") Long id);

    /**
     * returns an account
     *
     * Retrieves the account info in the system 
     *
     */
    @GET
    @Path("/account")
    @ApiOperation(value = "returns an account", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "account", response = Account.class) })
    Account getAccount();

    /**
     * returns an specific ingestion based on the id
     *
     * By passing in the appropriate options, you can search for available ingestion in the system 
     *
     */
    @GET
    @Path("/ingestion{id}")
    @ApiOperation(value = "returns an specific ingestion based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "ingestion matching id", response = Ingestion.class) })
    Ingestion getIngestion(@PathParam("id") Long id);


    /**
     * returns an specific ingestion sync based on the id
     *
     * By passing in the appropriate options, you can search for available Ingestion sync in the system 
     *
     */
    @GET
    @Path("/ingestionSync{id}")
    @ApiOperation(value = "returns an specific ingestion sync based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "ingestion sync matching id", response = Ingestion.class) })
    IngestionSyncProgress getIngestionSyncProgress(@PathParam("id") Long id);


    /**
     * returns an specific ingestion sync based on the id
     *
     * By passing in the appropriate options, you can search for available Ingestion sync in the system 
     *
     */
    @GET
    @Path("/ingestionSync{id}/progress")
    @ApiOperation(value = "returns an specific ingestion sync based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "ingestion sync matching id", response = Ingestion.class) })
    IngestionSync getIngestionSync(@PathParam("id") Long id);

    /**
     * returns an specific project based on the id
     *
     * returns an specific project based on the id
     *
     */
    @GET
    @Path("/project{id}")
    @ApiOperation(value = "returns an specific project based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "project matching id", response = Project.class) })
    Project getProject(@PathParam("id") Long id);

    /**
     * returns a specific source based on the id
     *
     * returns a specific source based on the id
     *
     */
    @GET
    @Path("/source{id}")
    @ApiOperation(value = "returns a specific source based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "source matching id", response = Source.class) })
    Source getSource(@PathParam("id") Long id);

    /**
     * returns an specific target based on the id
     *
     * returns an specific target based on the id
     *
     */
    @GET
    @Path("/target{id}")
    @ApiOperation(value = "returns an specific target based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "target matching id", response = Target.class) })
    Target getTarget(@PathParam("id") Long id);

    /**
     * returns an specific stream based on the id
     *
     * returns an specific stream based on the id
     *
     */
    @GET
    @Path("/stream{id}")
    @ApiOperation(value = "returns an specific stream based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "stream matching id", response = Stream.class) })
    Stream getStream(@PathParam("id") Long id);
  


    /**
     * returns an specific project based on the name
     *
     * returns an specific project based on the name
     *
     */
    @GET
    @Path("/project{name}")
    @ApiOperation(value = "returns an specific project based on the name", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "project matching name", response = Project.class) })
    Project getProject(@PathParam("name") String name);

    /**
     * returns a specific source based on the name
     *
     * returns a specific source based on the name
     *
     */
    @GET
    @Path("/source{name}")
    @ApiOperation(value = "returns a specific source based on the name", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "source matching name", response = Source.class) })
    Source getSource(@PathParam("name") String name);

    /**
     * returns a specific source based on the name
     *
     * returns a specific source based on the name
     *
     */
    @GET
    @Path("/project{id}/source{name}")
    @ApiOperation(value = "returns a specific source based on the name (and optionally projectId)", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "source matching name within project", response = Source.class) })
    Source getSource(@PathParam("name") String name, @PathParam("id") Long id);

     /**
     * returns an specific target based on the name
     *
     * returns an specific target based on the name
     *
     */
    @GET
    @Path("/target{name}")
    @ApiOperation(value = "returns an specific target based on the name", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "target matching name", response = Target.class) })
    Target getTarget(@PathParam("name") String name);

    /**
     * returns an specific target based on the name
     *
     * returns an specific target based on the name
     *
     */
    @GET
    @Path("/project{id}/target{name}")
    @ApiOperation(value = "returns an specific target based on the name (and optionally projectId)", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "target matching name within project", response = Target.class) })
    Target getTarget(@PathParam("name") String name, @PathParam("id") Long projectId);

     /**
     * returns an specific stream based on the name
     *
     * returns an specific stream based on the name
     *
     */
    @GET
    @Path("/stream{name}")
    @ApiOperation(value = "returns an specific stream based on the name", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "stream matching name", response = Stream.class) })
    Stream getStream(@PathParam("name") String name);

    /**
     * returns an specific stream based on the name
     *
     * returns an specific stream based on the name
     *
     */
    @GET
    @Path("/project{id}/stream{name}")
    @ApiOperation(value = "returns an specific stream based on the name (and optionally projectId)", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "stream matching name within project", response = Stream.class) })
    Stream getStream(@PathParam("name") String name, @PathParam("id") Long id);
  

    /**
     * returns an specific ingestion based on the name
     *
     * By passing in the appropriate options, you can search for available Ingestion in the system 
     *
     */
    @GET
    @Path("/project{id}/ingestion{name}")
    @ApiOperation(value = "returns an specific ingestion based on the name projectId", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "ingestion matching name (and optionally projectId)", response = Ingestion.class) })
    Ingestion getIngestion(@PathParam("name") String name, @PathParam("projectId") Long projectId);

    /**
     * returns an specific ingestionSync based on the name
     *
     * By passing in the appropriate options, you can search for available ingestionSync in the system 
     *
     */
    @GET
    @Path("/project{id}/ingestionSync{name}")
    @ApiOperation(value = "returns an specific ingestionSync based on the name projectId", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "ingestionSync matching name (and optionally projectId)", response = Ingestion.class) })
    IngestionSync getIngestionSync(@PathParam("name") String name, @PathParam("projectId") Long projectId);


    /**
     * returns an ingestion
     *
     * By passing in the appropriate options, you can search for available Ingestion in the system 
     *
     */
    @GET
    @Path("/ingestion")
    @ApiOperation(value = "returns an ingestion", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "search results matching criteria", response = Ingestion.class, responseContainer = "List"),
        @ApiResponse(code = 400, message = "bad input parameter") })
    List<Ingestion> searchIngestion(@QueryParam("searchPattern")String searchPattern, @QueryParam("name")String name, @QueryParam("projectId") Long projectId, @QueryParam("skip") @DefaultValue("0")Integer skip, @QueryParam("limit") @DefaultValue("0")Integer limit);

    /**
     * returns a project
     *
     * By passing in the appropriate options, you can search for available Source in the system - in: query   name: searchPattern   description: pass an optional search string for looking up project.  Search returns all projects where the name matches the search string.  Regular expressions are valid in the searchPattern.   required: false   schema:     type: string - in: query   name: skip   description: number of records to skip for pagination.   schema:     type: integer     format: int32     minimum: 0 - in: query   name: limit   description: maximum number of records to return.   schema:     type: integer     format: int32     minimum: 0     maximum: 50 
     *
     */
    @GET
    @Path("/project")
    @ApiOperation(value = "returns a project", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "search results matching criteria", response = Project.class, responseContainer = "List"),
        @ApiResponse(code = 400, message = "bad input parameter") })
    List<Project> searchProject(@QueryParam("searchPattern")String searchPattern, @QueryParam("name")String name, @QueryParam("skip") @DefaultValue("0")Integer skip, @QueryParam("limit") @DefaultValue("0")Integer limit);

    /**
     * returns an source
     *
     * By passing in the appropriate options, you can search for available Source in the system 
     *
     */
    @GET
    @Path("/source")
    @ApiOperation(value = "returns an source", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "search results matching criteria", response = Source.class, responseContainer = "List"),
        @ApiResponse(code = 400, message = "bad input parameter") }
        )
    List<Source> searchSource(@QueryParam("searchPattern")String searchPattern, @QueryParam("name")String name, @QueryParam("projectId") Long projectId, @QueryParam("skip") @DefaultValue("0")Integer skip, @QueryParam("limit") @DefaultValue("0")Integer limit);

    /**
     * returns an target
     *
     * By passing in the appropriate options, you can search for available target in the system 
     *
     */
    @GET
    @Path("/target")
    @ApiOperation(value = "returns an target", tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "search results matching criteria", response = Target.class, responseContainer = "List"),
        @ApiResponse(code = 400, message = "bad input parameter") })
    List<Target> searchTarget(@QueryParam("searchPattern")String searchPattern, @QueryParam("name")String name, @QueryParam("projectId") Long projectId, @QueryParam("skip") @DefaultValue("0")Integer skip, @QueryParam("limit") @DefaultValue("0")Integer limit);

    /**
     * returns an stream
     *
     * By passing in the appropriate options, you can search for available stream in the system 
     *
     */
    @GET
    @Path("/stream")
    @ApiOperation(value = "returns an stream", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "search results matching criteria", response = Target.class, responseContainer = "List"),
        @ApiResponse(code = 400, message = "bad input parameter") })
    List<Stream> searchStream(@QueryParam("searchPattern")String searchPattern, @QueryParam("name")String name, @QueryParam("projectId") Long projectId, @QueryParam("skip") @DefaultValue("0")Integer skip, @QueryParam("limit") @DefaultValue("0")Integer limit);


    //#region Trash

     /**
     * returns an specific ingestion based on the id
     *
     * returns an specific ingestion based on the id
     *
     */
    @GET
    @Path("trash/ingestion{id}")
    @ApiOperation(value = "returns an specific deleted ingestion based on the id", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "ingestion matching id", response = Ingestion.class) })
    Ingestion getDeletedIngestion(@PathParam("id") Long id);

     /**
     * acts on the specific ingestion specified by the id
     *
     * performs the action passed via the query param, either an DELETE (permanent), or an UNDELETE
     *
     */
    @PATCH
    @Path("trash/ingestion{id}")
    @ApiOperation(value = "acts on the specific ingestion specified by the id. Performs the action passed via the query param, either an DELETE (permanent), or an UNDELETE", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "ingestion matching id", response = Ingestion.class) })
    void patchIngestion(@PathParam("id") Long id, @QueryParam("operation") TrashOperation operation);

    //#endregion

}

