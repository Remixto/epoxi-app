package io.epoxi.app.etlengine.api;

import io.epoxi.app.repository.model.MessageQueue;
import io.epoxi.app.repository.model.StepEndpointTemplate;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.*;
import java.util.List;

@Path("etlEngine")
@Api(value = "/")
public interface ETLEngineApi {

   /**
     * adds an messageQueue
     *
     * adds an messageQueue to the system.
     *
     */
    @POST
    @Path("/messageQueue")
    @Consumes({ "application/json" })
    @ApiOperation(value = "adds an messageQueue", tags={ "admins",  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "messageQueue created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing messageQueue already exists") })
    void addMessageQueue(MessageQueue messageQueue);

    /**
     * adds an stepEndpointTemplate
     *
     * adds an stepEndpointTemplate to the system.
     *
     */
    @POST
    @Path("/stepEndpointTemplate")
    @Consumes({ "application/json" })
    @ApiOperation(value = "adds an stepEndpointTemplate", tags={ "admins",  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "stepEndpointTemplate created"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing stepEndpointTemplate already exists") })
    void addStepEndpointTemplate(StepEndpointTemplate stepEndpointTemplate);
   

    /**
     * receive a specific message from a Push Pubsub Subscription
     *
     * receive a specific message from a Push Pubsub Subscription
     */
    @POST
    @Path("/messageQueue{name}/message")
    @Consumes({ "application/json" })
    @ApiOperation(value = "receive a specific message from a Push Pubsub Subscription", tags={ "admins",  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "message received"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "message not received") })
    void receiveMessage(@PathParam("name") String name, String message, @QueryParam("runAsync") @DefaultValue("true")Boolean runAsync);


    /**
     * receive messages from a Pull Pubsub Subscription
     *
     * receive messages from a Pull Pubsub Subscription
     *
     */    
    @POST
    @Path("/messageQueue{name}/messages")
    @Consumes({ "application/json" })
    @ApiOperation(value = "receive messages from a Pull Pubsub Subscription", tags={ "admins",  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "message received"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "message not received") })
    void receiveMessages(@PathParam("name") String name, @DefaultValue("true")Boolean runAsync);


    /**
     * deletes a messageQueue
     *
     * deletes a messageQueue.
     */
    @DELETE
    @Path("/messageQueue{id}")
    @ApiOperation(value = "deletes a messageQueue", tags={ "admins",  })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "messageQueue deleted"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing messageQueue item already exists") })
    void deleteMessageQueue(@PathParam("id") Long id);

    /**
     * returns an specific messageQueue based on the id
     *
     * returns an specific messageQueue based on the id
     *
     */
    @GET
    @Path("/messageQueue{id}")
    @ApiOperation(value = "returns an specific messageQueue based on the id", tags={ "admins",  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "source matching id", response = MessageQueue.class) })
    MessageQueue getMessageQueue(@PathParam("id") Long id);

     /**
     * returns an specific messageQueue based on the name
     *
     * returns an specific messageQueue based on the name
     *
     */
    @GET
    @Path("/messageQueue{name}")
    @ApiOperation(value = "returns an specific messageQueue based on the name", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "messageQueue matching name", response = MessageQueue.class) })
    MessageQueue getMessageQueue(@PathParam("name") String name);

    /**
     * returns an specific stepEndpointTemplate based on the id
     *
     * returns an specific stepEndpointTemplate based on the id
     *
     */
    @GET
    @Path("/stepEndpointTemplate{id}")
    @ApiOperation(value = "returns an specific stepEndpointTemplate based on the id", tags={ "admins",  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "source matching id", response = StepEndpointTemplate.class) })
    StepEndpointTemplate getStepEndpointTemplate(@PathParam("id") Long id);

  
     /**
     * returns an specific stepEndpointTemplate based on the name
     *
     * returns an specific stepEndpointTemplate based on the name
     *
     */
    @GET
    @Path("/stepEndpointTemplate{name}")
    @ApiOperation(value = "returns an specific stepEndpointTemplate based on the name", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "stepEndpointTemplate matching name", response = MessageQueue.class) })
    StepEndpointTemplate getStepEndpointTemplate(@PathParam("name") String name);

    /**
     * deletes a stepEndpointTemplate
     *
     * deletes a stepEndpointTemplate.
     *
     */
    @DELETE
    @Path("/stepEndpointTemplate{id}")
    @ApiOperation(value = "deletes a stepEndpointTemplate", tags={ "admins" })
    @ApiResponses(value = { 
        @ApiResponse(code = 201, message = "stepEndpointTemplate deleted"),
        @ApiResponse(code = 400, message = "invalid input, object invalid"),
        @ApiResponse(code = 409, message = "an existing stepEndpointTemplate item already exists") })
    void deleteStepEndpointTemplate(@PathParam("id") Long id);

    /**
     * returns an stepEndpointTemplate
     *
     * By passing in the appropriate options, you can search for available stepEndpointTemplate in the system 
     *
     */
    @GET
    @Path("/stepEndpointTemplate")
    @ApiOperation(value = "returns an stepEndpointTemplate", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "search results matching criteria", response = StepEndpointTemplate.class, responseContainer = "List"),
        @ApiResponse(code = 400, message = "bad input parameter") })
    List<StepEndpointTemplate> searchStepEndpointTemplate(@QueryParam("searchPattern")String searchPattern, @QueryParam("name")String name, @QueryParam("skip") @DefaultValue("0")Integer skip, @QueryParam("limit") @DefaultValue("0")Integer limit);

    /**
     * returns an messageQueue
     *
     * By passing in the appropriate options, you can search for available messageQueue in the system 
     *
     */
    @GET
    @Path("/messageQueue")
    @ApiOperation(value = "returns an messageQueue", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "search results matching criteria", response = StepEndpointTemplate.class, responseContainer = "List"),
        @ApiResponse(code = 400, message = "bad input parameter") })
    List<MessageQueue> searchMessageQueue(@QueryParam("searchPattern")String searchPattern, @QueryParam("name") String name, @QueryParam("skip")  @DefaultValue("0")Integer skip, @QueryParam("limit")  @DefaultValue("0")Integer limit);

}

