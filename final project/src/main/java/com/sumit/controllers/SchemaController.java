package com.sumit.controllers;

import com.sumit.PHXException.PHXException;
import com.sumit.service.SchemaService;
import org.json.simple.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Sumit on 11/18/2016.
 */
@SuppressWarnings({"unused", "UnusedAssignment", "Convert2Diamond", "DefaultFileTemplate"})
@RestController
public class SchemaController {

    private final String className = getClass().getName();
    private SchemaService schemaService = new SchemaService();

    public SchemaController() throws PHXException {
    }

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "/api/schema", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<JSONObject> getSchema(){
        JSONObject jsonObject = null;
        try {
            jsonObject = schemaService.getSchema();
        } catch (PHXException e) {
            String message = "Exception while getting schema.";
            PHXException px = new PHXException(message + "||"
                    + className +".getSchema()" + "||" + e.getMessage());
            px.printStackTrace();
            return new ResponseEntity<JSONObject>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        if (jsonObject == null) {
            return new ResponseEntity<JSONObject>(HttpStatus.NOT_FOUND);
        }
        else{
            return new ResponseEntity<JSONObject>(jsonObject, HttpStatus.OK);
        }
    }

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "/api/schema", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.ALL_VALUE)
    public ResponseEntity<String> createSchema(@RequestBody String schemaString){
        String result = null;
        try {
            result = schemaService.saveSchema(schemaString);
        } catch (PHXException e) {
            String message = "Exception while creating plan.";
            PHXException px = new PHXException(message + "||"
                    + className +".createSchema()" + "||" + e.getMessage());
            px.printStackTrace();
            return new ResponseEntity<String>(px.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        if(result == null){
            return new ResponseEntity<String>("Schema Saved!!", HttpStatus.OK);
        }
        else {
            return new ResponseEntity<String>(result, HttpStatus.BAD_REQUEST);
        }

    }

}
