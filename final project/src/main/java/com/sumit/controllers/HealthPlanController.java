package com.sumit.controllers;

import com.sumit.PHXException.PHXException;
import com.sumit.service.HealthPlanService;
import org.json.simple.JSONObject;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.core.EntityTag;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Created by Sumit on 11/19/2016.
 */
@SuppressWarnings("ALL")
@RestController
public class HealthPlanController {

    private final String className = getClass().getName();
    HealthPlanService planService = new HealthPlanService();

    public HealthPlanController() throws PHXException {
    }

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "/api/plans", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.ALL_VALUE)
    public ResponseEntity<String> savePlan(@RequestBody String plan){
        String savedPlan;
        try {
            savedPlan = planService.savePlan(plan);
        } catch (PHXException e) {
            String message = "Exception while saving plan.";
            PHXException px = new PHXException(message + "||"
                    + className +".savePlan()" + "||" + e.getMessage());
            px.printStackTrace();
            return new ResponseEntity<String>(px.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        if(savedPlan.equalsIgnoreCase("Please Check the input JSON!!! JSON is not compatible with JSON Schema!!")){
            return new ResponseEntity<String>(savedPlan, HttpStatus.BAD_REQUEST);
        }
        else{
            return new ResponseEntity<String>("Plan Saved With id - "+savedPlan, HttpStatus.OK);
        }
    }

    @PreAuthorize("hasRole('USER') OR hasRole('ADMIN')")
    @RequestMapping(value = "/api/plan/{planId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<JSONObject> getPlan(@RequestHeader HttpHeaders headers, @PathVariable("planId") String planId){
        JSONObject jsonObject = null;
        try {
            jsonObject = planService.getPlan(planId);
        } catch (PHXException e) {
            String message = "Exception while getting plan by Id.";
            PHXException px = new PHXException(message + "||"
                    + className +".getPlan()" + "||" + e.getMessage());
            px.printStackTrace();
            return new ResponseEntity<JSONObject>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        if (jsonObject == null) {
            return new ResponseEntity<JSONObject>(HttpStatus.NOT_FOUND);
        }
        else{
            try {
                ResponseEntity.BodyBuilder bodyBuilder = (ResponseEntity.BodyBuilder) ResponseEntity.ok();
                MessageDigest messageDigest = MessageDigest.getInstance("MD5");
                messageDigest.reset();
                String planJSONString = jsonObject.toJSONString();
                messageDigest.update(planJSONString.getBytes());
                byte[] digest = messageDigest.digest();
                BigInteger bigInt = new BigInteger(1,digest);
                String hashtext = bigInt.toString(16);
                EntityTag etag = new EntityTag(hashtext);
                bodyBuilder.eTag(etag.toString());
                String header = headers.getFirst("If-None-Match");
                if (header != null) {
                    if (!header.equals(hashtext)) {
                        bodyBuilder.body(HttpStatus.OK);
                        return bodyBuilder.body(jsonObject);
                    } else {
                        return new ResponseEntity<JSONObject>(HttpStatus.NOT_MODIFIED);
                    }
                }
            } catch (Exception e) {
                String message = "Exception while getting plan by Id(eTag).";
                PHXException px = new PHXException(message + "||"
                        + className +".getPlan()" + "||" + e.getMessage());
                px.printStackTrace();
                return new ResponseEntity<JSONObject>(HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return new ResponseEntity<JSONObject>(jsonObject, HttpStatus.OK);
        }

    }


    @PreAuthorize("hasRole('USER') OR hasRole('ADMIN')")
    @RequestMapping(value = "/api/plans", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<JSONObject>> getPlanns(){
        List<JSONObject> jsonObjects;
        try {
            jsonObjects = planService.getPlans();
            return new ResponseEntity<List<JSONObject>>(jsonObjects, HttpStatus.OK);
        } catch (PHXException e) {
            String message = "Exception while getting all the plans.";
            PHXException px = new PHXException(message + "||"
                    + className +".getPlanns()" + "||" + e.getMessage());
            px.printStackTrace();
            return new ResponseEntity<List<JSONObject>>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "/api/plan/{id}", method = RequestMethod.PATCH, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.ALL_VALUE)
    public ResponseEntity<String> patchPlan(@PathVariable("id") String id, @RequestBody String planJson){
        String updatedPlan = null;
        try {
            updatedPlan = planService.patchPlan(id, planJson);
        } catch (PHXException e) {
            String message = "Exception while updating an plan.";
            PHXException px = new PHXException(message + "||"
                    + className +".patchPlan()" + "||" + e.getMessage());
            px.printStackTrace();
            return new ResponseEntity<String>(px.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
        if (updatedPlan == null) {
            return new ResponseEntity<String>("Plan doesn't exist!",HttpStatus.NOT_FOUND);
        }
        else if(updatedPlan.equalsIgnoreCase("1")){
            return new ResponseEntity<String>("Plan : "+id+" patched successfully!!", HttpStatus.OK);
        }
        else{
            return new ResponseEntity<String>(updatedPlan, HttpStatus.NOT_ACCEPTABLE);
        }
    }

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "/api/plan/{planId}", method = RequestMethod.DELETE, produces = MediaType.ALL_VALUE)
    public ResponseEntity<String> deletePlan(@PathVariable("planId") String planId) {
        boolean deleted = false;
        try {
            deleted = planService.deletePlan(planId);
        } catch (Exception e) {
            String message = "Exception while deleting a plan.";
            PHXException px = new PHXException(message + "||"
                    + className +".deletePlan()" + "||" + e.getMessage());
            px.printStackTrace();
            return new ResponseEntity<String>(px.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
        if (!deleted) {
            return new ResponseEntity<String>("Plan with id :"+planId+" doesn't exist.",HttpStatus.NOT_FOUND);
        }
        else{
            return new ResponseEntity<String>("Plan with id :"+planId+" deleted.", HttpStatus.OK);
        }
    }
}
