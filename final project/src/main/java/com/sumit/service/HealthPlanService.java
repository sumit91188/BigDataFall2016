package com.sumit.service;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.sumit.JSONValidation.ValidationUtils;
import com.sumit.PHXException.PHXException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Sumit on 11/19/2016.
 */
@SuppressWarnings("ALL")
public class HealthPlanService {

    private final String className = getClass().getName();
    private static final String redisHost = "localhost";
    private static final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    private JedisPool jedisPool;
    private JSONObject jsonObject, jsonSchemaObject;
    private JSONObject jsonPatchObject;
    private Jedis jedis;
    private String jsonSchemaString;
    private String jsonPatchSchemaString;
    private Gson gson;
    private JSONParser jsonParser;

    public HealthPlanService() throws PHXException {
        jsonParser = new JSONParser();
        gson = new Gson();
        try {
            jedis = getJedisConnectionPool().getResource();
        } catch (PHXException e) {
            String message = "Exception while creating HealthPlanService Controller.";
            PHXException px = new PHXException(message + "||" + className + ".JSONParser()" + "||" + e.getMessage());
            throw px;
        }
    }

    private JedisPool getJedisConnectionPool() throws PHXException {
        try {
            if (jedisPool == null) {
                jedisPool = new JedisPool(jedisPoolConfig, redisHost);
            }
        } catch (Exception e) {
            String message = "Exception while getting Jedis-Connnection pool.";
            PHXException px = new PHXException(message + "||" + className + ".getJedisConnectionPool()" + "||" + e.getMessage());
            throw px;
        }
        return jedisPool;
    }

    private String readJSONSchema() throws PHXException {
        try {
            jsonSchemaString = jedis.get("JSONSchema_HealthPlan");
        } catch (Exception e) {
            String message = "Exception while reading JSON Schema.";
            PHXException px = new PHXException(message + "||" + className + ".readJSONSchema()" + "||" + e.getMessage());
            throw px;
        }
        return jsonSchemaString;
    }

    private boolean isJSONValid(String jsonSchemaString, String inputJSON) throws PHXException {
        try {
            if (ValidationUtils.isJsonValid(jsonSchemaString, inputJSON)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            String message = "Exception while validating JSON.";
            PHXException px = new PHXException(message + "||" + className + ".isJSONValid()" + "||" + e.getMessage());
            throw px;
        }
    }

    private String processElement(String elementName, Object object, String parentElementName, String parentId) throws PHXException {
        String uID = null;
        try {
            Map<String, Object> elementMap = (LinkedTreeMap<String, Object>) object;
            Iterator<?> attributes = elementMap.keySet().iterator();
            Map<String, String> newElementMap = new HashMap<String, String>();
            Map<String, Object> newElementRefMap = new HashMap<String, Object>();
            jedis.incr("count_" + elementName);
            uID = elementName + jedis.get("count_" + elementName);
            while (attributes.hasNext()) {
                String attribute = (String) attributes.next();
                if (elementMap.get(attribute) instanceof Map) {
                    String id = processElement(attribute, elementMap.get(attribute), elementName, uID);
                    newElementRefMap.put(attribute, id);
                } else if (elementMap.get(attribute) instanceof ArrayList) {
                    ArrayList list = (ArrayList) elementMap.get(attribute);
                    List<String> refList = new ArrayList<String>();
                    for (int i = 0; i < list.size(); i++) {
                        if (list.get(i) instanceof Map) {
                            String id = processElement(attribute, list.get(i), elementName, uID);
                            refList.add(id);
                        }
                    }
                    newElementRefMap.put(attribute, refList);
                } else {
                    newElementMap.put(attribute, (String) elementMap.get(attribute));
                }
            }

            if (!newElementRefMap.isEmpty()) {
                String planRefMapJSON = gson.toJson(newElementRefMap);
                jedis.set("ref_" + uID, planRefMapJSON);
            }
            newElementMap.put("id", uID);
            newElementMap.put("objectType", elementName);
            newElementMap.put("createdOn", new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
            newElementMap.put("lastUpdatedOn", new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
            jedis.hmset(uID, newElementMap);
            newElementMap.put("parentElement", parentElementName);
            newElementMap.put("parentId", parentId);
            newElementMap.put("isActive","Y");
            String newElementMapString = gson.toJson(newElementMap);
            jedis.rpush("queuePlans", newElementMapString);
        } catch (Exception e) {
            String message = "Exception while processing children elements.";
            PHXException px = new PHXException(
                    message + "||" + className + ".processElement()" + "||" + e.getMessage());
            throw px;
        }
        return uID;
    }

    private Map<String, Object> processGetElement(Map<String, Object> mapObject, String id) throws PHXException {
        try {
            JSONObject refJsonObject = (JSONObject) jsonParser.parse(jedis.get("ref_" + id));
            Iterator<?> attributes = (refJsonObject.keySet()).iterator();
            while (attributes.hasNext()) {
                String attribute = (String) attributes.next();
                if (refJsonObject.get(attribute) instanceof ArrayList) {
                    ArrayList list = (ArrayList) refJsonObject.get(attribute);
                    List<Map> refList = new ArrayList<Map>();
                    for (int i = 0; i < list.size(); i++) {
                        Map<String, Object> refMap = (Map) jedis.hgetAll((String) list.get(i));
                        if (jedis.get("ref_" + (String) list.get(i)) != null) {
                            refMap = processGetElement(refMap, (String) list.get(i));
                        }
                        refList.add(refMap);
                    }
                    mapObject.put(attribute, refList);
                } else {
                    Map<String, Object> refMap = (Map) jedis.hgetAll((String) refJsonObject.get(attribute));
                    if (jedis.get("ref_" + (String) refJsonObject.get(attribute)) != null) {
                        refMap = processGetElement(refMap, (String) refJsonObject.get(attribute));
                    }
                    mapObject.put(attribute, refMap);
                }
            }
        } catch (Exception e) {
            String message = "Exception while getting Plan by Id.";
            PHXException px = new PHXException(
                    message + "||" + className + ".getPlan()" + "||" + e.getMessage());
            throw px;
        }
        return mapObject;
    }

    public String savePlan(String planString) throws PHXException {
        String uID = null;
        try {
            jsonSchemaString = readJSONSchema();
            if (isJSONValid(jsonSchemaString, planString)) {
                Map<String, Object> inputMap = gson.fromJson(planString, new TypeToken<Map<String, Object>>() {
                }.getType());
                Iterator<?> attributes = (inputMap.keySet()).iterator();
                Map<String, String> planMap = new HashMap<String, String>();
                Map<String, Object> planRefMap = new HashMap<String, Object>();
                jedis.incr("count_Plan");
                uID = "plan" + jedis.get("count_Plan");
                while (attributes.hasNext()) {
                    String attribute = (String) attributes.next();
                    if (inputMap.get(attribute) instanceof Map) {
                        String id = processElement(attribute, inputMap.get(attribute), "plan", uID);
                        planRefMap.put(attribute, id);
                    } else if (inputMap.get(attribute) instanceof ArrayList) {
                        ArrayList list = (ArrayList) inputMap.get(attribute);
                        List<String> refList = new ArrayList<String>();
                        for (int i = 0; i < list.size(); i++) {
                                if (list.get(i) instanceof Map) {
                                String id = processElement(attribute, list.get(i), "plan", uID);
                                refList.add(id);
                            }
                        }
                        planRefMap.put(attribute, refList);
                    } else {
                        planMap.put(attribute, (String) inputMap.get(attribute));
                    }
                }
                planMap.put("id", uID);
                planMap.put("objectType", "plan");
                planMap.put("createdOn", new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
                planMap.put("lastUpdatedOn", new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
                jedis.hmset(uID, planMap);
                planMap.put("parentElement","root");
                planMap.put("isActive","Y");
                String planRefMapJSON = gson.toJson(planRefMap);
                jedis.set("ref_" + uID, planRefMapJSON);
                String planMapString = gson.toJson(planMap);
                jedis.rpush("queuePlans", planMapString);
            } else {
                return "Please Check the input JSON!!! JSON is not compatible with JSON Schema!!";
            }
        } catch (Exception e) {
            String message = "Exception while saving plan.";
            PHXException px = new PHXException(
                    message + "||" + className + ".savePlan()" + "||" + e.getMessage());
            throw px;
        }
        return uID;
    }

    public JSONObject getPlan(String id) throws PHXException {
        JSONObject jsonObject = null;
        if (!jedis.hgetAll(id).isEmpty()) {
            try {
                Map<String, Object> planMap = (Map) jedis.hgetAll(id);
                planMap = processGetElement(planMap, id);
                String planMapString = gson.toJson(planMap);
                jsonObject = (JSONObject) jsonParser.parse(planMapString);
            } catch (Exception e) {
                String message = "Exception while getting Plan by Id.";
                PHXException px = new PHXException(
                        message + "||" + className + ".getPlan()" + "||" + e.getMessage());
                throw px;
            }
        }
        return jsonObject;
    }

    public List<JSONObject> getPlans() throws PHXException {
        List<JSONObject> jsonObjects = null;
        try {
            jsonObjects = new ArrayList<>();
            Set<String> keys = jedis.keys("plan*");
            Iterator<String> iterator = keys.iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                if (!key.substring(0, 5).equalsIgnoreCase("plan_")) {
                    Map<String, Object> planMap = (Map) jedis.hgetAll(key);
                    planMap = processGetElement(planMap, key);
                    String planMapString = gson.toJson(planMap);
                    jsonObject = (JSONObject) jsonParser.parse(planMapString);
                    jsonObjects.add(jsonObject);
                }
            }
        } catch (Exception e) {
            String message = "Exception while getting all the plans.";
            PHXException px = new PHXException(
                    message + "||" + className + ".getPlans()" + "||" + e.getMessage());
            throw px;
        }
        return jsonObjects;
    }

    public String patchPlan(String id, String planString) throws PHXException {
        try {
            jsonSchemaString = readJSONSchema();
            jsonObject = (JSONObject) jsonParser.parse(planString);
            jsonPatchObject = (JSONObject) jsonParser.parse(jsonSchemaString);
            jsonPatchObject.remove("required");
            jsonPatchSchemaString = jsonPatchObject.toString();
            Map<String, String> planMap = jedis.hgetAll(id);
            if (planMap.isEmpty()) {
                return null;
            } else if (!isJSONValid(jsonPatchSchemaString, planString)) {
                return "Please Check the input JSON!!! JSON is not compatible with JSON Schema!!";
            } else {
                JSONObject refJsonObject = (JSONObject) jsonParser.parse(jedis.get("ref_" + id));
                Map<String, Object> schemaMap = new Gson().fromJson(jsonSchemaString, new TypeToken<Map<String, Object>>() {
                }.getType());
                Map<String, Map> propertiesMap = (Map<String, Map>) schemaMap.get("properties");
                Iterator<?> properties = (propertiesMap.keySet()).iterator();
                Map<String, Object> newObjectMap = new Gson().fromJson(planString, new TypeToken<Map<String, Object>>() {
                }.getType());
                while (properties.hasNext()) {
                    String property = (String) properties.next();
                    if (newObjectMap.get(property) instanceof List) {
                        ArrayList list = (ArrayList) refJsonObject.get(property);
                        ArrayList newList = (ArrayList) newObjectMap.get(property);
                        for (int i = 0; i < newList.size(); i++) {
                            Map<String, String> newMap = (Map<String, String>) newList.get(i);
                            if (newMap.get("id") != null && !list.contains(newMap.get("id"))) {
                                return "Please check the id for " + property + "! " + newMap.get("id") + " is invalid!";
                            }
                        }
                    }
                }
                properties = (propertiesMap.keySet()).iterator();
                while (properties.hasNext()) {
                    String property = (String) properties.next();
                    if (propertiesMap.get(property).get("type").equals("array") && newObjectMap.get(property) instanceof List) {
                        ArrayList list = (ArrayList) refJsonObject.get(property);
                        ArrayList newList1 = null;
                        ArrayList newList = (ArrayList) newObjectMap.get(property);
                        for (int i = 0; i < newList.size(); i++) {
                            Map<String, String> newMap = (Map<String, String>) newList.get(i);
                            if (newMap.get("id") != null && list != null) {
                                Map<String, String> refMap = jedis.hgetAll(newMap.get("id"));
                                Map<String, List> items = (Map<String, List>) propertiesMap.get(property).get("items");
                                Map<String, String> objectSchemaMap = (Map<String, String>) items.get("properties");
                                Iterator<?> objectMapAtts = (objectSchemaMap.keySet()).iterator();
                                while (objectMapAtts.hasNext()) {
                                    String objectMapAtt = (String) objectMapAtts.next();
                                    refMap.put(objectMapAtt, newMap.get(objectMapAtt));
                                }
                                refMap.put("lastUpdatedOn", new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
                                jedis.hmset(newMap.get("id"), refMap);
                                refMap.put("parentElement", "plan");
                                refMap.put("parentId", id);
                                planMap.put("isActive","Y");
                                String newElementMapString = gson.toJson(refMap);
                                jedis.rpush("queuePlans", newElementMapString);
                            } else if (newMap.get("id") == null && list != null) {
                                String savedId = processElement(property, newMap, "plan", id);
                                list.add(savedId);
                                refJsonObject.put(property, list);
                                jedis.set("ref_" + id, gson.toJson(refJsonObject));
                            } else if (newMap.get("id") == null && list == null) {
                                String savedId = processElement(property, newMap, "plan", id);
                                if (newList1 == null) {
                                    newList1 = new ArrayList();
                                }
                                newList1.add(savedId);
                                refJsonObject.put(property, newList1);
                                jedis.set("ref_" + id, gson.toJson(refJsonObject));
                            } else {
                                return "Please check the id for " + property + "! " + newMap.get("id") + " is invalid!";
                            }
                        }
                    } else if (propertiesMap.get(property).get("type").equals("object") && newObjectMap.get(property) instanceof Map) {
                        Map<String, Object> newMap = (Map<String, Object>) newObjectMap.get(property);
                        Map<String, Object> planRefMap = null;
                        if (refJsonObject.get(property) != null) {
                            Map<String, String> refMap = jedis.hgetAll((String) refJsonObject.get(property));
                            Map<String, String> objectSchemaMap = (Map<String, String>) propertiesMap.get(property).get("properties");
                            Iterator<?> objectMapAtts = (objectSchemaMap.keySet()).iterator();
                            while (objectMapAtts.hasNext()) {
                                String objectMapAtt = (String) objectMapAtts.next();
                                if (newMap.get(objectMapAtt) instanceof Map) {
                                    String refId = processElement(objectMapAtt, newMap.get(objectMapAtt), "plan", id);
                                    if (planRefMap == null) {
                                        planRefMap = new HashMap<String, Object>();
                                    }
                                    planRefMap.put(objectMapAtt, refId);
                                } else if (newMap.get(objectMapAtt) instanceof ArrayList) {
                                    ArrayList list = (ArrayList) newMap.get(objectMapAtt);
                                    List<String> refList = new ArrayList<String>();
                                    for (int i = 0; i < list.size(); i++) {
                                        if (list.get(i) instanceof Map) {
                                            String refId = processElement(objectMapAtt, list.get(i), "plan", id);
                                            refList.add(refId);
                                        }
                                    }
                                    if (planRefMap == null) {
                                        planRefMap = new HashMap<String, Object>();
                                    }
                                    planRefMap.put(objectMapAtt, refList);
                                } else {
                                    refMap.put(objectMapAtt, (String) newMap.get(objectMapAtt));
                                }
                            }
                            if (planRefMap != null) {
                                String planRefMapJSON = gson.toJson(planRefMap);
                                jedis.set("ref_" + (String) refJsonObject.get(property), planRefMapJSON);
                            }
                            refMap.put("lastUpdatedOn", new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
                            jedis.hmset((String) refJsonObject.get(property), refMap);
                            refMap.put("parentElement", "plan");
                            refMap.put("parentId", id);
                            refMap.put("isActive","Y");
                            String newElementMapString = gson.toJson(refMap);
                            jedis.rpush("queuePlans", newElementMapString);
                        } else {
                            String savedId = processElement(property, newMap, "plan", id);
                            refJsonObject.put(property, savedId);
                            jedis.set("ref_" + id, gson.toJson(refJsonObject));
                        }
                    } else if (propertiesMap.get(property).get("type").equals("string") && newObjectMap.get(property) != null) {
                        planMap.put(property, (String) newObjectMap.get(property));
                    }
                }
                planMap.put("lastUpdatedOn", new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
                jedis.hmset(id, planMap);
                planMap.put("parentElement","root");
                planMap.put("isActive","Y");
                String planMapString = gson.toJson(planMap);
                jedis.rpush("queuePlans", planMapString);
                return "1";
            }
        } catch (ParseException e) {
            String message = "Exception while updating plan.";
            PHXException px = new PHXException(
                    message + "||" + className + ".patchPlan()" + "||" + e.getMessage());
            throw px;
        }

    }

    private void deleteChildren(String id) throws PHXException {
        try {
            JSONObject refJsonObject = (JSONObject) jsonParser.parse(jedis.get("ref_" + id));
            Iterator<?> properties = (refJsonObject.keySet()).iterator();
            while (properties.hasNext()) {
                String property = (String) properties.next();
                if (refJsonObject.get(property) instanceof List) {
                    ArrayList list = (ArrayList) refJsonObject.get(property);
                    for (int i = 0; i < list.size(); i++) {
                        jedis.del((String) list.get(i));
                        if (jedis.get("ref_"+(String) list.get(i)) !=null){
                            deleteChildren((String) list.get(i));
                        }
                        Map<String, String> deleteMap = new HashMap<String, String>();
                        deleteMap.put("isActive", "N");
                        deleteMap.put("objectType", property);
                        deleteMap.put("id", (String) list.get(i));
                        deleteMap.put("parentId", id);
                        String planMapString = gson.toJson(deleteMap);
                        jedis.rpush("queuePlans", planMapString);
                    }
                }
                else {
                    jedis.del((String) refJsonObject.get(property));
                    if (jedis.get("ref_"+(String) refJsonObject.get(property)) !=null){
                        deleteChildren((String) refJsonObject.get(property));
                    }
                    Map<String, String> deleteMap = new HashMap<String, String>();
                    deleteMap.put("isActive", "N");
                    deleteMap.put("objectType", property);
                    deleteMap.put("id", (String) refJsonObject.get(property));
                    deleteMap.put("parentId", id);
                    String planMapString = gson.toJson(deleteMap);
                    jedis.rpush("queuePlans", planMapString);
                }
            }
            jedis.del("ref_" + id);

        } catch (Exception e) {
            String message = "Exception while deleting children.";
            PHXException px = new PHXException(
                    message + "||" + className + ".deleteChildren()" + "||" + e.getMessage());
            throw px;
        }
    }

    public boolean deletePlan(String planId) throws PHXException {
        JSONObject jsonObject = null;
        try {
            if (!jedis.hgetAll(planId).isEmpty()) {
                jedis.del(planId);
                if (jedis.get("ref_"+planId) !=null){
                    deleteChildren(planId);
                }
                Map<String, String> deleteMap = new HashMap<String, String>();
                deleteMap.put("isActive", "N");
                deleteMap.put("objectType", "plan");
                deleteMap.put("id", planId);
                String planMapString = gson.toJson(deleteMap);
                jedis.rpush("queuePlans", planMapString);
                return true;
            }
            else {
                return false;
            }
        } catch (Exception e) {
            String message = "Exception while deleting Plan by Id.";
            PHXException px = new PHXException(
                    message + "||" + className + ".deletePlan()" + "||" + e.getMessage());
            throw px;
        }
    }
}
