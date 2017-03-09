package com.sumit.service;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.sumit.PHXException.PHXException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.json.simple.parser.ParseException;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by Sumit on 11/18/2016.
 */
@SuppressWarnings("ALL")
public class SchemaService {
    private final String className = getClass().getName();
    private static final String redisHost = "localhost";
    private static final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    private JedisPool jedisPool;
    private Jedis jedis;
    private JSONParser jsonParser;

    public SchemaService() throws PHXException {
        jsonParser = new JSONParser();
        try {
            jedis = getJedisConnectionPool().getResource();
        } catch (PHXException e) {
            String message = "Exception while creating Schema service controller.";
            PHXException px = new PHXException(message + "||" + className + ".SchemaService()" + "||" + e.getMessage());
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

    public String saveSchema(String schemaString) throws PHXException {
        try {
            if (jedis.get("JSONSchema_HealthPlan") != null){
                String jsonSchemaString = jedis.get("JSONSchema_HealthPlan");
                Map<String, Object> oldSchemaMap = new Gson().fromJson(jsonSchemaString, new TypeToken<Map<String, Object>>() {
                }.getType());
                Map<String, Map> oldPropertyMap = (Map<String, Map>) oldSchemaMap.get("properties");
                Map<String, Object> newSchemaMap = new Gson().fromJson(schemaString, new TypeToken<Map<String, Object>>() {
                }.getType());
                Map<String, Map> newPropertyMap = (Map<String, Map>) newSchemaMap.get("properties");
                String result = validateSchema(oldPropertyMap, newPropertyMap);
                if (result==null){
                    jedis.set("JSONSchema_HealthPlan", schemaString);
                    return null;
                }
                else {
                    return result;
                }

            }
            else {
                jedis.set("JSONSchema_HealthPlan", schemaString);
                return null;
            }
        } catch (Exception e) {
            String message = "Exception while saving schema.";
            PHXException px = new PHXException(message + "||" + className + ".saveSchema()" + "||" + e.getMessage());
            throw px;
        }
    }

    public JSONObject getSchema() throws PHXException {
        JSONObject schemaJSONObject = null;
        if (jedis.get("JSONSchema_HealthPlan") != null) {
            try {
                schemaJSONObject = (JSONObject) jsonParser.parse(jedis.get("JSONSchema_HealthPlan"));
            } catch (ParseException e) {
                String message = "Exception while getting Schema.";
                throw new PHXException(message + "||"
                        + className + ".getSchema()" + "||" + e.getMessage());
            }
        }
        return schemaJSONObject;
    }

    private String validateSchema(Map<String, Map> oldMap, Map<String, Map> newMap){
        String result = null;
        Iterator<?> properties = (oldMap.keySet()).iterator();
        while (properties.hasNext()) {
            String property = (String) properties.next();
            if (oldMap.get(property).get("type").equals("string")){
                if (newMap.get(property) == null || !newMap.get(property).get("type").equals("string")){
                    return "Can not remove "+property+" from schema!";
                }
            }
            else if(oldMap.get(property).get("type").equals("object")){
                if (newMap.get(property) == null || !newMap.get(property).get("type").equals("object")){
                    return "Can not remove "+property+" from schema!";
                }
                else {
                    Map<String, Map> oldPropertyMap = (Map<String, Map>) oldMap.get(property).get("properties");
                    Map<String, Map> newPropertyMap = (Map<String, Map>) newMap.get(property).get("properties");
                    result = validateSchema(oldPropertyMap, newPropertyMap);
                    if (result!=null){
                        return result;
                    }
                }
            }
            else if (oldMap.get(property).get("type").equals("array")){
                if (newMap.get(property) == null || !newMap.get(property).get("type").equals("array")){
                    return "Can not remove "+property+" from schema!";
                }
                else {
                    Map<String, Map> oldPropertyMap = (Map<String, Map>)((Map<String, Map>) oldMap.get(property).get("items")).get("properties");
                    Map<String, Map> newPropertyMap = (Map<String, Map>)((Map<String, Map>) newMap.get(property).get("items")).get("properties");
                    result = validateSchema(oldPropertyMap, newPropertyMap);
                    if (result!=null){
                        return result;
                    }
                }
            }
            else {
                return "Invalid Schema";
            }

        }
        return result;
    }

}
