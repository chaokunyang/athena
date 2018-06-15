package com.timeyang.athena.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;

/**
 * Json utils
 *
 * @author https://github.com/chaokunyang
 */
public class JsonUtils {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        // mapper.registerModule(new JsonOrgModule());
        mapper.findAndRegisterModules();
        // mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public static String convertToString(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] convertToBytes(Object o) {
        try {
            return mapper.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static JSONObject readValue(InputStream inputStream) throws IOException {
        return mapper.readValue(inputStream, JSONObject.class);
    }

    public static <T> T parseJsonToObject(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T parseJsonToObject(String json, TypeReference typeReference) {
        try {
            return mapper.readValue(json, typeReference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
