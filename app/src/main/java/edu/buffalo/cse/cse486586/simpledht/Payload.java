package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static edu.buffalo.cse.cse486586.simpledht.Nodes.portForHash;

public class Payload {
    private static final String TAG = ChordRing.class.getName();

    private static final long serialVersionUID = -6229103345195763019L;
    private UUID queryId = UUID.randomUUID();
    private boolean completed = false;
    private String fromPort;
    private Type type;
    private String key;
    private String value;
    private String successor;
    private String predecessor;
    private Map<String, String> queryResults = new HashMap<>();

    public String serialize() {
        try {
            JSONObject jsonObject = new JSONObject()
                    .put("queryId", queryId)
                    .put("completed", completed)
                    .put("fromPort", fromPort)
                    .put("type", type)
                    .put("key", key)
                    .put("value", value)
                    .put("successor", successor)
                    .put("predecessor", predecessor)
                    .put("queryResults", new JSONObject(queryResults));
            return jsonObject.toString();
        } catch (JSONException e) {
            Log.e(TAG, "Cannot serialize payload " + toString(), e);
            return "";
        }
    }

    public static Payload deserialize(String json) {
        try {
            JSONObject jsonObject = new JSONObject(json);
            Payload payload = new Payload();
            if (!jsonObject.isNull("queryId")) {
                payload.queryId = UUID.fromString(jsonObject.getString("queryId"));
            }
            if (!jsonObject.isNull("completed")) {
                payload.completed = jsonObject.getBoolean("completed");
            }
            if (!jsonObject.isNull("fromPort")) {
                payload.fromPort = jsonObject.getString("fromPort");
            }
            if (!jsonObject.isNull("type")) {
                payload.type = Type.valueOf(jsonObject.getString("type"));
            }
            if (!jsonObject.isNull("key")) {
                payload.key = jsonObject.getString("key");
            }
            if (!jsonObject.isNull("value")) {
                payload.value = jsonObject.getString("value");
            }
            if (!jsonObject.isNull("successor")) {
                payload.successor = jsonObject.getString("successor");
            }
            if (!jsonObject.isNull("predecessor")) {
                payload.predecessor = jsonObject.getString("predecessor");
            }
            if (!jsonObject.isNull("queryResults")) {
                JSONObject queryResults = jsonObject.getJSONObject("queryResults");
                Iterator<String> keys = queryResults.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    payload.queryResults.put(key, queryResults.getString(key));
                }
            }
            return payload;
        } catch (JSONException e) {
            Log.e(TAG, "Cannot deserialize: " + json, e);
        }
        return null;
    }

    enum Type {
        JOIN, UPDATE, DELETE, INSERT, QUERY_REQUEST, QUERY_REPLY
    }

    public static Payload joinRequest(String myPort) {
        Payload payload = new Payload();
        payload.fromPort = myPort;
        payload.type = Type.JOIN;
        return payload;
    }

    public static Payload update(String predecessor, String successor) {
        Payload payload = new Payload();
        payload.predecessor = predecessor;
        payload.successor = successor;
        payload.type = Type.UPDATE;
        return payload;
    }

    public static Payload delete(String fromPort, String key) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.key = key;
        payload.type = Type.DELETE;
        return payload;
    }

    public static Payload insert(String fromPort, String key, String value) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.key = key;
        payload.value = value;
        payload.type = Type.INSERT;
        return payload;
    }

    public static Payload queryRequest(String fromPort, String key) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.key = key;
        payload.type = Type.QUERY_REQUEST;
        return payload;
    }

    public static Payload queryReply(String fromPort, UUID queryId, boolean completed) {
        Payload payload = new Payload();
        payload.fromPort = fromPort;
        payload.queryId = queryId;
        payload.type = Type.QUERY_REPLY;
        payload.completed = completed;
        return payload;
    }

    public UUID getQueryId() {
        return queryId;
    }

    public boolean isCompleted() {
        return completed;
    }

    public String getFromPort() {
        return fromPort;
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getSuccessor() {
        return successor;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public Map<String, String> getQueryResults() {
        return queryResults;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Payload{");
        sb.append("type=").append(type);
        sb.append(", fromPort='").append(fromPort).append('\'');
        sb.append(", key='").append(key).append('\'');
        sb.append(", value='").append(value).append('\'');
        sb.append(", queryId=").append(queryId);
        sb.append(", completed=").append(completed);
        sb.append(", successor='").append(successor).append('\'');
        sb.append(", predecessor='").append(predecessor).append('\'');
        sb.append(", queryResults=").append(queryResults);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Payload payload = (Payload) o;
        return Objects.equals(completed, payload.completed) &&
                Objects.equals(queryId, payload.queryId) &&
                Objects.equals(fromPort, payload.fromPort) &&
                Objects.equals(type, payload.type) &&
                Objects.equals(key, payload.key) &&
                Objects.equals(value, payload.value) &&
                Objects.equals(successor, payload.successor) &&
                Objects.equals(predecessor, payload.predecessor) &&
                Objects.equals(queryResults, payload.queryResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, completed, fromPort, type, key, value, successor, predecessor, queryResults);
    }
}
