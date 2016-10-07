package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static edu.buffalo.cse.cse486586.simpledht.ChordRing.genHash;

public class Nodes {
    private static final String TAG = Nodes.class.getName();

    private static final int SERVER = 10000;
    private static final List<String> NODES;
    private static final Map<String, String> NODES_HASH; // <port,  hash>
    private static final Map<String, String> HASH_NODE; // <hash,  port>

    static {
        NODES = new ArrayList<>(5);
        NODES.add("11108");
        NODES.add("11112");
        NODES.add("11116");
        NODES.add("11120");
        NODES.add("11124");

        NODES_HASH = new HashMap<>(5);
        HASH_NODE = new HashMap<>(5);
        for (String node : NODES) {
            String hash = genHash(String.valueOf(Integer.valueOf(node) / 2));
            NODES_HASH.put(node, hash);
            HASH_NODE.put(hash, node);
        }
    }

    public static int server() {
        return SERVER;
    }

    public static Collection<String> all() {
        return NODES;
    }

    public static String hashForPort(String node) {
        return NODES_HASH.get(node);
    }

    public static String portForHash(String id) {
        return HASH_NODE.get(id);
    }
}
