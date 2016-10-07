package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.util.Pair;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static edu.buffalo.cse.cse486586.simpledht.Nodes.hashForPort;
import static edu.buffalo.cse.cse486586.simpledht.Nodes.portForHash;
import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtDB.db;

public class ChordRing {
    private static final String TAG = ChordRing.class.getName();
    private static ChordRing INSTANCE = null;
    public static final String ALL = "*";
    public static final String LOCAL = "@";
    private static final List<String> RING;
    private String successor;
    private String predecessor;

    private static Map<UUID, Map<String, String>> QUERY_REPLIES = new ConcurrentHashMap<>();
    private static Map<UUID, Semaphore> QUERY_STATUS = new ConcurrentHashMap<>();

    private final Context context;
    private final SimpleDhtDB db;
    private String myId;
    private String myPort;
    private Uri uri;

    static {
        RING = new ArrayList<String>(5) {
            // NOTE: this is terrible!, violates interface contract.
            // Ideally we should use a Ring-buffer.
            @Override
            public String get(int index) {
                return (index < 0) ? super.get(size() + index) : super.get(index % size());
            }
        };
    }


    private ChordRing(Context context) {
        this.context = context;
        this.db = db(context);

        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        this.myId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        this.myPort = String.valueOf((Integer.parseInt(myId) * 2));

        this.uri = new Uri.Builder()
                .scheme("content")
                .authority("edu.buffalo.cse.cse486586.simpledht.provider")
                .build();

        if (myId.equals("5554")) {
            RING.add(hashForPort("11108"));
        }
    }

    public static ChordRing chordRing(Context context) {
        if (INSTANCE == null) {
            synchronized (ChordRing.class) {
                INSTANCE = new ChordRing(context);
            }
        }
        return INSTANCE;
    }

    public void handle(Payload payload) {
        switch (payload.getType()) {
            case JOIN: {
                Log.d(TAG, "JOIN " + payload);
                String nodeId = hashForPort(payload.getFromPort());
                if (!RING.contains(nodeId)) {
                    RING.add(nodeId);
                    Collections.sort(RING);

                    int index = RING.indexOf(nodeId);
                    Payload joinReply = Payload.update(RING.get(index - 1), RING.get(index + 1));

                    int preIndex = RING.indexOf(joinReply.getPredecessor());
                    Payload preUpdate = Payload.update(RING.get(preIndex - 1), RING.get(preIndex + 1));

                    int sucIndex = RING.indexOf(joinReply.getSuccessor());
                    Payload sucUpdate = Payload.update(RING.get(sucIndex - 1), RING.get(sucIndex + 1));

                    new ClientTask(context).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Pair.create(payload.getFromPort(), joinReply),
                            Pair.create(portForHash(joinReply.getPredecessor()), preUpdate),
                            Pair.create(portForHash(joinReply.getSuccessor()), sucUpdate));
                }
                Log.d(TAG, "RING: " + RING);
                break;
            }
            case UPDATE: {
                this.successor = payload.getSuccessor();
                this.predecessor = payload.getPredecessor();
                Log.d(TAG, "UPDATE PREDECESSOR: " + portForHash(predecessor) + " SUCCESSOR: " + portForHash(successor));
                break;
            }
            case DELETE: {
                if (!payload.getFromPort().equals(myPort)) {
                    if (payload.getKey().equals(ALL)) {
                        Log.d(TAG, "DELETE ALL: " + payload);
                        db.drop();
                        forwardToSuccessor(payload);
                    } else if (inLocalPartition(genHash(payload.getKey()))) {
                        Log.d(TAG, "DELETE IN LOCAL PARTITION: " + payload);
                        db.delete(payload.getKey());
                    } else {
                        forwardToSuccessor(payload);
                    }
                } else {
                    Log.d(TAG, "DELETE CYCLED " + payload);
                }
                break;
            }
            case INSERT: {
                if (inLocalPartition(genHash(payload.getKey()))) {
                    ContentValues contentValues = new ContentValues(1);
                    contentValues.put("key", payload.getKey());
                    contentValues.put("value", payload.getValue());
                    db.insert(contentValues);
                    Log.d(TAG, "INSERTED IN LOCAL PARTITION: " + contentValues.toString());
                } else {
                    forwardToSuccessor(payload);
                }
                break;
            }
            case QUERY_REQUEST: {
                if (payload.getFromPort().equals(myPort) || payload.isCompleted()) {
                    Log.d(TAG, "QUERY REQUEST CYCLED OR COMPLETED " + payload);
                    QUERY_STATUS.get(payload.getQueryId()).release();
                } else {
                    if (payload.getKey().equals(ALL)) {
                        Cursor cursor = db.all();
                        Payload queryReply = Payload.queryReply(myPort, payload.getQueryId(), false);
                        if (cursor != null && cursor.getCount() > 0) {
                            cursor.moveToFirst();
                            for (int i = 0; i < cursor.getCount(); i++) {
                                queryReply.getQueryResults().put(cursor.getString(0), cursor.getString(1));
                                cursor.moveToNext();
                            }
                        }
                        sendTo(payload.getFromPort(), queryReply);
                        forwardToSuccessor(payload);
                        Log.d(TAG, "QUERY REQUEST ALL " + payload);
                    } else if (inLocalPartition(genHash(payload.getKey()))) {
                        Cursor cursor = db.query(payload.getKey());
                        Payload queryReply = Payload.queryReply(myPort, payload.getQueryId(), true);
                        if (cursor != null && cursor.getCount() > 0) {
                            cursor.moveToFirst();
                            for (int i = 0; i < cursor.getCount(); i++) {
                                queryReply.getQueryResults().put(cursor.getString(0), cursor.getString(1));
                                cursor.moveToNext();
                            }
                        }
                        sendTo(payload.getFromPort(), queryReply);
                        Log.d(TAG, "QUERY REQUEST IN PARTITION " + payload);
                    } else {
                        forwardToSuccessor(payload);
                        Log.d(TAG, "QUERY REQUEST FORWARDED TO SUCCESSOR: " + payload);
                    }
                }
                break;
            }
            case QUERY_REPLY: {
                Log.d(TAG, "QUERY REPLY:" + payload);
                QUERY_REPLIES.get(payload.getQueryId()).putAll(payload.getQueryResults());
                if (payload.isCompleted()) {
                    Log.d(TAG, "QUERY REPLY COMPLETED " + payload);
                    QUERY_STATUS.get(payload.getQueryId()).release();
                }
                break;
            }
        }
    }

    private boolean inLocalPartition(String hash) {
        String nodeId = hashForPort(myPort);
        return (predecessor == null)
                || (predecessor.compareTo(nodeId) > 0 && (hash.compareTo(predecessor) > 0 || hash.compareTo(nodeId) <= 0))
                || (hash.compareTo(predecessor) > 0 && hash.compareTo(nodeId) <= 0);
    }

    private void forwardToSuccessor(Payload payload) {
        if (successor != null) {
            sendTo(portForHash(successor), payload);
            Log.d(TAG, "FORWARDED TO SUCCESSOR:" + portForHash(successor) + " : " + payload);
        }
    }

    private void sendTo(String fromPort, Payload payload) {
        new ClientTask(context).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                Pair.create(fromPort, payload));
    }

    public static String genHash(String input) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Should not happen!.");
            Log.e(TAG, e.getMessage());
            System.exit(1);
            return "";
        }
    }

    public void register() {
        try {
            ServerSocket serverSocket = new ServerSocket(Nodes.server());
            serverSocket.setReuseAddress(true);
            new ServerTask(context)
                    .executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        if (!myId.equals("5554")) {
            Log.d(TAG, "REGISTERING");
            new ClientTask(context).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    Pair.create("11108", Payload.joinRequest(myPort)));
        }
    }

    public long delete(String key) {
        switch (key) {
            case ALL: {
                db.drop();
                forwardToSuccessor(Payload.delete(myPort, ALL));
                break;
            }
            case LOCAL: {
                return db.drop();
            }
            default: {
                if (inLocalPartition(genHash(key))) {
                    return db.delete(key);
                } else {
                    forwardToSuccessor(Payload.delete(myPort, ALL));
                }
                break;
            }
        }
        return 0L;
    }

    public void insert(ContentValues values) {
        String key = values.getAsString("key");
        if (inLocalPartition(genHash(key)) || successor == null) {
            db.insert(values);
            Log.v(TAG, "Inserted " + values.toString());
        } else {
            forwardToSuccessor(Payload.insert(myPort, key, values.getAsString("value")));
        }
    }

    public Cursor query(String key) {
        switch (key) {
            case ALL: {
                Log.d(TAG, "QUERY ALL: " + key);
                MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});

                Payload query = queryRequest(ALL);

                Cursor cursor = db.all();
                if (cursor != null && cursor.getCount() > 0) {
                    cursor.moveToFirst();
                    for (int i = 0; i < cursor.getCount(); i++) {
                        result.addRow(new String[]{cursor.getString(0), cursor.getString(1)});
                        cursor.moveToNext();
                    }
                }
                forwardQueryAndBlock(key, result, query);
                return result;
            }
            case LOCAL: {
                Log.d(TAG, "QUERY for LOCAL");
                return db.all();
            }
            default: {
                if (inLocalPartition(genHash(key))) {
                    Log.d(TAG, "QUERY for key " + key + " is IN LOCAL PARTITION");
                    return db.query(key);
                } else {
                    Log.d(TAG, "QUERY for key " + key + " is NOT IN LOCAL PARTITION");
                    MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});

                    Payload query = queryRequest(key);
                    forwardQueryAndBlock(key, result, query);
                    return result;
                }
            }
        }
    }

    private Payload queryRequest(String key) {
        Payload query = Payload.queryRequest(myPort, key);
        QUERY_REPLIES.put(query.getQueryId(), new HashMap<String, String>());
        QUERY_STATUS.put(query.getQueryId(), new Semaphore(0));
        return query;
    }

    private void forwardQueryAndBlock(String key, MatrixCursor result, Payload query) {
        if (successor != null) {
            forwardToSuccessor(query);
            QUERY_STATUS.get(query.getQueryId()).acquireUninterruptibly();
        }
        for (Map.Entry<String, String> entry : QUERY_REPLIES.get(query.getQueryId()).entrySet()) {
            result.addRow(new String[]{entry.getKey(), entry.getValue()});
        }
        QUERY_REPLIES.remove(query.getQueryId());
        QUERY_STATUS.remove(query.getQueryId());
    }
}
