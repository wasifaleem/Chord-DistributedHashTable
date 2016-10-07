package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;
import android.util.Pair;

import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class ClientTask extends AsyncTask<Pair<String, Payload>, Void, Void> {
    private static final String TAG = ClientTask.class.getName();
    private final Context context;

    public ClientTask(Context context) {
        this.context = context;
    }

    @Override
    protected Void doInBackground(Pair<String, Payload>... payloads) { // <to, payload>
        for (Pair<String, Payload> payloadPair : payloads) {
            String node = payloadPair.first;
            try (Socket socket = new Socket(
                    InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(node));
                 OutputStream out = socket.getOutputStream()) {
                out.write(payloadPair.second.serialize().getBytes(StandardCharsets.UTF_8));
                out.flush();
            } catch (Exception e) {
                Log.e(TAG, "ClientTask socket Exception" + " while sending to " + node, e);
            }
        }

        return null;
    }
}