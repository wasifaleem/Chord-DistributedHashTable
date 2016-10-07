package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

class ServerTask extends AsyncTask<ServerSocket, String, Void> {
    private static final String TAG = ServerTask.class.getName();
    private final Context context;

    public ServerTask(Context context) {
        this.context = context;
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        while (true) {
            try (Socket socket = serverSocket.accept();
                 BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                StringBuilder total = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    total.append(line);
                }
                ChordRing.chordRing(context).handle(Payload.deserialize(total.toString()));
            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException");
            }
        }
    }
}