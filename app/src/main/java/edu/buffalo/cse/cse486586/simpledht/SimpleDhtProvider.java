package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    private static final String TAG = SimpleDhtProvider.class.getName();

    private ChordRing chordRing;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        return (int) chordRing.delete(selection);
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        chordRing.insert(values);
        return uri;
    }

    @Override
    public boolean onCreate() {
        this.chordRing = ChordRing.chordRing(getContext());
        this.chordRing.register();
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        return chordRing.query(selection);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }
}
