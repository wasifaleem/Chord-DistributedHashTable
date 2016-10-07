package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.util.Log;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE;

public class SimpleDhtDB extends SQLiteOpenHelper {
    private static final String TAG = SimpleDhtDB.class.getName();
    private static SimpleDhtDB INSTANCE = null;

    public static final String TABLE = "kv_store";
    public static final int DB_VERSION = 1;

    private SimpleDhtDB(Context context) {
        super(context, TABLE, null, DB_VERSION);
    }

    public static SimpleDhtDB db(Context context) {
        if (INSTANCE == null) {
            synchronized (SimpleDhtDB.class) {
                INSTANCE = new SimpleDhtDB(context);
            }
        }
        return INSTANCE;
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT)");
        Log.d(TAG, "Created DB");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS kv_store");
        onCreate(db);
    }

    public int drop() {
        return getWritableDatabase().delete(TABLE, null, null);
    }

    public long insert(ContentValues keyValue) {
        return getWritableDatabase().insertWithOnConflict(TABLE, null, keyValue, CONFLICT_REPLACE);
    }

    public long delete(String key) {
        return getWritableDatabase().delete(TABLE, "key = ?", new String[]{key});
    }

    public Cursor query(String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
        sqLiteQueryBuilder.setTables(TABLE);
        return sqLiteQueryBuilder.query(getWritableDatabase(), projection, selection, selectionArgs, null, null, sortOrder);
    }

    public Cursor query(String key) {
        return getWritableDatabase().rawQuery("SELECT * from " + TABLE + " where key = ?", new String[]{key});
    }

    public Cursor all() {
        return getWritableDatabase().rawQuery("SELECT * from " + TABLE, null);
    }
}
