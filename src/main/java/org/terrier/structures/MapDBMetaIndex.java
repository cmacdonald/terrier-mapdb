package org.terrier.structures;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.serializer.SerializerCompressionWrapper;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class MapDBMetaIndex implements MetaIndex {
   
    public static String FILENAME_SUFFIX = ".db";

    protected String[] keyNames;
    protected String[] revKeyNames;
    protected Map<String,List<String>> forwardmeta = new HashMap<>();
    protected Map<String,Map<String,Integer>> reversemeta = new HashMap<>();
    protected DB db;

    public static String construct_filename(IndexOnDisk index, String structureName) {
        return index.getPath() + "/" + index.getPrefix() + "."  + structureName + FILENAME_SUFFIX;
    }

    public MapDBMetaIndex(IndexOnDisk index, String structureName)
    {
        String db_filename = construct_filename(index, structureName);
        //source: http://www.mapdb.org/book/performance/
        db = DBMaker.fileDB(db_filename)
            .fileMmapEnableIfSupported() // Only enable mmap on supported platforms
            .fileMmapPreclearDisable()   // Make mmap file faster
            .readOnly()
            .make();

        keyNames = index.getIndexProperty("index."+structureName+".key-names", "").split("\\s*,\\s*");
        String[] sCompress = index.getIndexProperty("index."+structureName+".key-compress", "").split("\\s*,\\s*");
        int ki=0;
        for(String k : keyNames)
        {
            GroupSerializer<String> ser = Serializer.STRING;
            if (Boolean.parseBoolean(sCompress[ki]))
                ser = new SerializerCompressionWrapper<String>(ser);

            forwardmeta.put(k, db.indexTreeList("forward-" + k, ser).open());
            ki++;
        }

        revKeyNames = index.getIndexProperty("index."+structureName+".reverse-key-names", "").split("\\s*,\\s*");
        for(String k : revKeyNames)
        {
            Map<String, Integer> map = db.hashMap("reverse-" + k)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .open();
            reversemeta.put(k, map);
        }
    }


    @Override
    public void close() {
        db.close();
    }

    @Override
    public int size() {
        return forwardmeta.get(keyNames[0]).size();
    }

    @Override
    public String getItem(String Key, int docid) {
        return forwardmeta.get(Key).get(docid);
    }

    @Override
    public String[] getAllItems(int docid) {
        String[] rtr = new String[keyNames.length];
        int i=0;
        for(String k : keyNames)
        {
            rtr[i] = forwardmeta.get(keyNames[i]).get(docid);
            i++;
        }
        return rtr;
    }

    @Override
    public String[] getItems(String Key, int[] docids) {
        String[] rtr = new String[docids.length];
        int i=0;
        for(int docid : docids)
        {
            rtr[i] = getItem(Key, docid);
            i++;
        }
        return rtr;
    }

    @Override
    public String[] getItems(String[] keys, int docid) {
        String[] rtr = new String[keys.length];
        int i=0;
        for(String k : keys)
        {
            rtr[i] = forwardmeta.get(keys[i]).get(docid);
            i++;
        }
        return rtr;
    }

    @Override
    public String[][] getItems(String[] Keys, int[] docids) {
        //return array is indexed by document than by key
        String[][] rtr = new String[docids.length][];
        int i=0;
        for(int docid : docids)
        {
            rtr[i] = getItems(Keys, docid);
            i++;
        }
        return rtr;
    }

    @Override
    public int getDocument(String key, String value) {
        return reversemeta.get(key).getOrDefault(value, -1);
    }

    @Override
    public String[] getKeys() {
        return keyNames;
    }
}