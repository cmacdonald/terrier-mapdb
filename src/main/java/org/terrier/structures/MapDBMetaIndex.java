package org.terrier.structures;

import java.io.IOError;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.serializer.SerializerCompressionWrapper;
import org.terrier.utility.ArrayUtils;


/** An implementation of a metaindex that uses MapDB hashmaps and arraylists for serialization */
public class MapDBMetaIndex implements MetaIndex {
   
    public static String FILENAME_SUFFIX = ".mapdb";

    protected String[] keyNames;
    protected String[] revKeyNames;
    protected Map<String,List<String>> forwardmeta = new HashMap<>();
    protected Map<String,Map<String,Integer>> reversemeta = new HashMap<>();
    protected DB db;
    protected Set<String> forward_sorted = new HashSet<String>();

    public static class InputStream implements Iterator<String[]> {
        MetaIndex mi;
        int size;
        int i=0;
        public InputStream(IndexOnDisk index, String structureName) {
            mi = (MetaIndex) index.getIndexStructure(structureName.replace("-inputstream", ""));
            size = mi.size();
        }

        public boolean hasNext() {
            return i < size; 
        }

        public String[] next() {            
            try{
                String[] rtr = mi.getAllItems(i);
                i++;
                return rtr;
            } catch (IOException ioe) {
                throw new IOError(ioe);
            } 
        }
    }

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

        keyNames = ArrayUtils.parseCommaDelimitedString(index.getIndexProperty("index."+structureName+".key-names", ""));
        String[] sCompress = ArrayUtils.parseCommaDelimitedString(index.getIndexProperty("index."+structureName+".key-compress", ""));
        String[] sforward_sorted = ArrayUtils.parseCommaDelimitedString(index.getIndexProperty("index."+structureName+".value-sorted", ""));
        int ki=0;
        
        for(String k : keyNames)
        {
            GroupSerializer<String> ser = Serializer.STRING;
            if (Boolean.parseBoolean(sCompress[ki]))
                ser = new SerializerCompressionWrapper<String>(ser);

            forwardmeta.put(k, db.indexTreeList("forward-" + k, ser).open());
            if (Boolean.parseBoolean(sforward_sorted[ki]))
                forward_sorted.add(k);
            ki++;
        }

        revKeyNames = ArrayUtils.parseCommaDelimitedString(index.getIndexProperty("index."+structureName+".reverse-key-names", ""));
        for(String k : revKeyNames)
        {
            Map<String, Integer> map = db.hashMap("reverse-" + k)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .open();
            reversemeta.put(k, map);
        }
    }

    /** performs a binary search on the metaindex, if they keys happen to be in lexographical order */
	protected int _binarySearch(String key, String value) throws IOException {
		int l = 0, r = this.size() - 1; 
        while (l <= r) { 
            int m = l + (r - l) / 2; 
  
			String found = getItem(key, m);
			// Check if value is present at mid
			int compare = value.compareTo(found);			
            if (compare == 0)
                return m; 
  
            // If x greater, ignore left half 
            if (compare > 0) 
                l = m + 1; 
  
            // If x is smaller, ignore right half 
            else
                r = m - 1; 
        }  
        // if we reach here, then element was 
        // not present 
        return -1; 
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
            rtr[i] = forwardmeta.get(k).get(docid);
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
            rtr[i] = forwardmeta.get(k).get(docid);
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
        Map<String,Integer> reversemap = reversemeta.get(key);
        if (reversemap == null && forward_sorted.contains(key))
            try {
                return _binarySearch(key, value);
            } catch (IOException ioe) {
                throw new IOError(ioe);
            }
        return reversemap.getOrDefault(value, -1);
    }

    @Override
    public String[] getKeys() {
        return keyNames;
    }
}