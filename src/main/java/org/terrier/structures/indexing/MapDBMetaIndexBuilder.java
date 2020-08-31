
package org.terrier.structures.indexing;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.serializer.SerializerCompressionWrapper;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.MapDBMetaIndex;
import org.terrier.utility.ArrayUtils;

public class MapDBMetaIndexBuilder extends MetaIndexBuilder {
    
    DB db;
    String[] keyNames;
    String[] reverseKeyNames;
    IndexOnDisk index;
    String structureName;
    protected Map<String,List<String>> forwardmeta = new HashMap<>();
    boolean[] compress; 
    String[] lastValues;
    boolean[] valuesSorted;

    @Deprecated
    public MapDBMetaIndexBuilder(IndexOnDisk _index, String structureName, String[] _keyNames, int[] lengths, String[] _reverseKeys) throws IOException {
        this(_index, structureName, _keyNames, _reverseKeys);
    }

    @Deprecated
    public MapDBMetaIndexBuilder(IndexOnDisk _index,String[] _keyNames, int[] lengths, String[] _reverseKeys) throws IOException {
        this(_index, "meta", _keyNames, _reverseKeys);
    }


    public MapDBMetaIndexBuilder(IndexOnDisk _index, String structureName, String[] _keyNames, String[] _reverseKeys) throws IOException {
        String dbFilename = MapDBMetaIndex.construct_filename(_index, structureName);
        this.keyNames = _keyNames;
        this.index = _index;
        this.structureName = structureName;
        db = DBMaker.fileDB(dbFilename).make();

        compress = new boolean[keyNames.length];
        lastValues = new String[keyNames.length];
        valuesSorted = new boolean[keyNames.length];
        Arrays.fill(valuesSorted, true);

        int ki=0;
        for(String k : keyNames)
        {
            GroupSerializer<String> ser = Serializer.STRING;
            if (compress[ki])
                ser = new SerializerCompressionWrapper<String>(ser);

            forwardmeta.put(k, db.indexTreeList("forward-" + k, ser).make());
            ki++;
        }
        this.reverseKeyNames = _reverseKeys;
        for(String rk : reverseKeyNames)
        {
            if (! forwardmeta.containsKey(rk))
                throw new IllegalArgumentException(rk + " is a reverse meta key, but not forward meta key");
        }
    }

    protected void makeReverse(String k) {
        Map<String,Integer> revMap = db.hashMap("reverse-" + k)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .create();
        final int numDocs = forwardmeta.get(k).size();
        final List<String> meta = forwardmeta.get(k);
        for(int i=0;i<numDocs;i++)
        {
            revMap.put(meta.get(i), i); //this impl will overwrite duplicate keys
        }
    }

    @Override
    public void close() throws IOException {
        Arrays.asList(reverseKeyNames).parallelStream().forEach(k -> makeReverse(k) );
        db.close();
        index.setIndexProperty("index."+structureName+".key-names", String.join(",", keyNames));		
        index.setIndexProperty("index."+structureName+".reverse-key-names", ArrayUtils.join(this.reverseKeyNames, ","));
        index.setIndexProperty("index."+structureName+".key-compress", ArrayUtils.join(this.compress, ","));
        //one entry for each KEY, not "reverse" key
		index.setIndexProperty("index."+structureName+".value-sorted", ArrayUtils.join(valuesSorted, ","));
        index.addIndexStructure(structureName, MapDBMetaIndex.class.getName(), "org.terrier.structures.IndexOnDisk,java.lang.String", "index,structureName");
        index.addIndexStructureInputStream(structureName, MapDBMetaIndex.InputStream.class.getName(), "org.terrier.structures.IndexOnDisk,java.lang.String", "index,structureName");
        index.flush();		
    }

    @Override
    public void writeDocumentEntry(Map<String, String> data) {
        int i=0;
        for(String k : keyNames)
        {
            String value = data.getOrDefault(k, "");
            forwardmeta.get(k).add(value);
            if (lastValues[i] != null && value.compareTo(lastValues[i]) < 0)
				valuesSorted[i] = false;
            lastValues[i] = value;
            i++;
        }
    }

    @Override
    public void writeDocumentEntry(String[] data) {
        assert data.length == keyNames.length;
        int ki=0;
        for(String k : keyNames)
        {
            forwardmeta.get(k).add(data[ki]);
            ki++;
        }
    }
}