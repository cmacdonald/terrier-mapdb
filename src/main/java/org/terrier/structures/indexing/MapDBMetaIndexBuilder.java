
package org.terrier.structures.indexing;
import org.terrier.structures.MapDBMetaIndex;
import org.terrier.structures.IndexOnDisk;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.serializer.SerializerCompressionWrapper;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.Arrays;

public class MapDBMetaIndexBuilder extends MetaIndexBuilder {
    
    DB db;
    String[] keyNames;
    String[] reverseKeyNames;
    IndexOnDisk index;
    String structureName;
    protected Map<String,List<String>> forwardmeta = new HashMap<>();

    public MapDBMetaIndexBuilder(IndexOnDisk _index, String structureName, String[] _keyNames, String[] _reverseKeys) throws IOException {
        String dbFilename = MapDBMetaIndex.construct_filename(_index, structureName);
        this.keyNames = _keyNames;
        this.index = _index;
        this.structureName = structureName;
        db = DBMaker.fileDB(dbFilename).make();

        boolean[] compress = new boolean[keyNames.length];

        int ki=0;
        for(String k : keyNames)
        {
            GroupSerializer<String> ser = Serializer.STRING;
            if (compress[ki])
                ser = new SerializerCompressionWrapper<String>(ser);

            forwardmeta.put(k, db.indexTreeList("forward-" + k, ser).open());
            ki++;
        }
        this.reverseKeyNames = _reverseKeys;
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
        index.setIndexProperty("index."+structureName+".reverse-key-names", String.join(",", reverseKeyNames));
        index.flush();		
    }

    @Override
    public void writeDocumentEntry(Map<String, String> data) {
        for(String k : keyNames)
        {
            forwardmeta.get(k).add(data.getOrDefault(k, ""));
        }
    }

    @Override
    public void writeDocumentEntry(String[] data) {
        assert data.length == keyNames.length;
        int ki=0;
        for(String k : keyNames)
        {
            forwardmeta.get(k).add(data[ki]);
        }
    }
}