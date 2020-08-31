package org.terrier.structures.seralization;
import org.mapdb.Serializer;
import org.mapdb.serializer.*;
import org.apache.hadoop.io.Writable;
import java.io.*;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import java.util.Comparator;


public class WritableSerializer<A extends Writable> extends GroupSerializerObjectArray<A>{

    WriteableFactory<A> factory;

    public WritableSerializer(WriteableFactory<A> f) {
        this.factory = f;
    }

    @Override
    public void serialize(DataOutput2 out, A value) {
        try{
            DataOutputStream dos = new DataOutputStream((OutputStream)out);        
            value.write(dos);
            dos.flush();
        } catch (IOException ioe) {
            throw new IOError(ioe);
        }
    }

    @Override
    public A deserialize(DataInput2 in, int available) {
        try{
            A newInstance = factory.newInstance();
            DataInputStream dis = new DataInputStream(new DataInput2.DataInputToStream(in));
            newInstance.readFields(dis);            
            return newInstance;
        } catch (IOException ioe) {
            throw new IOError(ioe);
        }
    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        DataInputStream in2 = new DataInputStream(new DataInput2.DataInputToStream(in));
        Object[] rtr = new Object[size];
        for(int i=0;i<size;i++)
        {
            rtr[i] = deserialize(in, -1);
        }
        return rtr;
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        A[] objs = (A[]) vals;
        for(A o : objs)
            serialize(out, o);
    }
    
    
}