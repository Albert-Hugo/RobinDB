package com.ido.robin.sstable.extension;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.kryo5.serializers.JavaSerializer;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author Ido
 * @date 2019/3/10 11:08
 */
abstract class CollectionValuePlugin {



    /**
     * 获取集合类型的序列化处理器
     *
     * @return
     */
    protected Serializer getSerializer() {

        CollectionSerializer serializer = new CollectionSerializer();
        serializer.setElementClass(getCollectionClz(), new JavaSerializer());
        serializer.setElementsCanBeNull(false);

        return serializer;
    }

    /**
     * 序列化
     *
     * @param obj   需要序列化的对象
     * @param clazz 集合的具体类型
     * @param <T>
     * @return
     */
    @SuppressWarnings("all")
    final protected <T extends Serializable> String serialization(Object obj, Class<T> clazz) {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(true);

        Serializer serializer = getSerializer();


        kryo.register(clazz, new JavaSerializer());
        kryo.register(getCollectionClz(), serializer);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeObject(output, obj);
        output.flush();
        output.close();

        byte[] b = baos.toByteArray();
        try {
            baos.flush();
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(new Base64().encode(b));
    }

    protected abstract Class getCollectionClz();

    /**
     * 反序列化
     *
     * @param obj   需要反序列化的对象
     * @param clazz 集合中的具体类型
     * @param <T>
     * @return 序列化后的对象
     */
    @SuppressWarnings("all")
    final protected <T extends Serializable> Object deserialization(String obj, Class<T> clazz) {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(true);

        Serializer serializer = getSerializer();
        kryo.register(clazz, new JavaSerializer());
        kryo.register(ArrayList.class, serializer);

        ByteArrayInputStream bais = new ByteArrayInputStream(new Base64().decode(obj));
        Input input = new Input(bais);
        return kryo.readObject(input, getCollectionClz(), serializer);
    }
}
