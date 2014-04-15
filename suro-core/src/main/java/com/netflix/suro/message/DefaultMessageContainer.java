package com.netflix.suro.message;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import java.util.List;

public class DefaultMessageContainer implements MessageContainer {
    private final Message message;
    private final ObjectMapper jsonMapper;

    class Item {
        TypeReference<?> tr;
        Object obj;
    }

    private List<Item> cache;

    public DefaultMessageContainer(Message message, ObjectMapper jsonMapper) {
        this.message = message;
        this.jsonMapper = jsonMapper;
    }
    @Override
    public <T> T getEntity(Class<T> clazz) throws Exception {
        if (clazz.equals(byte[].class)){
            return (T)message.getPayload();
        } else if (clazz.equals(String.class)) {
            return (T)new String(message.getPayload());
        } else {
            return getEntity(new TypeReference<T>(){});
        }
    }

    @Override
    public <T> T getEntity(TypeReference<T> typeReference) throws Exception {
        if (cache == null) {
            cache = Lists.newLinkedList();
        }
        for (Item item : cache) {
            if (item.tr.equals(typeReference))
                return (T)item.obj;
        }
        Item item = new Item();
        item.tr = typeReference;
        item.obj = jsonMapper.readValue(message.getPayload(), typeReference);
        cache.add(item);

        return (T)item.obj;
    }

    @Override
    public String getRoutingKey() {
        return message.getRoutingKey();
    }

    @Override
    public Message getMessage() {
        return message;
    }
}
