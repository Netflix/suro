package com.netflix.suro.message;

public class StringMessage implements MessageContainer {

    private final Message message;
    
    public StringMessage(String routingKey, String body) {
        message = new Message(routingKey, body.getBytes());
    }
    
    public StringMessage(Message message) {
        this.message = message;
    }
    
    @Override
    public String getRoutingKey() {
        return message.getRoutingKey();
    }

    @Override
    public <T> T getEntity(Class<T> clazz) throws Exception {
        if (clazz.equals(byte[].class)) {
            return (T)message.getPayload();
        }
        else if (clazz.equals(String.class)) {
            return (T)new String(message.getPayload());
        }
        else {
            throw new RuntimeException("Message cannot be deserialized to " + clazz.getCanonicalName());
        }
    }

    @Override
    public Message getMessage() {
        return message;
    }

}
