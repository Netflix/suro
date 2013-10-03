package com.netflix.suro;

/**
 * Holder for a generic subtype to be registered with guice as a Map<String, TypeWrapper>
 * so that we can cross link Guice injection with Jackson injection.
 * 
 * Tried to use TypeLiteral but guice doesn't seem to like mixing TypeLiteral with
 * MapBinder
 * 
 * @author elandau
 *
 */
public class TypeHolder {
    private final Class<?> type;
    private final String name;
    
    public TypeHolder(String name, Class<?> type) {
        this.type = type;
        this.name = name;
    }
    
    public Class<?> getRawType() {
        return type;
    }
    
    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return "TypeHolder [type=" + type + ", name=" + name + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypeHolder other = (TypeHolder) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }
}
