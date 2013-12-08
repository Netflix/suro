package com.netflix.suro.routing.filter;

/**
 * Base implementation of {@link SerializableMessageFilter} to implement serialization mechanism. <br/>
 * The idea behind serialization is to store the DSL from which this filter was created. This approach works as there is
 * no time where we create this instance without the DSL string. On top of that, creating the DSL from the filter
 * string is too much of work both at runtime and development time.
 *
 * @author Nitesh Kant (nkant@netflix.com)
 */
public abstract class BaseMessageFilter implements SerializableMessageFilter {

    protected String originalDslString;

    @Override
    public String serialize() {
        return originalDslString;
    }

    /**
     * This must be called at the top level MessageFilter and not for any sub-filters under the main filter. <br/>
     * eg: for an event filter, say filter0, which is expressed as (Filter1 AND Filter2), this method is required to
     * only be called for filter0 and not for filter1 and filter2. This value if set for filter1, filter2 will be ignored.
     *
     * @param originalDslString The original DSL string.
     */
    public void setOriginalDslString(String originalDslString) {
        this.originalDslString = originalDslString;
    }
}
