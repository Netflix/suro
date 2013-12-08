package com.netflix.suro.routing.filter.lang;

/**
 * A generic exception representing an invalid filter expression.
 *
 * @author Nitesh Kant (nkant@netflix.com)
 */
public class InvalidFilterException extends Exception {

    private static final long serialVersionUID = -5878696854757828678L;

    private Object filter;

    public InvalidFilterException(String message, Throwable cause, Object filter) {
        super(String.format("Invalid filter %s. Error: %s", filter, message), cause);
        this.filter = filter;
    }

    public InvalidFilterException(Throwable cause, Object filter) {
        super(String.format("Invalid filter %s.", filter), cause);
        this.filter = filter;
    }
}
