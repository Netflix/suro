package com.netflix.suro.routing.filter;

import com.netflix.suro.routing.filter.lang.InvalidFilterException;
import com.netflix.suro.routing.filter.lang.MessageFilterParser;
import com.netflix.suro.routing.filter.lang.MessageFilterParsingException;
import com.netflix.suro.routing.filter.lang.MessageFilterTranslatable;
import org.antlr.runtime.RecognitionException;

/**
 * A compiler to compile the message filter string to an {@link MessageFilter} for consumption
 *
 */
public class MessageFilterCompiler {

    /**
     * Compiles a filter expressed in infix notation to an {@link MessageFilter} instance.
     *
     * @param filter Filter to compile.
     *
     * @return {@link MessageFilter} instance compiled from the passed filter.
     *
     * @throws InvalidFilterException If the input filter is invalid.
     */
    public static MessageFilter compile(String filter) throws InvalidFilterException {
        MessageFilterParser parser = MessageFilterParser.createParser(filter);
        MessageFilterTranslatable t = parseToTranslatable(parser);
        MessageFilter translate = t.translate();
        if (BaseMessageFilter.class.isAssignableFrom(translate.getClass())) {
            ((BaseMessageFilter) translate).setOriginalDslString(filter);
        }

        return translate;
    }

    private static MessageFilterTranslatable parseToTranslatable(MessageFilterParser parser) {
        try {
            MessageFilterParser.filter_return result = parser.filter();
            return (MessageFilterTranslatable) result.getTree();
        } catch (RecognitionException e) {
            throw new MessageFilterParsingException(e.toString(), e);
        }
    }
}
