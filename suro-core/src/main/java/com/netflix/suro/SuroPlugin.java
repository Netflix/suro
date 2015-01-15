package com.netflix.suro;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.netflix.suro.input.RecordParser;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.routing.Filter;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.notice.Notice;
import com.netflix.suro.sink.remotefile.RemotePrefixFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice based suro plugin with convenience methods for adding pluggable components
 * using Guice's MapBinder.
 * 
 * @author elandau
 *
 */
public abstract class SuroPlugin extends AbstractModule {
    protected static final Logger LOG = LoggerFactory.getLogger(SuroPlugin.class);
    
    /**
     * Add a sink implementation to Suro.  typeName is the expected value of the
     * 'type' field of a JSON configuration.
     * 
     * @param typeName
     * @param sinkClass
     */
    public <T extends Sink> void addSinkType(String typeName, Class<T> sinkClass) {
        LOG.info("Adding sinkType: " + typeName + " -> " + sinkClass.getCanonicalName());
        
        Multibinder<TypeHolder> bindings
            = Multibinder.newSetBinder(binder(), TypeHolder.class);
        bindings.addBinding().toInstance(new TypeHolder(typeName, sinkClass));
    }

    public <T extends SuroInput> void addInputType(String typeName, Class<T> inputClass) {
        LOG.info("Adding inputType: " + typeName + " -> " + inputClass.getCanonicalName());

        Multibinder<TypeHolder> bindings
                = Multibinder.newSetBinder(binder(), TypeHolder.class);
        bindings.addBinding().toInstance(new TypeHolder(typeName, inputClass));
    }

    public <T extends RecordParser> void addRecordParserType(String typeName, Class<T> recordParserClass) {
        LOG.info("Adding recordParser: " + typeName + " -> " + recordParserClass.getCanonicalName());

        Multibinder<TypeHolder> bindings
                = Multibinder.newSetBinder(binder(), TypeHolder.class);
        bindings.addBinding().toInstance(new TypeHolder(typeName, recordParserClass));
    }

    public <T extends RemotePrefixFormatter> void addRemotePrefixFormatterType(String typeName, Class<T> remotePrefixFormatterClass) {
        LOG.info("Adding remotePrefixFormatterType: " + typeName + " -> " + remotePrefixFormatterClass.getCanonicalName());

        Multibinder<TypeHolder> bindings
                = Multibinder.newSetBinder(binder(), TypeHolder.class);
        bindings.addBinding().toInstance(new TypeHolder(typeName, remotePrefixFormatterClass));
    }

    public <T extends Notice> void addNoticeType(String typeName, Class<T> noticeClass) {
        LOG.info("Adding notice: " + typeName + "->" + noticeClass.getCanonicalName());

        Multibinder<TypeHolder> bindings
                = Multibinder.newSetBinder(binder(), TypeHolder.class);
        bindings.addBinding().toInstance(new TypeHolder(typeName, noticeClass));
    }

    public <T extends Filter> void addFilterType(String typeName, Class<T> filterClass) {
        LOG.info("Adding filterType: " + typeName + "->" + filterClass.getCanonicalName());

        Multibinder<TypeHolder> bindings
                = Multibinder.newSetBinder(binder(), TypeHolder.class);
        bindings.addBinding().toInstance(new TypeHolder(typeName, filterClass));
    }
}
