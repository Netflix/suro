package com.netflix.suro.input;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.config.DynamicStringProperty;
import com.netflix.governator.annotations.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.List;

public class DynamicPropertyInputConfigurator {
    public static final String INPUT_CONFIG_PROPERTY = "SuroServer.inputConfig";

    private static Logger LOG = LoggerFactory.getLogger(DynamicPropertyInputConfigurator.class);

    private final InputManager inputManager;
    private final ObjectMapper jsonMapper;

    @Configuration(INPUT_CONFIG_PROPERTY)
    private String initialInputConfig;

    @Inject
    public DynamicPropertyInputConfigurator(
            InputManager inputManager,
            ObjectMapper jsonMapper) {
        this.inputManager = inputManager;
        this.jsonMapper = jsonMapper;
    }

    @PostConstruct
    public void init() {
        DynamicStringProperty inputFP = new DynamicStringProperty(INPUT_CONFIG_PROPERTY, initialInputConfig) {
            @Override
            protected void propertyChanged() {
                buildInput(get(), false);
            }
        };

        buildInput(inputFP.get(), true);
    }

    private void buildInput(String inputListStr, boolean initialSet) {
        try {
            List<SuroInput> inputList = jsonMapper.readValue(
                    inputListStr,
                    new TypeReference<List<SuroInput>>() {});
            if (initialSet) {
                inputManager.initialSet(inputList);
            } else {
                inputManager.set(inputList);
            }
        } catch (Exception e) {
            LOG.info("Error reading input config from fast property: "+e.getMessage(), e);
        }
    }
}
