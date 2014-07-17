package com.netflix.suro.input;

import com.google.common.collect.Sets;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Singleton
public class InputManager {
    private static final Logger log = LoggerFactory.getLogger(InputManager.class);

    private ConcurrentMap<String, SuroInput> inputMap = new ConcurrentHashMap<String, SuroInput>();

    public void initialStart() throws Exception {
        for (SuroInput suroInput : inputMap.values()) {
            suroInput.start();
        }
    }

    public void initialSet(List<SuroInput> inputList) {
        if (!inputMap.isEmpty()) {
            throw new RuntimeException("inputMap is not empty");
        }

        for (SuroInput suroInput : inputList) {
            inputMap.put(suroInput.getId(), suroInput);
        }
    }

    public void set(List<SuroInput> inputList) {
        for (SuroInput suroInput : inputList) {
            if (!inputMap.containsKey(suroInput.getId())) {
                try {
                    suroInput.start();
                    inputMap.put(suroInput.getId(), suroInput);
                } catch (Exception e) {
                    log.error("Exception on starting the input", e);
                }
            }
        }

        HashSet<SuroInput> suroInputIdSet = Sets.newHashSet(inputList);

        for (Map.Entry<String, SuroInput> e : inputMap.entrySet()) {
            if (!suroInputIdSet.contains(e.getValue())) {
                SuroInput input = inputMap.remove(e.getKey());
                input.shutdown();
            }
        }
    }

    public SuroInput getInput(String id) {
        return inputMap.get(id);
    }

    public void shutdown() {
        for (SuroInput input : inputMap.values()) {
            input.shutdown();
        }
    }
}
