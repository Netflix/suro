package com.netflix.suro.queue;

import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.suro.input.SuroInput;

import java.util.ArrayList;
import java.util.List;

@LazySingleton
public class TrafficController {
    private List<SuroInput> inputList = new ArrayList<SuroInput>();

    public void stopTakingTraffic() {
        for (SuroInput input : inputList) {
            input.stopTakingTraffic();
        }
    }

    public void startTakingTraffic() {
        for (SuroInput input : inputList) {
            input.startTakingTraffic();
        }
    }

    public void registerService(SuroInput input) {
        inputList.add(input);
    }

    public void unregisterService(SuroInput input) {
        inputList.remove(input);
    }
}
