package com.netflix.suro.server;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.suro.input.InputManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

/**
 * Created by adamschmidt on 18/02/15.
 */
@Singleton
@Path("/suroinputstat")
public class InputStat {
    private final InputManager inputManager;

    @Inject
    public InputStat(InputManager inputManager) {
        this.inputManager = inputManager;
    }

    @GET
    @Produces("text/plain")
    public String get() {
        return inputManager.reportInputStat();
    }
}
