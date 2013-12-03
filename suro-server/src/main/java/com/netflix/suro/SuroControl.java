/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.suro;

import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * A simple blocking control server that processes user-sent command
 */
public class SuroControl {
    private static final Logger log = LoggerFactory.getLogger(SuroControl.class);

    public void start(int port) throws IOException {
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(port);
            log.info("Suro control service started at port " + port);
        } catch (IOException e) {
            throw new IOException(String.format("Can't start server socket at port %d for Suro's control service: %s", port, e.getMessage()), e);
        }

        try{
            while (true) {
                Socket clientSocket = null;
                try{
                    clientSocket = listen(port, serverSocket);
                    Command cmd = processCommand(clientSocket);
                    if(cmd == Command.EXIT) {
                        return;
                    }
                }finally {
                    closeAndIgnoreError(clientSocket);
                }
            }
        }finally {
            closeAndIgnoreError(serverSocket);
            log.info("Suro's control service exited");
        }
    }

    private Socket listen(int port, ServerSocket serverSocket) throws IOException {
        try {
            return serverSocket.accept();
        }catch(IOException e) {
            throw new IOException(String.format("Error when Suro control was accepting user connection at port %d: %s", port, e.getMessage()), e);
        }
    }

    /**
     * Processes user command. For now it supports only "exit", case insensitive.
     * @param clientSocket The client socket after connection is established between a client and this server
     *
     * @return the last processed command
     */
    private Command processCommand(Socket clientSocket) {
        BufferedReader in = null;
        BufferedWriter out = null;
        try{
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            String s;
            while ((s = in.readLine()) != null) {
                if("exit".equalsIgnoreCase(s.trim())) {
                    respond(out, "ok");

                    return Command.EXIT;
                }else {
                    respond(out, String.format("Unknown command '%s'", s));
                }
            }
        }catch (IOException e) {
            log.warn(String.format("Failed to accept user command at port %d: %s", clientSocket.getPort(), e.getMessage()), e);
        }finally {
            try{
                Closeables.close(in, true);
                Closeables.close(out, true);
            } catch(IOException ignored) {

            }

        }

        return null;
    }

    // Implemented this method for both Socket and ServerSocket because we want to
    // make Suro compilable and runnable under Java 6.
    private static void closeAndIgnoreError(Socket socket) {
        if(socket == null) return;

        try{
            socket.close();
        }catch(IOException e) {
            log.warn(String.format("Failed to close the client socket on port %d: %s. Exception ignored.", socket.getPort(), e.getMessage()), e);
        }
    }

    private static void closeAndIgnoreError(ServerSocket socket) {
        if(socket == null) return;

        try{
            socket.close();
        }catch(IOException e) {
            log.warn(String.format("Failed to close the server socket on port %d: %s. Exception ignored.", socket.getLocalPort(), e.getMessage()), e);
        }
    }

    /**
     * Writes line-based response.
     * @param out the channel used to write back response
     * @param response A string that ends with a new line
     * @throws IOException
     */
    private void respond(BufferedWriter out, String response) throws IOException {
        out.append(response);
        out.append("\n");
        out.flush();
    }

    private static enum Command {
        EXIT("exit")
        ;

        private final String name;
        private Command(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static void main(String[] args) throws Exception {
        SuroControl control = new SuroControl();
        control.start(8080);
    }
}
