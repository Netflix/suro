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
package com.netflix.suro.routing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import org.junit.Test;

import static com.netflix.suro.routing.FilterTestUtil.makeMessageContainer;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class XpathFilterTest {
    private ObjectMapper jsonMapper = new ObjectMapper();

    @Test
    public void testXPathFilterWorksWithMap() throws Exception {
        String path = "//foo/bar/value";
        Integer value = 6;
        XPathFilter filter = new XPathFilter(String.format("xpath(\"%s\") > 5", path), new JsonMapConverter());

        assertTrue(filter.doFilter(makeMessageContainer("key", path, value)));
    }

    @Test
    public void testExistFilter() throws Exception {
        XPathFilter filter = new XPathFilter("xpath(\"data.fit.sessionId\") exists", new JsonMapConverter());
        assertTrue(filter.doFilter(new DefaultMessageContainer(new Message(
            "routingKey",
            jsonMapper.writeValueAsBytes(
                new ImmutableMap.Builder<String, Object>()
                    .put("data.fit.sessionId", "abc")
                    .put("f1", "v1")
                    .build())),
            jsonMapper)));

        assertFalse(filter.doFilter(new DefaultMessageContainer(new Message(
            "routingKey",
            jsonMapper.writeValueAsBytes(
                new ImmutableMap.Builder<String, Object>()
                    .put("data.fit.sessionIdABC", "abc")
                    .put("f1", "v1")
                    .build())),
            jsonMapper)));
    }
}
