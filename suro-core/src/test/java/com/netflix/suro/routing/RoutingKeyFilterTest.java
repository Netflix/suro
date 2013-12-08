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

import com.netflix.suro.message.MessageContainer;
import org.junit.Test;

import static com.netflix.suro.routing.FilterTestUtil.makeMessageContainer;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class RoutingKeyFilterTest {
    @Test
    public void testDoFilter() throws Exception {
        RoutingKeyFilter filter = new RoutingKeyFilter("(?i)key");

        MessageContainer container = makeMessageContainer("routingkEYname", null, null);

        assertTrue(filter.doFilter(container));
    }

    @Test
    public void negativeTest() throws Exception {
        RoutingKeyFilter filter = new RoutingKeyFilter("nomatch");

        MessageContainer container = makeMessageContainer("routingkey", null, null);

        assertFalse(filter.doFilter(container));
    }
}
