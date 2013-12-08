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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.suro.message.MessageContainer;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.netflix.suro.routing.FilterTestUtil.makeMessageContainer;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 *
 */
public class XpathFilterTest {
    @Test
    public void testXPathFilterWorksWithMap() throws Exception {
        String path = "//foo/bar/value";
        Integer value = 6;
        XPathFilter filter = new XPathFilter(String.format("xpath(\"%s\") > 5", path), new JsonMapConverter());

        assertTrue(filter.doFilter(makeMessageContainer("key", path, value)));
    }
}
