/*
 * Copyright 2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.rest.factory;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.rest.GraphConfig;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DefaultGraphFactory implements GraphFactory {
    private static boolean initialised = false;
    private static final Map<String, Graph> GRAPHS = new HashMap<>();
    private static Map<String, GraphConfig> graphConfigs = new HashMap<>();

    public DefaultGraphFactory() {
        // Graph factories should be constructed via the createGraphFactory static method,
        // public constructor is required only by HK2
        if (!initialised) {
            graphConfigs = loadGraphConfigs();
            addGraphs(graphConfigs);
            initialised = true;
        }
    }

    public static void clear() {
        initialised = false;
        GRAPHS.clear();
        graphConfigs.clear();
    }

    @Override
    public void addGraphConfig(final String graphName, final GraphConfig graphConfig) {
        graphConfigs.put(graphName, graphConfig);
    }

    @Override
    public GraphConfig getGraphConfig(final String graphName) {
        return graphConfigs.get(graphName);
    }


    @Override
    public void addGraph(final String graphName, final Graph graph) {
        if (GRAPHS.containsKey(graphName)) {
            throw new IllegalArgumentException("GraphName " + graphName + " already exists");
        }
        GRAPHS.put(graphName, graph);
    }

    @Override
    public Graph getGraph(final String graphName) {
        return GRAPHS.get(graphName);
    }

    @Override
    public Collection<String> getGraphNames() {
        return GRAPHS.keySet();
    }

    @Override
    public Collection<Graph> getGraphs() {
        return GRAPHS.values();
    }
}
