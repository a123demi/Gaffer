/*
 * Copyright 2016-2017 Crown Copyright
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
import uk.gov.gchq.gaffer.graph.hook.OperationAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.OperationChainLimiter;
import uk.gov.gchq.gaffer.rest.GraphConfig;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A <code>GraphFactory</code> creates instances of {@link uk.gov.gchq.gaffer.graph.Graph} to be reused for all queries.
 */
public interface GraphFactory {
    static GraphFactory createGraphFactory() {
        final String graphFactoryClass = System.getProperty(SystemProperty.GRAPH_FACTORY_CLASS,
                SystemProperty.GRAPH_FACTORY_CLASS_DEFAULT);

        try {
            return Class.forName(graphFactoryClass)
                    .asSubclass(GraphFactory.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to create graph factory from class: " + graphFactoryClass, e);
        }
    }

    default Map<String, GraphConfig> loadGraphConfigs() {
        final Map<String, GraphConfig> graphs = new HashMap<>();
        for (final Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            if (entry.getKey().toString().startsWith(SystemProperty.GRAPH_PREFIX)) {
                final String key = entry.getKey().toString().replace(SystemProperty.GRAPH_PREFIX, "");
                if (key.endsWith(SystemProperty.GRAPH_ID_SUFFIX)) {
                    final String graphName = key.replace(SystemProperty.GRAPH_ID_SUFFIX, "");
                    final GraphConfig config = graphs.computeIfAbsent(graphName, k -> new GraphConfig());
                    if (null != config.getGraphId()) {
                        throw new IllegalArgumentException("GraphId for " + graphName + " has already already been defined");
                    }
                    config.setGraphId(entry.getValue().toString());
                } else if (key.endsWith(SystemProperty.GRAPH_SCHEMA_SUFFIX)) {
                    final String graphName = key.replace(SystemProperty.GRAPH_SCHEMA_SUFFIX, "");
                    final GraphConfig config = graphs.computeIfAbsent(graphName, k -> new GraphConfig());
                    if (null != config.getSchema()) {
                        throw new IllegalArgumentException("Schema for " + graphName + " has already already been defined");
                    }
                    config.setSchemaPath(entry.getValue().toString());
                } else if (key.endsWith(SystemProperty.GRAPH_PROPERTIES_SUFFIX)) {
                    final String graphName = key.replace(SystemProperty.GRAPH_PROPERTIES_SUFFIX, "");
                    final GraphConfig config = graphs.computeIfAbsent(graphName, k -> new GraphConfig());
                    if (null != config.getProperties()) {
                        throw new IllegalArgumentException("Properties for " + graphName + " has already already been defined");
                    }
                    config.setPropertiesPath(entry.getValue().toString());
                } else {
                    throw new IllegalArgumentException("Invalid system property: " + entry.getKey());
                }
            }
        }
        return graphs;
    }

    default void addGraphs(final Map<String, GraphConfig> graphParts) {
        for (final Map.Entry<String, GraphConfig> entry : graphParts.entrySet()) {
            addGraph(entry.getKey(), entry.getValue());
        }
    }

    default void addGraph(final String graphName, final GraphConfig graphConfig) {
        final String parentGraph = graphConfig.getParentGraph();
        if (null != parentGraph) {
            graphConfig.setParentGraph(null);

            if (null != getGraph(parentGraph) && null != getGraph(parentGraph).getSchema()) {
                if (null == graphConfig.getSchema()) {
                    graphConfig.setSchema(getGraph(parentGraph).getSchema());
                } else {
                    graphConfig.setSchema(new Schema.Builder()
                            .merge(getGraph(parentGraph).getSchema())
                            .merge(graphConfig.getSchema())
                            .build());
                }
            }

            if (null != getGraphConfig(parentGraph) && null != getGraphConfig(parentGraph).getProperties()) {
                final Properties properties = new Properties(getGraphConfig(parentGraph).getProperties());
                if (null != graphConfig.getProperties()) {
                    properties.putAll(graphConfig.getProperties());
                }
                graphConfig.setProperties(properties);
            }
        }

        addGraphConfig(graphName, graphConfig);
        addGraph(graphName, createGraph(graphConfig.getGraphId(), graphConfig.getSchema(), StoreProperties.loadStoreProperties(graphConfig.getProperties())));
    }

    void addGraph(final String graphName, final Graph graph);

    Graph getGraph(final String graphName);

    void addGraphConfig(final String graphName, final GraphConfig graphConfig);

    GraphConfig getGraphConfig(final String graphName);

    Collection<Graph> getGraphs();

    Collection<String> getGraphNames();

    default Graph createGraph(final String graphId, final Schema schema, final StoreProperties properties) {
        return createGraphBuilder(graphId, schema, properties).build();
    }

    default Graph.Builder createGraphBuilder(final String graphId, final Schema schema, final StoreProperties properties) {
        return new Graph.Builder()
                .graphId(graphId)
                .addSchema(schema)
                .storeProperties(properties)
                .addHook(createOpAuthoriser())
                .addHook(createChainLimiter());
    }

    default OperationAuthoriser createOpAuthoriser() {
        OperationAuthoriser opAuthoriser = null;

        final String opAuthsPathStr = System.getProperty(SystemProperty.OP_AUTHS_PATH);
        if (null != opAuthsPathStr) {
            final Path opAuthsPath = Paths.get(System.getProperty(SystemProperty.OP_AUTHS_PATH));
            if (opAuthsPath.toFile().exists()) {
                opAuthoriser = new OperationAuthoriser(opAuthsPath);
            } else {
                throw new IllegalArgumentException("Could not find operation authorisation properties from path: " + opAuthsPathStr);
            }
        }

        return opAuthoriser;
    }

    default OperationChainLimiter createChainLimiter() {
        final boolean isEnabled = Boolean.parseBoolean(System.getProperty(SystemProperty.ENABLE_CHAIN_LIMITER, "false"));
        if (isEnabled) {
            if (null == System.getProperty(SystemProperty.OPERATION_SCORES_FILE, null)) {
                throw new IllegalArgumentException("Required property has not been set: " + SystemProperty.OPERATION_SCORES_FILE);
            }

            if (null == System.getProperty(SystemProperty.AUTH_SCORES_FILE, null)) {
                throw new IllegalArgumentException("Required property has not been set: " + SystemProperty.AUTH_SCORES_FILE);
            }

            return new OperationChainLimiter(Paths.get(System.getProperty(SystemProperty.OPERATION_SCORES_FILE)), Paths.get(System.getProperty(SystemProperty.AUTH_SCORES_FILE)));
        }

        return null;
    }
}
