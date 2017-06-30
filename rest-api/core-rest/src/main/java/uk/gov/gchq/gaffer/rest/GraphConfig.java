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
package uk.gov.gchq.gaffer.rest;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class GraphConfig {
    private String graphId;
    private Schema schema;
    private Properties properties;
    private String parentGraph;

    public GraphConfig() {
    }

    public GraphConfig(final String graphId, final Schema schema, final Properties properties) {
        this.graphId = graphId;
        this.schema = schema;
        this.properties = properties;
    }

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    public void setSchemaPath(final String schemaPath) {
        if (null == schemaPath) {
            setSchema(null);
        } else {
            final String[] schemaPathsArray = schemaPath.split(",");
            final Path[] paths = new Path[schemaPathsArray.length];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = Paths.get(schemaPathsArray[i]);
            }
            setSchema(Schema.fromJson(paths));
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    public void setPropertiesPath(final String propertiesPath) {
        final Properties props = new Properties();

        InputStream stream = null;
        try {
            if (new File(propertiesPath).exists()) {
                stream = Files.newInputStream(Paths.get(propertiesPath));
            } else {
                stream = StreamUtil.openStream(StoreProperties.class, propertiesPath);
            }
            props.load(stream);
        } catch (IOException e) {
            throw new IllegalArgumentException("Properties could not be loaded from: " + propertiesPath, e);
        } finally {
            CloseableUtil.close(stream);
        }

        setProperties(props);
    }

    public String getParentGraph() {
        return parentGraph;
    }

    public void setParentGraph(final String parentGraph) {
        this.parentGraph = parentGraph;
    }
}
