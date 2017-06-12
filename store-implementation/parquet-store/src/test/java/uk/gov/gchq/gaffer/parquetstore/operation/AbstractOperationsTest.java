/*
 * Copyright 2017. Crown Copyright
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
 * limitations under the License
 */

package uk.gov.gchq.gaffer.parquetstore.operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 *
 */
public abstract class AbstractOperationsTest {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractOperationsTest.class);
    static User USER = new User();
    Graph graph;
    ArrayList<ElementSeed> seedsList;
    View view;

    abstract void setupSeeds();
    abstract void setupView();
    abstract void checkData(CloseableIterable<? extends Element> data);
    abstract void checkGetSeededElementsData(CloseableIterable<? extends Element> data);
    abstract void checkGetFilteredElementsData(CloseableIterable<? extends Element> data);
    abstract void checkGetSeededAndFilteredElementsData(CloseableIterable<? extends Element> data);

    @AfterClass
    public static void cleanUpData() throws IOException {
        LOGGER.info("Cleaning up the data");
        final FileSystem fs = FileSystem.get(new Configuration());
        final ParquetStoreProperties props = (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                StreamUtil.storeProps(AbstractOperationsTest.class));
        deleteFolder(props.getDataDir(), fs);
    }

    private static void deleteFolder(final String path, final FileSystem fs) throws IOException {
        Path dataDir = new Path(path);
        if (fs.exists(dataDir)) {
            fs.delete(dataDir, true);
            while (fs.listStatus(dataDir.getParent()).length == 0) {
                dataDir = dataDir.getParent();
                fs.delete(dataDir, true);
            }
        }
    }

    @Test
    public void getAllElementsTest() throws OperationException {
        LOGGER.info("Starting getAllElementsTest");
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetAllElements.Builder().build(), USER);
        checkData(data);
        data.close();
    }

    @Test
    public void getElementsTest() throws OperationException {
        LOGGER.info("Starting getElementsTest");
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetElements.Builder().build(), USER);
        assertFalse(data.iterator().hasNext());
        data.close();
    }

    @Test
    public void getSeededElementsTest() throws OperationException {
        LOGGER.info("Starting getSeededElementsTest");
        setupSeeds();
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetElements.Builder().input(this.seedsList).build(), USER);
        checkGetSeededElementsData(data);
        data.close();
    }

    @Test
    public void getFilteredElementsTest() throws OperationException {
        LOGGER.info("Starting getFilteredElementsTest");
        setupView();
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetAllElements.Builder().view(this.view).build(), USER);
        checkGetFilteredElementsData(data);
        data.close();
    }

    @Test
    public void getSeededAndFilteredElementsTest() throws OperationException {
        LOGGER.info("Starting getSeededAndFilteredElementsTest");
        setupSeeds();
        setupView();
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetElements.Builder().input(this.seedsList).view(this.view).build(), USER);
        checkGetSeededAndFilteredElementsData(data);
        data.close();
    }

    @Test
    public void getElementsWithPostAggregationFilterTest() throws OperationException {
        LOGGER.info("Starting getElementsWithInvalidViewTest");
        final View view = new View.Builder().edge("BasicEdge",
                new ViewElementDefinition.Builder()
                        .postAggregationFilter(
                                new ElementFilter.Builder()
                                        .select("property2")
                                        .execute(
                                                new IsEqual(2.0))
                                        .build())
                        .build())
                .build();
        try {
            this.graph.execute(new GetElements.Builder().view(view).build(), USER);
            fail();
        } catch (OperationException e) {
            assertEquals("The ParquetStore does not currently support post aggregation filters.", e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void getElementsWithPostTransformFilterTest() throws OperationException {
        LOGGER.info("Starting getElementsWithInvalidViewTest");
        final View view = new View.Builder().edge("BasicEdge",
                new ViewElementDefinition.Builder()
                        .postTransformFilter(
                                new ElementFilter.Builder()
                                        .select("property2")
                                        .execute(
                                                new IsEqual(2.0))
                                        .build())
                        .build())
                .build();
        try {
            this.graph.execute(new GetElements.Builder().view(view).build(), USER);
            fail();
        } catch (OperationException e) {
            assertEquals("The ParquetStore does not currently support post transformation filters.", e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }
}