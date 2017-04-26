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

package uk.gov.gchq.gaffer.hazelcast.cache;


import org.hamcrest.core.IsCollectionContaining;
import org.junit.*;
import org.junit.rules.ExpectedException;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.util.CacheSystemProperty;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class HazelcastCacheServiceTest {

    private HazelcastCacheService service = new HazelcastCacheService();
    private static final String CACHE_NAME = "test";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void beforeEach() {
        System.clearProperty(CacheSystemProperty.CACHE_CONFIG_FILE);
    }

    @After
    public void afterEach() {
        service.shutdown();
    }

    @Test
    public void shouldThrowAnExceptionWhenConfigFileIsMisConfigured() {
        String madeUpFile = "/made/up/file.xml";
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(madeUpFile);
        System.setProperty(CacheSystemProperty.CACHE_CONFIG_FILE, "/made/up/file.xml");
        service.initialise();
    }

    private void initialiseWithTestConfig() {
        String filePath = new File("src/test/resources/hazelcast.xml").getAbsolutePath();
        System.setProperty(CacheSystemProperty.CACHE_CONFIG_FILE, filePath);
        service.initialise();
    }

    @Test
    public void shouldAllowUserToConfigureCacheUsingConfigFilePath() {

        // given
        initialiseWithTestConfig();

        // when
        ICache<String, Integer> cache = service.getCache(CACHE_NAME);

        // then
        Assert.assertEquals(0, cache.size());
        service.shutdown();
    }

    @Test
    public void shouldReUseCacheIfOneExists() throws CacheOperationException {

        // given
        initialiseWithTestConfig();
        ICache<String, Integer> cache = service.getCache(CACHE_NAME);
        cache.put("key", 1);

        // when
        ICache<String, Integer> sameCache = service.getCache(CACHE_NAME);

        // then
        Assert.assertEquals(1, sameCache.size());
        Assert.assertEquals(new Integer(1), sameCache.get("key"));

        service.shutdown();

    }

    @Test
    public void shouldShareCachesBetweenServices() throws CacheOperationException {

        // given
        initialiseWithTestConfig();
        HazelcastCacheService service1 = new HazelcastCacheService();
        service1.initialise();

        // when
        service1.getCache(CACHE_NAME).put("Test", 2);

        // then
        Assert.assertEquals(1, service.getCache(CACHE_NAME).size());
        Assert.assertEquals(2, service.getCache(CACHE_NAME).get("Test"));

        service1.shutdown();
        service.shutdown();

    }

    @Test
    public void shouldAddEntriesToCache() throws CacheOperationException {
        initialiseWithTestConfig();

        service.putInCache(CACHE_NAME, "test", 1);

        assertEquals((Integer) 1, service.getFromCache(CACHE_NAME, "test"));
    }

    @Test
    public void shouldOnlyUpdateIfInstructed() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test", 1);

        try {
            service.putSafeInCache(CACHE_NAME, "test", 2);
            fail("Expected an exception");
        } catch (CacheOperationException e) {
            assertEquals((Integer) 1, service.getFromCache(CACHE_NAME, "test"));
        }

        service.putInCache(CACHE_NAME,"test", 2);

        assertEquals((Integer) 2, service.getFromCache(CACHE_NAME, "test"));
    }

    @Test
    public void shouldBeAbleToDeleteCacheEntries() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test", 1);

        service.removeFromCache(CACHE_NAME, "test");
        assertEquals(0, service.sizeOfCache(CACHE_NAME));
    }

    @Test
    public void shouldBeAbleToClearCache() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);


        service.clearCache(CACHE_NAME);

        assertEquals(0, service.sizeOfCache(CACHE_NAME));
    }

    @Test
    public void shouldGetAllKeysFromCache() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);

        assertEquals(3, service.sizeOfCache(CACHE_NAME));
        assertThat(service.getAllKeysFromCache(CACHE_NAME), IsCollectionContaining.hasItems("test1", "test2", "test3"));
    }

    @Test
    public void shouldGetAllValues() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);
        service.putInCache(CACHE_NAME, "duplicate", 3);

        assertEquals(4, service.sizeOfCache(CACHE_NAME));
        assertEquals(4, service.getAllValuesFromCache(CACHE_NAME).size());

        assertThat(service.getAllValuesFromCache(CACHE_NAME), IsCollectionContaining.hasItems(1, 2, 3));
    }
}