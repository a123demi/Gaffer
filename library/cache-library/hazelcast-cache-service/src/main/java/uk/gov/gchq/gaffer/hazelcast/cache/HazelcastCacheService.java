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


import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.ICacheService;
import static uk.gov.gchq.gaffer.cache.util.CacheSystemProperty.CACHE_CONFIG_FILE;

public class HazelcastCacheService implements ICacheService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastCacheService.class);
    private static final HazelcastInstance HAZELCAST = Hazelcast.newHazelcastInstance();

    @Override
    public void initialise() {
        String configFile = System.getProperty(CACHE_CONFIG_FILE);

        if (configFile == null) {
            LOGGER.warn("Config file not set using system property: " + CACHE_CONFIG_FILE
                    + ". Using default settings");
        } else {

        }

        LOGGER.info(HAZELCAST.getCluster().getClusterState().name()); // bootstraps hazelcast
    }

    @Override
    public void shutdown() {
        HAZELCAST.shutdown();
    }

    @Override
    public <K, V> ICache<K, V> getCache(final String cacheName) {
        IMap<K, V> cache = HAZELCAST.getMap(cacheName);
        return new HazelcastCache<>(cache);
    }
}