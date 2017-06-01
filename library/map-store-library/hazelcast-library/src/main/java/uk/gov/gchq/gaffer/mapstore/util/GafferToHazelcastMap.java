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

package uk.gov.gchq.gaffer.mapstore.util;

import com.hazelcast.core.IMap;
import uk.gov.gchq.gaffer.mapstore.utils.MapWrapper;

public class GafferToHazelcastMap<K, V> extends MapWrapper<K, V> {
    public GafferToHazelcastMap(final IMap<K, V> map) {
        super(map);
    }

    @Override
    public V put(final K key, final V value) {
        // This is more efficient.
        getMap().set(key, value);
        return null;
    }

    @Override
    public V remove(final Object key) {
        getMap().delete(key);
        return null;
    }

    @Override
    protected IMap<K, V> getMap() {
        return (IMap<K, V>) super.getMap();
    }
}
