/*
 * Copyright 2016 Crown Copyright
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

package gaffer.store.operation.handler;

import gaffer.operation.OperationException;
import gaffer.operation.impl.cache.FetchCache;
import gaffer.store.Context;
import gaffer.store.Store;
import java.util.Map;

/**
 * An <code>FetchCacheHandler</code> handles {@link FetchCache} operations.
 * Simply returns the cache.
 */
public class FetchCacheHandler implements OperationHandler<FetchCache, Map<String, Iterable<?>>> {
    @Override
    public Map<String, Iterable<?>> doOperation(final FetchCache fetchCache,
                                                final Context context, final Store store)
            throws OperationException {
        return context.getCache();
    }
}