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

package uk.gov.gchq.gaffer.operation;

import com.google.common.collect.Sets;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.comparison.ElementComparator;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import java.util.Collections;
import java.util.Set;

public interface ElementComparison {
    default Set<Pair<String, String>> _getComparablePair(final ElementPropertyComparator comparator) {
        final Set<Pair<String, String>> properties = Sets.newHashSet();
        if (null == comparator.getComparator()) {
            final Pair<String, String> pair = new Pair<>(comparator.getGroupName(), comparator.getPropertyName());
            properties.add(pair);
        }
        return Collections.unmodifiableSet(properties);
    }

    Set<Pair<String, String>> getComparablePair();

    ElementComparator getComparator();
}
