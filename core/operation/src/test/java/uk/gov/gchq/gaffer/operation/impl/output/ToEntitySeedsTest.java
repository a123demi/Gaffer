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

package uk.gov.gchq.gaffer.operation.impl.output;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;


public class ToEntitySeedsTest extends OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Override
    public Class<? extends Operation> getOperationClass() {
        return ToEntitySeeds.class;
    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final ToEntitySeeds op = new ToEntitySeeds();

        // When
        byte[] json = serialiser.serialise(op, true);
        final ToEntitySeeds deserialisedOp = serialiser.deserialise(json, ToEntitySeeds.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ToEntitySeeds toEntitySeeds = new ToEntitySeeds.Builder().input("1", "2").build();

        // Then
        assertThat(toEntitySeeds.getInput(), is(notNullValue()));
        assertThat(toEntitySeeds.getInput(), iterableWithSize(2));
        assertThat(toEntitySeeds.getInput(), containsInAnyOrder("1", "2"));
    }
}
