// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.setfilters.cuckoofilter;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class CuckooFilterTest {

  private static class Sha256HashFunction implements CuckooFilterConfig.HashFunction {
    @Override
    public <T> HashCode hash(T element, Funnel<? super T> funnel) {
      return Hashing.sha256().hashObject(element, funnel);
    }
  }

  @Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{new Sha256HashFunction(), true},
        {CuckooFilterHashFunctions.MURMUR3_128, false}});
  }

  @Parameter(0)
  public CuckooFilterConfig.HashFunction hashFunction;
  @Parameter(1)
  public boolean useSpaceOptimization;

  private CuckooFilterConfig config;
  private CuckooFilter<Integer> cuckooFilter;

  @Before
  public void setUp() {
    config =
        CuckooFilterConfig.newBuilder()
            .setSize(
                CuckooFilterConfig.Size.newBuilder()
                    .setBucketCount(100)
                    .setBucketCapacity(4)
                    .setFingerprintLength(16)
                    .build())
            .setHashFunction(hashFunction)
            .setStrategy(CuckooFilterStrategies.SIMPLE_MOD)
            .setUseSpaceOptimization(useSpaceOptimization)
            .build();
    cuckooFilter = CuckooFilter.createNew(config, Funnels.integerFunnel());
  }

  @Test
  public void insertAndContains() {
    final int insertedElementsCount = 380;

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(cuckooFilter.insert(i)).isTrue();
    }

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(cuckooFilter.contains(i)).isTrue();
    }

    final int testCountNonExistentElements = 300;

    for (int i = 0; i < testCountNonExistentElements; i++) {
      assertThat(cuckooFilter.contains(i + insertedElementsCount)).isFalse();
    }
  }

  @Test
  public void insert_failsWhenFull_insertSameElements() {
    // Exhaust two buckets that element 0 can belong to.
    for (int i = 0; i < 2 * config.size().bucketCapacity(); i++) {
      assertThat(cuckooFilter.insert(0)).isTrue();
    }

    assertThat(cuckooFilter.insert(0)).isFalse();
  }

  @Test
  public void insert_insertFailureReversesTheReplacements() {
    int insertedCount = 0;
    while (true) {
      if (!cuckooFilter.insert(insertedCount)) {
        break;
      }
      insertedCount++;
    }

    for (int i = 0; i < insertedCount; i++) {
      assertThat(cuckooFilter.contains(i)).isTrue();
    }
    assertThat(cuckooFilter.contains(insertedCount)).isFalse();
  }

  @Test
  public void delete_deletesExistingElements() {
    final int insertedElementsCount = 150;

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(cuckooFilter.insert(i)).isTrue();
      assertThat(cuckooFilter.insert(i)).isTrue();
    }

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(cuckooFilter.delete(i)).isTrue();
      assertThat(cuckooFilter.delete(i)).isTrue();
    }
  }

  @Test
  public void delete_deletingNonExistingElementsFails() {
    final int insertedElementsCount = 150;

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(cuckooFilter.delete(i)).isFalse();
    }
  }

  @Test
  public void size() {
    assertThat(cuckooFilter.size()).isEqualTo(config.size());
  }

  @Test
  public void count() {
    final int insertedElementsCount = 300;
    final int deletedElementCount = 150;

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(cuckooFilter.insert(i)).isTrue();
    }
    assertThat(cuckooFilter.count()).isEqualTo(insertedElementsCount);

    for (int i = 0; i < deletedElementCount; i++) {
      assertThat(cuckooFilter.delete(i)).isTrue();
    }
    assertThat(cuckooFilter.count()).isEqualTo(insertedElementsCount - deletedElementCount);

    // Attempt to delete non existing elements.
    for (int i = 0; i < deletedElementCount; i++) {
      assertThat(cuckooFilter.delete(insertedElementsCount + i)).isFalse();
    }
    assertThat(cuckooFilter.count()).isEqualTo(insertedElementsCount - deletedElementCount);
  }

  @Test
  public void serializeAndDeserialize() {
    final int insertedElementsCount = 300;

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(cuckooFilter.insert(i)).isTrue();
    }

    SerializedCuckooFilterTable serializedTable = cuckooFilter.serializeTable();

    CuckooFilter<Integer> anotherCuckooFilter =
        CuckooFilter.createFromSerializedTable(
            serializedTable, config.hashFunction(), config.strategy(), Funnels.integerFunnel());

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(anotherCuckooFilter.contains(i)).isTrue();
    }
    assertThat(anotherCuckooFilter.contains(insertedElementsCount)).isFalse();
  }
}
