// Copyright 2024 Google LLC
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
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class CuckooFilterLargeTest {

  private static class GoodFastHashFunction implements CuckooFilterConfig.HashFunction {

    @Override
    public <T> HashCode hash(T element, Funnel<? super T> funnel) {
      return Hashing.goodFastHash(128).hashObject(element, funnel);
    }
  }

  @Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{new GoodFastHashFunction(), false},
        {CuckooFilterHashFunctions.MURMUR3_128, true}});
  }

  @Parameter(0)
  public CuckooFilterConfig.HashFunction hashFunction;

  @Parameter(1)
  public boolean useSpaceOptimization;

  @Test
  public void serializeAndDeserialize() {
    final int insertedElementsCount = 100000000;
    final double targetFalsePositiveRate = 0.001;

    CuckooFilterConfig config =
        CuckooFilterConfig.newBuilder()
            .setSize(CuckooFilterConfig.Size.computeEfficientSize(
                targetFalsePositiveRate, insertedElementsCount))
            .setHashFunction(hashFunction)
            .setStrategy(CuckooFilterStrategies.SIMPLE_MOD)
            .setUseSpaceOptimization(useSpaceOptimization)
            .build();

    CuckooFilter<Long> cuckooFilter = CuckooFilter.createNew(config, Funnels.longFunnel());

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(cuckooFilter.insert((long)i)).isTrue();
    }

    SerializedCuckooFilterTable serializedTable = cuckooFilter.serializeTable();

    CuckooFilter<Long> anotherCuckooFilter =
        CuckooFilter.createFromSerializedTable(
            serializedTable, config.hashFunction(), config.strategy(), Funnels.longFunnel());

    for (int i = 0; i < insertedElementsCount; i++) {
      assertThat(anotherCuckooFilter.contains((long)i)).isTrue();
    }
    assertThat(anotherCuckooFilter.contains((long)insertedElementsCount)).isFalse();
  }

  @Test
  public void loadIsHigh() {
    Random random = new Random();

    final int[] bucketCounts = {1000, 10000, 100000, 1000000};
    final int[] bucketCapacities = {4, 5, 6, 7, 8};
    final int fingerprintLength = 16;

    for (int bucketCount : bucketCounts) {
      for (int bucketCapacity : bucketCapacities) {
        CuckooFilter<Long> cuckooFilter =
            CuckooFilter.createNew(
                CuckooFilterConfig.newBuilder()
                    .setSize(
                        CuckooFilterConfig.Size.newBuilder()
                            .setBucketCount(bucketCount)
                            .setBucketCapacity(bucketCapacity)
                            .setFingerprintLength(fingerprintLength)
                            .build())
                    .setHashFunction(hashFunction)
                    .setStrategy(CuckooFilterStrategies.SIMPLE_MOD)
                    .setUseSpaceOptimization(useSpaceOptimization)
                    .build(),
                Funnels.longFunnel());

        long element = 0;
        do {
          element = Math.abs(random.nextLong());
        } while (cuckooFilter.insert(element));

        assertThat(cuckooFilter.load()).isAtLeast(0.95);
      }
    }
  }

  @Test
  public void computeEfficientSize_achievesTargetFalsePositiveRateAndCapacity() {
    Random random = new Random();

    final double[] targetFalsePositiveRates = {0.05, 0.01, 0.001};
    final long[] elementsCountUpperBounds = {1, 5, 10, 50, 100, 500, 1000, 5000, 10000};

    for (double targetFalsePositiveRate : targetFalsePositiveRates) {
      for (long elementsCountUpperBound : elementsCountUpperBounds) {
        CuckooFilter<Long> cuckooFilter =
            CuckooFilter.createNew(
                CuckooFilterConfig.newBuilder()
                    .setSize(
                        CuckooFilterConfig.Size.computeEfficientSize(
                            targetFalsePositiveRate, elementsCountUpperBound))
                    .setHashFunction(hashFunction)
                    .setStrategy(CuckooFilterStrategies.SIMPLE_MOD)
                    .setUseSpaceOptimization(useSpaceOptimization)
                    .build(),
                Funnels.longFunnel());

        long element = 0;
        do {
          element = Math.abs(random.nextLong());
        } while (cuckooFilter.insert(element));

        assertThat(computeFalsePositiveRate(cuckooFilter, 2000000))
            .isAtMost(targetFalsePositiveRate);

        if (elementsCountUpperBound < 10) {
          assertThat(cuckooFilter.count()).isAtLeast(
              (int) Math.ceil(0.5 * elementsCountUpperBound));
        } else if (elementsCountUpperBound < 100) {
          assertThat(cuckooFilter.count()).isAtLeast(
              (int) Math.ceil(0.70 * elementsCountUpperBound));
        } else if (elementsCountUpperBound == 100) {
          assertThat(cuckooFilter.count()).isAtLeast(
              (int) Math.ceil(0.95 * elementsCountUpperBound));
        } else {
          assertThat(cuckooFilter.count()).isAtLeast(elementsCountUpperBound);
        }
      }
    }
  }

  @Test
  public void closeToTheoreticalFalsePositiveRate() {
    Random random = new Random();

    final int bucketCount = 1000;
    final int[] bucketCapacities = {2, 3, 4, 5, 6, 7, 8};
    for (int bucketCapacity : bucketCapacities) {
      // Due to time out issue, we only go up to 12 bits (otherwise we have to sample too many times
      // to get a reliable measurement).
      // TODO: Add a separate benchmark to test for longer fingerprint length.
      for (int fingerprintLength = 8; fingerprintLength <= 12; fingerprintLength++) {
        CuckooFilter<Long> cuckooFilter =
            CuckooFilter.createNew(
                CuckooFilterConfig.newBuilder()
                    .setSize(
                        CuckooFilterConfig.Size.newBuilder()
                            .setBucketCount(bucketCount)
                            .setBucketCapacity(bucketCapacity)
                            .setFingerprintLength(fingerprintLength)
                            .build())
                    .setHashFunction(hashFunction)
                    .setStrategy(CuckooFilterStrategies.SIMPLE_MOD)
                    .setUseSpaceOptimization(useSpaceOptimization)
                    .build(),
                Funnels.longFunnel());

        long element = 0;
        do {
          element = Math.abs(random.nextLong());
        } while (cuckooFilter.insert(element));

        // Let f = fingerprintLength. A random element not in the cuckoo filter has 1 / (2^f - 1)
        // probability of matching a random fingerprint, and the probability it matches at least one
        // of the x fingerprints is 1 - (1 - 1 / (2^f - 1))^x which is approximately x / (2^f - 1)
        // when x << 2^f - 1.
        //
        // If X is a random variable denoting number of fingerprints in a randomly chosen two
        // buckets, false positive probability is roughly E[X / (2^f - 1)] = E[X] / (2^f - 1).
        // Let a be the cuckoo filter's load and b be the bucketCapacity. Then E[X] = a * 2b.
        // Thus, theoretical false positive rate is ~ a * 2b / (2^f - 1).
        double load = cuckooFilter.load();
        double theoreticalFalsePositiveRate =
            load * 2 * bucketCapacity / ((1 << fingerprintLength) - 1);

        double relativeDiff =
            Math.abs(computeFalsePositiveRate(cuckooFilter, 2000000) - theoreticalFalsePositiveRate)
                / theoreticalFalsePositiveRate;
        assertThat(relativeDiff).isAtMost(0.04);
      }
    }
  }

  private static double computeFalsePositiveRate(
      CuckooFilter<Long> cuckooFilter, int sampleCount) {
    int falsePositiveCount = 0;
    for (int i = 0; i < sampleCount; i++) {
      if (cuckooFilter.contains((long)(-i - 1))) {
        falsePositiveCount++;
      }
    }
    return (double) falsePositiveCount / sampleCount;
  }
}
