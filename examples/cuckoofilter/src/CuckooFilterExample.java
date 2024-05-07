package com.google.setfilters.examples.cuckoofilter;

import com.google.common.hash.Funnels;
import com.google.setfilters.cuckoofilter.CuckooFilter;
import com.google.setfilters.cuckoofilter.CuckooFilterConfig;
import com.google.setfilters.cuckoofilter.CuckooFilterConfig.Size;
import com.google.setfilters.cuckoofilter.CuckooFilterHashFunctions;
import com.google.setfilters.cuckoofilter.CuckooFilterStrategies;
import com.google.setfilters.cuckoofilter.SerializedCuckooFilterTable;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

public class CuckooFilterExample {

  /**
   * In this example code, we create a new cuckoo filter with 1,000,000 integers and configure the
   * target false positive probability as 0.01.
   */
  public static void simpleExample() {
    // Create a new cuckoo filter with 1,000,000 elements.
    int numElements = 1000000;
    CuckooFilterConfig config = CuckooFilterConfig.newBuilder()
        .setSize(Size.computeEfficientSize(0.01, numElements))
        .setHashFunction(CuckooFilterHashFunctions.MURMUR3_128)
        .setStrategy(CuckooFilterStrategies.SIMPLE_MOD)
        .build();
    CuckooFilter<Integer> cuckooFilter = CuckooFilter.createNew(config, Funnels.integerFunnel());

    // Insert 1,000,000 integers to the empty cuckoo filter.
    HashSet<Integer> elements = new HashSet<>();
    for (int i = 0; i < numElements; i++) {
      elements.add(i);
    }
    for (int element : elements) {
      if (!cuckooFilter.insert(element)) {
        // This should not print.
        System.out.println("Element " + element + " could not be inserted!");
      }
    }

    // Verifies that all inserted elements are in the cuckoo filter, e.g. no false negatives.
    if (hasFalseNegative(cuckooFilter, elements)) {
      System.out.println("False negative in the cuckoo filter!");
    }

    // Computes (approximate) false positive rate. The printed false positive rate should be
    // < 0.01, or approximately equal to it.
    System.out.println("Estimated false positive rate: "
        + computeFalsePositiveRate(cuckooFilter, elements, /* numRuns= */100000));

    // Serialize the cuckoo filter.
    SerializedCuckooFilterTable table = cuckooFilter.serializeTable();
    byte [] rawTableBytes = table.asByteArray();
    System.out.println("Serialized cuckoo filter size in bytes: " + rawTableBytes.length);

    // Deserialize the serialized cuckoo filter.
    SerializedCuckooFilterTable table2 =
        SerializedCuckooFilterTable.createFromByteArray(rawTableBytes);
    // Note that the hash function, strategy, and funnel objects are NOT part of the serialization.
    // The same hash function, strategy, and funnel that were used to create the original cuckoo
    // filter object must be supplied.
    CuckooFilter<Integer> cuckooFilter2 =
        CuckooFilter.createFromSerializedTable(table2, config.hashFunction(), config.strategy(),
            Funnels.integerFunnel());

    // Verify correctness of the deserialized filter.
    // Verifies that all inserted elements are in the cuckoo filter, e.g. no false negatives.
    if (hasFalseNegative(cuckooFilter2, elements)) {
      System.out.println("False negative in the cuckoo filter!");
    }

    // Computes (approximate) false positive rate. The printed false positive rate should be
    // < 0.01, or approximately equal to it.
    System.out.println("Estimated false positive rate of deserialized cuckoo filter: "
        + computeFalsePositiveRate(cuckooFilter2, elements, /* numRuns= */100000));
  }

  // Returns whether the given cuckoo filter has false negatives, with original elements
  // as {@code elements}.
  private static boolean hasFalseNegative(CuckooFilter<Integer> cuckooFilter,
      HashSet<Integer> elements) {
    for (int element : elements) {
      if (!cuckooFilter.contains(element)) {
        return true;
      }
    }
    return false;
  }

  // Computes an estimated false positive rate of the given cuckoo filter by querying
  // random non-member elements {@code numRuns} times.
  private static double computeFalsePositiveRate(CuckooFilter<Integer> cuckooFilter,
      HashSet<Integer> elements, int numRuns) {
    Random random = new Random();
    int falsePositiveCount = 0;
    for (int i = 0; i < numRuns; i++) {
      int randomElement;
      do {
        randomElement = random.nextInt();
      } while (elements.contains(randomElement));
      if (cuckooFilter.contains(randomElement)) {
        falsePositiveCount++;
      }
    }
    return (falsePositiveCount + 0.0) / numRuns;
  }

  public static void main (String[] args) {
    simpleExample();
  }
}
