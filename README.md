[![Build Status](https://github.com/google/setfilters/workflows/CI/badge.svg?branch=master)](https://github.com/google/setfilters/actions)

# Setfilters Library

This repository contains implementations of a collection of set filter data structures, also commonly referred to as approximate membership query data structures. We will use the pronoun "Setfilters" to refer to the library.

# Adding Setfilters library to the project

Setfilters' Maven group ID is `com.google.setfilters`, and its artifact id is `setfilters`. To add dependency using Maven, add the following lines to your project's `pom.xml`: 

```xml
<dependency>
  <groupId>com.google.setfilters</groupId>
  <artifactId>setfilters</artifactId>
  <version>1.0.0</version>
</dependency>
```

# Supported Data Structures

## Cuckoo Filter
Cuckoo filter is a space efficient, approximate membershp query data structure that supports insertions and deletions. False positives are allowed (e.g. a non-member element may incorrectly be labeled as a member), but false negatives are not. The code for the cuckoo filter is located in [setfilters/src/com/google/setfilters/cuckoofilter/](https://github.com/google/setfilters/tree/master/setfilters/src/com/google/setfilters/cuckoofilter) directory. For example code on how to use the library, please see [examples/cuckoofilter/](https://github.com/google/setfilters/tree/master/examples/cuckoofilter).

# Note

This is not an officially supported Google product.
