load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

RULES_JVM_EXTERNAL_TAG = "4.2"

RULES_JVM_EXTERNAL_SHA = "cd1a77b7b02e8e008439ca76fd34f5b07aecb8c752961f9640dea15e9e5ba1ca"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

GUAVA_VERSION = "27.1"

ERROR_PRONE_VERSION = "2.14.0"

maven_install(
    artifacts = [
        "com.google.errorprone:error_prone_annotation:%s" % ERROR_PRONE_VERSION,
        "com.google.guava:guava:%s-jre" % GUAVA_VERSION,
        "com.google.truth:truth:1.1",
        "com.google.truth.extensions:truth-java8-extension:1.1.3",
        "junit:junit:4.13",
        "org.mockito:mockito-core:2.28.2",
    ],
    repositories = [
        "https://repo1.maven.org/maven2",
        "https://maven.google.com",
    ],
)
