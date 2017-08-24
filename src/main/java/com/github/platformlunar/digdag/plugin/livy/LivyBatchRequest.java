package com.github.platformlunar.digdag.plugin.livy;

import com.google.common.base.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.immutables.value.Value;

import java.util.Map;
import java.util.List;

@Value.Immutable
@JsonDeserialize(as = ImmutableLivyBatchRequest.class)
@JsonInclude(Include.NON_EMPTY)
interface LivyBatchRequest
{
    @JsonProperty("file")
    String file();

    @JsonProperty("proxyUser")
    Optional<String> proxyUser();

    @JsonProperty("className")
    Optional<String> className();

    @JsonProperty("args")
    Optional<List<String>> args();

    @JsonProperty("jars")
    Optional<List<String>> jars();

    @JsonProperty("pyFiles")
    Optional<List<String>> pyFiles();

    @JsonProperty("files")
    Optional<List<String>> files();

    @JsonProperty("driverMemory")
    Optional<String> driverMemory();

    @JsonProperty("driverCores")
    Optional<Integer> driverCores();

    @JsonProperty("executorMemory")
    Optional<String> executorMemory();

    @JsonProperty("executorCores")
    Optional<Integer> executorCores();

    @JsonProperty("numExecutors")
    Optional<Integer> numExecutors();

    @JsonProperty("archives")
    Optional<List<String>> archives();

    @JsonProperty("queue")
    Optional<String> queue();

    @JsonProperty("name")
    Optional<String> name();

    @JsonProperty("conf")
    Optional<Map<String, String>> conf();
}
