package com.github.platformlunar.digdag.plugin.livy;

import com.google.common.base.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.immutables.value.Value;

import java.util.Map;
import java.util.List;

@Value.Immutable
@JsonDeserialize(as = ImmutableLivyTaskState.class)
interface LivyTaskState
{
    @JsonProperty("id")
    Integer id();

    @JsonProperty("state")
    String state();

    @JsonProperty("appId")
    Optional<String> appId();

    @JsonProperty("appInfo")
    Map<String, Optional<String>> appInfo();

    @JsonProperty("log")
    List<String> log();
}
