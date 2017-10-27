package com.github.platformlunar.digdag.plugin.livy;

import com.google.common.base.Optional;

import org.immutables.value.Value;

@Value.Immutable
interface LivyHttpConfig
{
    // HTTP connection essentials
    String host();
    Optional<Integer> port();
    Optional<Boolean> https();
    Optional<String> username();
    Optional<String> password();
    Optional<Integer> connectTimeout();
    Optional<Integer> readTimeout();
    Optional<Integer> writeTimeout();
}
