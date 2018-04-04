package com.github.platformlunar.digdag.plugin.livy;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import io.digdag.client.config.Config;
import io.digdag.spi.TaskResult;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.util.BaseOperator;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.standards.operator.DurationInterval;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;


import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;

public class LivyOperator extends BaseOperator
{
    private final TaskState state;

    private static Logger logger = LoggerFactory.getLogger(LivyOperator.class);

    private OkHttpClient httpClient;

    private final Optional<LivyHttpConfig> systemLivyHttpConfig;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.registerModule(new GuavaModule());
    }

    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json");
    private static final String JOB_ID = "jobId";
    private static final String STATE_START = "start";
    private static final String STATE_RUNNING = "running";
    private static final String STATE_CHECK = "check";
    private static final Integer DEFAULT_HTTP_TIMEOUT = 30;

    public LivyOperator(OperatorContext context)
    {
        super(context);
        this.state = TaskState.of(request);
        this.systemLivyHttpConfig = systemLivyHttpConfig(request.getConfig());
    }

    @Override
    public TaskResult runTask()
    {
        SecretProvider secrets = context.getSecrets().getSecrets("livy");

        Config params = request.getConfig().mergeDefault(
                request.getConfig().getNestedOrGetEmpty("livy"));

        // Set Livy HTTP endpoint config
        LivyHttpConfig livyConf = userLivyHttpConfig(secrets, params)
            .or(systemLivyHttpConfig)
            .orNull();

        this.httpClient = new OkHttpClient.Builder()
            .connectTimeout(livyConf.connectTimeout().get(), TimeUnit.SECONDS)
            .writeTimeout(livyConf.writeTimeout().get(), TimeUnit.SECONDS)
            .readTimeout(livyConf.readTimeout().get(), TimeUnit.SECONDS)
            .build();

        // Set Livy batch session request (task) config
        LivyBatchRequest batchSubmission = userBatchRequestConfig(secrets, params);

        String scheme = livyConf.https().isPresent() && livyConf.https().get() ? "https" : "http";
        String userinfo = "";

        if (livyConf.username().isPresent() && livyConf.password().isPresent()) {
            userinfo = livyConf.username().get() + ":" + livyConf.password().get() + "@";
        }

        String address = scheme + "://" + userinfo + livyConf.host() + ":" + livyConf.port().get();

        try {
            return run(address, batchSubmission);
        } catch (Throwable t) {
            boolean retry = t instanceof TaskExecutionException &&
                ((TaskExecutionException) t).getRetryInterval().isPresent();
            throw Throwables.propagate(t);
        }
    }

    private TaskResult run(String livyAddress, LivyBatchRequest submission) throws IOException
    {
        String applicationName = submission.name().or("unknown");

        LivyTaskState submissionState = pollingRetryExecutor(state, STATE_START)
            .withErrorMessage("Livy job submission failed: %s", applicationName)
            .runOnce(LivyTaskState.class, (TaskState state) -> {
                    logger.info("Submitting Livy job: {}", applicationName);

                    try {
                        String requestBody = objectMapper.writeValueAsString(submission);

                        RequestBody jsonRequestBody = RequestBody.create(JSON_MEDIA_TYPE, requestBody);

                        Request submissionRequest = new Request.Builder()
                            .url(livyAddress + "/batches")
                            .post(jsonRequestBody)
                            .build();

                        Response response = httpClient.newCall(submissionRequest).execute();

                        String responseBody = response.body().string();

                        LivyTaskState currentState = objectMapper.readValue(responseBody, ImmutableLivyTaskState.class);

                        logger.info("Successfully submitted Livy application id: {}", currentState.id());

                        return currentState;
                    } catch (JsonProcessingException ex) {
                        throw new TaskExecutionException(ex);
                    } catch (IOException ex) {
                        throw new TaskExecutionException(ex);
                    }
                }
            );

        String logUrl = getLogUrl(livyAddress, submissionState.id());

        LivyTaskState executionState = pollingWaiter(state, STATE_RUNNING)
            .withPollInterval(DurationInterval.of(Duration.ofSeconds(1), Duration.ofSeconds(10)))
            .withWaitMessage("Livy task id %d is still running (%s)", submissionState.id(), logUrl)
            .awaitOnce(LivyTaskState.class, pollState -> checkTaskCompletion(submissionState.id(), livyAddress, pollState));

        logger.info("Livy application id {} ended with status {}", executionState.id(), executionState.state());

        return TaskResult.defaultBuilder(request).build();
    }

    private Optional<LivyTaskState> checkTaskCompletion(Integer jobId, String livyAddress, TaskState pollState) throws IOException
    {
        return pollingRetryExecutor(pollState, STATE_CHECK)
            .withRetryInterval(DurationInterval.of(Duration.ofSeconds(15), Duration.ofSeconds(15)))
            .run(s -> {
                    Request request = new Request.Builder()
                        .url(livyAddress + "/batches/" + jobId)
                        .build();

                    try {
                        Response response = httpClient.newCall(request).execute();
                        String responseBody = response.body().string();
                        ImmutableLivyTaskState currentTaskState = objectMapper.readValue(responseBody, ImmutableLivyTaskState.class);
                        String currentState = currentTaskState.state();

                        if (currentTaskState.appId().isPresent()) {
                            logger.info("Livy task id {} ({}) is currently {}", currentTaskState.id(), currentTaskState.appId().get(), currentTaskState.state());
                        } else {
                            logger.info("Livy task id {} is currently {}", currentTaskState.id(), currentTaskState.state());
                        }

                        switch (currentState) {
                            case "not_started":
                            case "starting":
                            case "recovering":
                            case "idle":
                            case "running":
                            case "busy":
                            case "shutting_down":
                                return Optional.absent();
                            case "success":
                                return Optional.of(currentTaskState);
                            case "error":
                            case "dead":
                                throw new TaskExecutionException("Livy task id " + currentTaskState.id() + " finished with status " + currentState);
                            default:
                                throw new RuntimeException("Unknown Livy task state: " + currentTaskState);
                        }

                    } catch (IOException e) {
                        throw new TaskExecutionException("Livy server is unreachable");
                    }
            });
    }

    private static String getLogUrl(String livyAddress, int taskId) {
        return livyAddress + "/ui/batch/" + taskId + "/log";
    }

    private static Optional<LivyHttpConfig> systemLivyHttpConfig(Config systemConfig)
    {
        Optional<String> host = systemConfig.getOptional("config.livy.host", String.class);
        if (!host.isPresent()) {
            return Optional.absent();
        }

        LivyHttpConfig config = ImmutableLivyHttpConfig.builder()
            .host(host.get())
            .port(systemConfig.get("config.livy.port", int.class, 8998))
            .https(systemConfig.get("config.livy.https", boolean.class, false))
            .username(systemConfig.getOptional("config.livy.username", String.class))
            .password(systemConfig.getOptional("config.livy.password", String.class))
            .connectTimeout(systemConfig.get("config.livy.connect_timeout", int.class, DEFAULT_HTTP_TIMEOUT))
            .readTimeout(systemConfig.get("config.livy.read_timeout", int.class, DEFAULT_HTTP_TIMEOUT))
            .writeTimeout(systemConfig.get("config.livy.write_timeout", int.class, DEFAULT_HTTP_TIMEOUT))
            .build();

        return Optional.of(config);
    }

    private static Optional<LivyHttpConfig> userLivyHttpConfig(SecretProvider secrets, Config params)
    {
        Optional<String> userHost = secrets.getSecretOptional("host").or(params.getOptional("host", String.class));
        if (!userHost.isPresent()) {
            return Optional.absent();
        }

        LivyHttpConfig config = ImmutableLivyHttpConfig.builder()
            .host(userHost.get())
            .port(secrets.getSecretOptional("port").transform(Integer::parseInt).or(params.get("port", int.class)))
            .https(secrets.getSecretOptional("https").transform(Boolean::parseBoolean).or(params.get("https", boolean.class, false)))
            .username(secrets.getSecretOptional("username").or(params.getOptional("username", String.class)))
            .password(secrets.getSecretOptional("password"))
            .connectTimeout(params.get("connect_timeout", int.class, DEFAULT_HTTP_TIMEOUT))
            .readTimeout(params.get("read_timeout", int.class, DEFAULT_HTTP_TIMEOUT))
            .writeTimeout(params.get("write_timeout", int.class, DEFAULT_HTTP_TIMEOUT))
            .build();

        return Optional.of(config);
    }

    private static LivyBatchRequest userBatchRequestConfig(SecretProvider secrets, Config params)
    {
        return ImmutableLivyBatchRequest.builder()
            .file(params.get("file", String.class))
            .proxyUser(params.getOptional("proxy_user", String.class))
            .className(params.getOptional("class_name", String.class))
            .args(params.getListOrEmpty("args", String.class))
            .jars(params.getListOrEmpty("jars", String.class))
            .pyFiles(params.getListOrEmpty("py_files", String.class))
            .files(params.getListOrEmpty("files", String.class))
            .driverMemory(params.getOptional("driver_memory", String.class))
            .driverCores(params.getOptional("driver_cores", Integer.class))
            .executorMemory(params.getOptional("executor_memory", String.class))
            .executorCores(params.getOptional("executor_cores", Integer.class))
            .numExecutors(params.getOptional("num_executors", Integer.class))
            .archives(params.getListOrEmpty("archives", String.class))
            .queue(params.getOptional("queue", String.class))
            .name(params.getOptional("name", String.class))
            .conf(params.getMapOrEmpty("conf", String.class, String.class))
            .build();
    }
}
