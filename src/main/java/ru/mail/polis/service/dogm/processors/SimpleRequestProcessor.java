package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.service.dogm.Topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class SimpleRequestProcessor<DataT, RequestT extends ParsedRequest> {
    protected final Logger log = Logger.getLogger(getClass().getName());
    protected final SharedInfo info;
    protected static final long TIMEOUT_CLUSTER = Bridges.TIMEOUT_CONNECT.toMillis() * 3 / 2;

    public SimpleRequestProcessor(@NotNull final SharedInfo sharedInfo) {
        this.info = sharedInfo;
    }

    @Nullable
    protected abstract DataT getDataFromResponse(@NotNull final Response response);

    protected abstract boolean isValid(@Nullable final DataT data,
                                       @NotNull final Response response);

    @NotNull
    protected abstract Response resolveClusterResponse(@NotNull final RequestT request,
                                                       @NotNull final List<DataT> responses);

    @NotNull
    public abstract Response processDirectly(@NotNull final RequestT request);

    @NotNull
    protected abstract List<String> getNodesByRequest(@NotNull final Topology topology,
                                                      @NotNull final RequestT request);

    /**
     * Coordinate processing request in a cluster.
     * @param request a single request
     */
    @NotNull
    public Response processAsCluster(@NotNull final RequestT request) {
        final var successfulResponses = new AtomicInteger(0);
        final var responses = new ArrayList<DataT>(request.fraction.from);
        final var maxNumberOfExceptions = new AtomicInteger(
                request.fraction.from - request.fraction.ack);
        final var result = new CompletableFuture<Integer>();

        final var nodes = getNodesByRequest(info.topology, request);
        for (int i = 0; i < nodes.size(); i++) {
            responses.add(null); // will be replaced in future if succeed

            final var node = nodes.get(i);
            final var index = i;
            (info.topology.isMe(node)
                    ? CompletableFuture.supplyAsync(() -> processDirectly(request))
                    : processRequestRemotely(node, request.raw))
                .thenAccept(
                        response -> {
                            final var data = getDataFromResponse(response);
                            if (isValid(data, response)) {
                                responses.set(index, data); // replace NULL with data
                                final var succeeded = successfulResponses.incrementAndGet();
                                if (succeeded >= request.fraction.ack) {
                                    result.complete(succeeded);
                                }
                            }
                        })
                .exceptionally(e -> futureErrorHandler(e, maxNumberOfExceptions, result));
        }

        try {
            if (result.get(TIMEOUT_CLUSTER, TimeUnit.MILLISECONDS) < request.fraction.ack) {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            } else {
                return resolveClusterResponse(request, responses);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            final var message = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
            return new Response(Response.GATEWAY_TIMEOUT, message.getBytes(UTF_8));
        }
    }

    protected Void futureErrorHandler(@NotNull final Throwable e,
                                      @NotNull final AtomicInteger maxNumberOfExceptions,
                                      @NotNull final CompletableFuture<Integer> result) {
        final var message = e.getMessage() == null
                                ? e.getClass().getName()
                                : e.getMessage();
        log.warning(String.format("%s:\n%s", Protocol.WARN_PROCESSOR, message));
        if (maxNumberOfExceptions.decrementAndGet() < 0) {
            result.completeExceptionally(new IOException("Too many exceptions"));
        }
        return null;
    }

    @NotNull
    protected static Response getWrongProcessorResponse() {
        return new Response(Response.INTERNAL_ERROR, Protocol.FAIL_WRONG_PROCESSOR.getBytes(UTF_8));
    }

    @NotNull
    protected CompletableFuture<Response> processRequestRemotely(@NotNull final String node,
                                                                 @NotNull final Request request) {
        return info.bridges.sendRequestTo(request, node).thenApply(
                response -> new Response(String.valueOf(response.statusCode()), response.body())
        );
    }
}
