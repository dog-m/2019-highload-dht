package ru.mail.polis.service.dogm.processors.execjs;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Scriptable;
import ru.mail.polis.service.dogm.processors.SharedInfo;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Request processor for POST method to handle requests on /v0/execjs.
 */
public class ExecJSProcessorPost extends ExecJSProcessor {
    private static final Object JS_CONTEXT_MONITOR = new Object();

    /**
     * Create a new "Execute JavaScript" POST-processor.
     * @param sharedInfo Common information between processors
     */
    public ExecJSProcessorPost(@NotNull final SharedInfo sharedInfo) {
        super(sharedInfo);
    }

    @Nullable
    @Override
    protected String getDataFromResponse(@NotNull final Response response) {
        final var nodeResult = response.getBodyUtf8();
        return nodeResult == null ? "" : nodeResult;
    }

    @Override
    protected boolean isValid(@Nullable final String data,
                              @NotNull final Response response) {
        return response.getStatus() == 200;
    }

    @Override
    @NotNull
    protected Response resolveClusterResponse(@NotNull final ExecJSRequest request,
                                              @NotNull final List<String> responses) {
        // synchronization because of static nature of Mozilla:Rhino
        synchronized (JS_CONTEXT_MONITOR) {
            final var cx = Context.enter();
            try {
                Scriptable scope = cx.initStandardObjects();
                // Execute script to build functions and init global objects
                cx.evaluateString(scope, request.js, "<client>", 1, null);

                final var onReducer = scope.get("onReducer", scope);
                if (!(onReducer instanceof Function)) {
                    return new Response(Response.NOT_ACCEPTABLE,
                            "Required function onReducer not found".getBytes(UTF_8));
                }

                final var clusterResults = responses.toArray();
                final Object[] onReducerArgs = {
                        Context.javaToJS(info.dao, scope),
                        Context.javaToJS(clusterResults, scope)
                };
                final var result = ((Function) onReducer).call(cx, scope, scope, onReducerArgs);
                final var resultString = Context.toString(result);
                // for debug purposes use log.info("JS onReducer: " + resultString);
                return Response.ok(resultString);
            } finally {
                Context.exit();
            }
        }

    }

    @Override
    @NotNull
    public Response processDirectly(@NotNull final ExecJSRequest request) {
        // synchronization because of static nature of Mozilla:Rhino
        synchronized (JS_CONTEXT_MONITOR) {
            final var cx = Context.enter();
            try {
                Scriptable scope = cx.initStandardObjects();
                // Execute script to build functions and init global objects
                cx.evaluateString(scope, request.js, "<client>", 1, null);

                final var onNode = scope.get("onNode", scope);
                if (!(onNode instanceof Function)) {
                    return new Response(Response.NOT_ACCEPTABLE,
                            "Required function onNode not found".getBytes(UTF_8));
                }

                final Object[] onNodeArgs = {
                        Context.javaToJS(info.dao, scope)
                };
                final var result = ((Function) onNode).call(cx, scope, scope, onNodeArgs);
                final var resultString = Context.toString(result);
                // for debug purposes use log.info("JS onNode: " + resultString);
                return Response.ok(resultString);
            } finally {
                Context.exit();
            }
        }
    }
}
