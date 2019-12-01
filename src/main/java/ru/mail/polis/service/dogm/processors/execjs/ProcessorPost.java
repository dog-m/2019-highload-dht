package ru.mail.polis.service.dogm.processors.execjs;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Topology;
import ru.mail.polis.service.dogm.processors.Bridges;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Request processor for POST method to handle requests on /v0/execjs.
 */
public class ProcessorPost extends ExecJSProcessor {
    /**
     * Create a new JS POST-processor.
     * @param dao DAO implementation
     * @param topology cluster topology
     * @param bridges connection to other nodes in cluster
     */
    public ProcessorPost(final RocksDAO dao, final Topology topology, final Bridges bridges) {
        super(dao, topology, bridges);
    }

    @Nullable
    @Override
    protected String getDataFromResponse(@NotNull final Response response) {
        return response.getBodyUtf8();
    }

    @Override
    protected boolean isValid(@Nullable final String data,
                              @NotNull final Response response) {
        return data != null;
    }

    @Override
    protected Response resolveClusterResponse(@NotNull final ExecJSRequest request,
                                              @NotNull final List<String> responses) {
        return null;
    }

    @Override
    public Response processDirectly(@NotNull final ExecJSRequest request) {
        final var cx = Context.enter();
        try {
            Scriptable scope = cx.initStandardObjects();
            // Execute script to build functions and init global objects
            cx.evaluateString(scope, request.js, "<client>", 1, null);

            final var onNode = scope.get("onNode", scope);
            final var onReducer = scope.get("onReducer", scope);
            if (!(onNode instanceof Function && onReducer instanceof Function)) {
                return new Response(Response.NOT_ACCEPTABLE, "Required functions not found".getBytes(UTF_8));
            }

            // Add a global variable "dao" that is a JavaScript reflection of DAO of this node
            ScriptableObject.putProperty(scope, "dao", Context.javaToJS(dao, scope));

            final var clusterResults = new Object[1];
            clusterResults[0] = ((Function) onNode).call(cx, scope, scope, null);
            log.info("JS result: " + Context.toString(clusterResults[0]));

            final Object[] onReducerArgs = { Context.javaToJS(clusterResults, scope) };
            final var result = ((Function) onReducer).call(cx, scope, scope, onReducerArgs);

            return Response.ok(Context.toString(result));
        } finally {
            Context.exit();
        }
    }
}
