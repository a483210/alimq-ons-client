package com.aliyun.openservices.ons.api.spi;

/**
 * <strong>Thread Safety:</strong> For asynchronous API, sendAsync for example, preHandle and postHandle may run in
 * different threads. Use thread-local specific features with caution.
 *
 * @param <T>
 */
public interface Interceptor<T> {
    /**
     * Execute hook-in logic before executing business processing.
     *
     * <p>
     * <strong>Exception Handling</strong> If this method raised an exception, the exception will be logged and
     * interception chain will proceed to the next one.
     * </p>
     *
     * @param invocationContext Invocation context
     * @param instance client instance that is intercepted
     * @return true if interceptor chain is allowed to proceed; false otherwise.
     * @throws Exception If anything wrong is raised.
     */
    boolean preHandle(InvocationContext invocationContext, T instance) throws Exception;

    /**
     * Execute hook-in logic after business processing. Potential execution result can be acquired through inspecting
     * invocation context.
     *
     * @param invocationContext Invocation context
     * @param instance Client instance being intercepted.
     * @throws Exception If anything wrong raised.
     */
    void postHandle(InvocationContext invocationContext, T instance) throws Exception;
}
