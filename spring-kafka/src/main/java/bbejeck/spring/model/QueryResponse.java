package bbejeck.spring.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class QueryResponse<T> {

    private  String errorMessage;
    private  T result;
    private Map<String, Set<String>> executionInfo;

    private String hostType = "NOT SET";

    public QueryResponse() {
    }

    private QueryResponse(String exception, T result) {
        this.errorMessage = exception;
        this.result = result;
    }

    public static <T> QueryResponse<T> withError(String errorMessage) {
         return new QueryResponse<>(errorMessage, null);
    }

    public static <T> QueryResponse<T> withResult(T result) {
        return new QueryResponse<>(null, result);
    }

    public QueryResponse<T> addExecutionInfo(Map<String, Set<String>>executionInfo) {
        this.executionInfo = new HashMap<>(executionInfo);
        return this;
    }

    public QueryResponse<T> setHostType(String hostType) {
        this.hostType = hostType;
        return this;
    }

    public boolean hasError() {
        return errorMessage != null;
    }

    public String getHostType() {
        return hostType;
    }
    public String getErrorMessage() {
        return errorMessage;
    }

    public T getResult() {
        return result;
    }

    public Map<String, Set<String>> getExecutionInfo() {
        return executionInfo;
    }

    @Override
    public String toString() {
        return "QueryResponse{" +
                "errorMessage='" + errorMessage + '\'' +
                ", result=" + result +
                ", executionInfo=" + executionInfo +
                ", hostType='" + hostType + '\'' +
                '}';
    }
}
