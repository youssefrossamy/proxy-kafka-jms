package ma.cdgk.integration.eventNormalizer;

public interface EventNormalizer<T, R> {

    R normalize(T event);
}
