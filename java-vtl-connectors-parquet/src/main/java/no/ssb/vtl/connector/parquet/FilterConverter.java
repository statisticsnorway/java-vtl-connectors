package no.ssb.vtl.connector.parquet;

import com.google.common.collect.Iterators;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.VTLObject;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;

import java.util.Iterator;
import java.util.function.BiFunction;

/**
 * Filter conversion for parquet.
 */
public class FilterConverter implements BiFunction<DataStructure, FilteringSpecification, FilterPredicate> {

    @Override
    public FilterPredicate apply(DataStructure structure, FilteringSpecification specification) {
        switch (specification.getOperator()) {
            case AND:
                return handleAnd(structure, specification.getOperands().iterator());
            case OR:
                return handleOr(structure, specification.getOperands().iterator());
            case EQ:
                return handleEq(structure, specification);
            case GT:
                return handleGt(structure, specification);
            case LT:
                return handleLt(structure, specification);
            case TRUE:
            default:
                return null;
        }
    }

    private FilterPredicate handleLt(DataStructure structure, FilteringSpecification specification) {
        String columnName = specification.getColumn();
        Class<?> columnType = structure.getTypes().get(columnName);
        VTLObject value = specification.getValue();

        FilterPredicate predicate;
        if (columnType.equals(String.class)) {
            predicate = FilterApi.lt(FilterApi.binaryColumn(columnName), Binary.fromCharSequence((CharSequence) value.get()));
        } else if (columnType.equals(Long.class)) {
            predicate = FilterApi.lt(FilterApi.longColumn(columnName), (Long) value.get());
        } else {
            throw new IllegalArgumentException("unsupported type" + columnType + "(column " + columnName + ")");
        }
        return predicate;
    }

    private FilterPredicate handleGt(DataStructure structure, FilteringSpecification specification) {
        String columnName = specification.getColumn();
        Class<?> columnType = structure.getTypes().get(columnName);
        VTLObject value = specification.getValue();

        FilterPredicate predicate;
        if (columnType.equals(String.class)) {
            predicate = FilterApi.gt(FilterApi.binaryColumn(columnName), Binary.fromCharSequence((CharSequence) value.get()));
        } else if (columnType.equals(Long.class)) {
            predicate = FilterApi.gt(FilterApi.longColumn(columnName), (Long) value.get());
        } else {
            throw new IllegalArgumentException("unsupported type" + columnType + "(column " + columnName + ")");
        }
        return predicate;
    }

    private FilterPredicate handleEq(DataStructure structure, FilteringSpecification specification) {
        String columnName = specification.getColumn();
        Class<?> columnType = structure.getTypes().get(columnName);
        VTLObject value = specification.getValue();

        FilterPredicate predicate;
        if (columnType.equals(String.class)) {
            predicate = FilterApi.eq(FilterApi.binaryColumn(columnName), Binary.fromCharSequence((CharSequence) value.get()));
        } else if (columnType.equals(Long.class)) {
            predicate = FilterApi.eq(FilterApi.longColumn(columnName), (Long) value.get());
        } else {
            throw new IllegalArgumentException("unsupported type" + columnType + "(column " + columnName + ")");
        }
        return predicate;
    }

    private FilterPredicate handleOr(DataStructure structure, Iterator<? extends FilteringSpecification> it) {
        if (it.hasNext()) {
            FilterPredicate left = apply(structure, it.next());
            if (it.hasNext()) {
                FilterPredicate right = apply(structure, it.next());
                return FilterApi.or(left, right);
            } else {
                return left;
            }
        } else {
            throw new IllegalArgumentException("and predicate with only no operand.");
        }
    }

    private FilterPredicate handleAnd(DataStructure structure, Iterator<? extends FilteringSpecification> it) {
        // TODO: Handle better.
        it = Iterators.filter(it, o -> {
            if (o != null) {
                return o.getOperator() != FilteringSpecification.Operator.TRUE;
            } else {
                return true;
            }
        });
        if (it.hasNext()) {
            FilterPredicate left = apply(structure, it.next());
            if (it.hasNext()) {
                FilterPredicate right = handleAnd(structure, it);
                return FilterApi.and(left, right);
            } else {
                return left;
            }
        } else {
            throw new IllegalArgumentException("and predicate with only no operand.");
        }
    }
}
