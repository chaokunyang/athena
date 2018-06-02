package com.timeyang.athena.utill.jdbc;

import java.util.List;
import java.util.stream.Collectors;

/**
 * page starts from 0
 *
 * @author https://github.com/chaokunyang
 */
public class Page {
    private int page;
    private int size;
    private List<Sort> sorts;
    private List<Criterion> criteria;

    public Page() { }

    public Page(int page, int size) {
        this(page, size, null, null);
    }

    public Page(int page, int size, List<Sort> sorts) {
        this(page, size, sorts, null);
    }

    public Page(int page, int size, List<Sort> sorts, List<Criterion> criteria) {
        this.page = page;
        this.size = size;
        this.sorts = sorts;
        this.criteria = criteria;

        checkFilters(criteria);
    }

    private void checkFilters(List<Criterion> filters) {
        if (filters != null) {
            if (filters.size() > 0) {
            }
        }
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String buildOrderByClause() {
        String clause = "";
        if (sorts != null) {
            String orderBy = sorts.stream()
                    .map(sort -> sort.getField() + " " + sort.getOrder().toString())
                    .collect(Collectors.joining(", "));
            orderBy = " ORDER BY " + orderBy;
            clause = orderBy;
        }

        return clause;
    }

    public String buildCriterionClause() {
        StringBuilder filterBuilder = new StringBuilder(" ");
        if (criteria != null) {
            int length = criteria.size();
            for (int i = 0; i < length; i++) {
                Criterion criterion = criteria.get(i);
                if (i == 0) { // ignore first criterion's condition
                    filterBuilder.append(criterion.toClause());
                } else {
                    filterBuilder.append(criterion.getCondition().toClause(criterion));
                }
            }
        }
        filterBuilder.append(" ");

        return filterBuilder.toString();
    }

    public static class Sort {
        private String field;
        private Order order;

        public Sort(String field, Order order) {
            this.field = field;
            this.order = order;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public Order getOrder() {
            return order;
        }

        public void setOrder(Order order) {
            this.order = order;
        }
    }

    public enum Order {
        ASC, DESC
    }

}
