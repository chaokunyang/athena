package com.timeyang.athena.util.jdbc;

/**
 * @author https://github.com/chaokunyang
 */
public class Criterion {
    private final String field;
    private final Predicate predicate;
    private final Object compareTo;
    private final Condition condition;

    public Criterion(String field, Predicate predicate, Object compareTo) {
        this(field, predicate, compareTo, Condition.AND);
    }

    public Criterion(String field, Predicate predicate,
                  Object compareTo, Condition condition) {
        this.field = field;
        this.predicate = predicate;
        this.compareTo = compareTo;
        this.condition = condition;
    }

    public String getField() {
        return field;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public Object getCompareTo() {
        return compareTo;
    }

    public Condition getCondition() {
        return condition;
    }

    public String toClause() {
        return predicate.toClause(this);
    }

    public enum Predicate {
        LIKE {
            @Override
            public String toClause(Criterion criterion) {
                return String.format(" %s LIKE '%%%s%%' ", criterion.field, getString(criterion));
            }
        },
        NOT_LIKE {
            @Override
            public String toClause(Criterion criterion) {
                return String.format(" %s NOT LIKE '%%%s%%' ", criterion.field, getString(criterion));
            }
        },
        EQ {
            @Override
            public String toClause(Criterion criterion) {
                if (criterion.getCompareTo() instanceof String) {
                    return String.format(" %s = '%s' ",
                            criterion.field, criterion.getCompareTo());
                } else {
                    return String.format(" %s = %s ",
                            criterion.field, criterion.getCompareTo());
                }
            }
        },
        NEQ {
            @Override
            public String toClause(Criterion criterion) {
                if (criterion.getCompareTo() instanceof String) {
                    return String.format(" %s <> '%s' ",
                            criterion.field, criterion.getCompareTo());
                } else {
                    return String.format(" %s <> %s ",
                            criterion.field, criterion.getCompareTo());
                }
            }
        };

        public abstract String toClause(Criterion criterion);

        public static String getString(Criterion f) {
            if (!(f.getCompareTo() instanceof String))
                throw new IllegalArgumentException(f.getField());

            return (String) f.getCompareTo();
        }
    }

    public enum Condition {
        AND {
            @Override
            public String toClause(Criterion criterion) {
                return String.format(" %s %s ", "AND", criterion.toClause());
            }
        },
        OR {
            @Override
            public String toClause(Criterion criterion) {
                return String.format(" %s %s ", "OR", criterion.toClause());
            }
        },
        AND_START {
            @Override
            public String toClause(Criterion criterion) {
                return String.format(" %s %s ( ", "AND", criterion.toClause());
            }
        },
        AND_END {
            @Override
            public String toClause(Criterion criterion) {
                return String.format(" %s %s ) ", "AND", criterion.toClause());
            }
        },
        OR_START {
            @Override
            public String toClause(Criterion criterion) {
                return String.format(" %s %s ( ", "OR", criterion.toClause());
            }
        },
        OR_END {
            @Override
            public String toClause(Criterion criterion) {
                return String.format(" %s %s ) ", "OR", criterion.toClause());
            }
        };

        public abstract String toClause(Criterion criterion);
    }

}
