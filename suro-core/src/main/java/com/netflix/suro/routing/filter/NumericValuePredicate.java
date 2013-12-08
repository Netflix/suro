package com.netflix.suro.routing.filter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;

import java.util.Map;
import java.util.Set;

public class NumericValuePredicate implements ValuePredicate<Number> {

	private Number value;
	private Operator op;

	public NumericValuePredicate(Number value, String fnName) {
		
		Preconditions.checkNotNull(value, "A null number doesn't make sense. ");

		this.value = value;
		this.op = Operator.lookup(fnName);
	}

	public Number getValue() {
		return value;
	}
	
	public String getOpName () {
		return this.op.getOpName();
	}
	
	@Override
	public boolean apply(Number input) {
		Preconditions.checkNotNull(input, "A null number doesn't make sense.");
		return op.compare(input, value);
	}

	private static enum Operator {
		GREATER_THAN(">") {
			@Override
			public boolean compare(Number left, Number right) {
				return doComparison(left, right) > 0;
			}
		},
		GREATER_THAN_OR_EQUAL(">=") {
			@Override
			public boolean compare(Number left, Number right) {
				return doComparison(left, right) >= 0;
			}
		},
		LESS_THAN("<") {
			@Override
			public boolean compare(Number left, Number right) {
				return doComparison(left, right) < 0;
			}
		},
		LESS_THAN_OR_EQUAL("<=") {
			@Override
			public boolean compare(Number left, Number right) {
				return doComparison(left, right) <= 0;
			}
		},
		EQUAL("=") {
			@Override
			public boolean compare(Number left, Number right) {
				return doComparison(left, right) == 0;
			}
		},
		NOT_EQUAL("!=") {
			@Override
			public boolean compare(Number left, Number right) {
				return doComparison(left, right) != 0;
			}
		}
		;
		
		private String name;

		private Operator(String name) {
			this.name = name;
		}

		public String getOpName() {
			return this.name;
		}

		public static Operator lookup(String opName) {
			Operator op = LOOKUP_TABLE.get(opName);
			
			if(op == null){
				throw new IllegalArgumentException(
					String.format("The operator '%s' is not supported. Supported operations: %s", 
							opName,
							getOperatorNames()
					));
			}
			
			return op;
		}
		
		public static Set<String> getOperatorNames() {
			return LOOKUP_TABLE.keySet();
		}
		
		private static int doComparison(Number a, Number b) {
			return Doubles.compare(a.doubleValue(), b.doubleValue());
		}

		public abstract boolean compare(Number left, Number right);

		// According to JSR-133's FAQ (http://www.cs.umd.edu/~pugh/java/memoryModel/jsr-133-faq.html#finalRight), 
		// " In addition, the visible values for any other object or array referenced by those final 
		// fields will be at least as up-to-date as the final fields.". Therefore, no need to use sync'd map
		// here as this lookup table will be read-only. 
		private static final Map<String, Operator> LOOKUP_TABLE = Maps.newHashMap();
		static {
			for (Operator op : Operator.values()) {
				LOOKUP_TABLE.put(op.getOpName(), op);
			}
		}
	}

	@Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("NumericValuePredicate [value=");
	    builder.append(value);
	    builder.append(", op=");
	    builder.append(op);
	    builder.append("]");
	    return builder.toString();
    }

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((op == null) ? 0 : op.hashCode());
	    result = prime * result + ((value == null) ? 0 : value.hashCode());
	    return result;
    }

	@Override
    public boolean equals(Object obj) {
	    if (this == obj) {
		    return true;
	    }
	    if (obj == null) {
		    return false;
	    }
	    if (getClass() != obj.getClass()) {
		    return false;
	    }
	    NumericValuePredicate other = (NumericValuePredicate) obj;
	    if (op != other.op) {
		    return false;
	    }
	    if (value == null) {
		    if (other.value != null) {
			    return false;
		    }
	    } else if (!value.equals(other.value)) {
		    return false;
	    }
	    return true;
    }
}
