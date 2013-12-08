package com.netflix.suro.routing.filter.parser;

import com.google.common.base.Joiner;


public enum FilterPredicate {
	stringComp("xpath(\"%s\") %s \"%s\""), 
	numberComp("xpath(\"%s\") %s %s"),
	between("xpath(\"%s\") between (%s, %s)") {
		public String create(String path, String operator, Object value) {
			Object[] values = (Object[])value;
			return String.format(getTemplate(), path, values[0], values[1]);
		}
	},
	isNull("xpath(\"%s\") is null") {
		public String create(String path, String operator, Object value) {
			return String.format(getTemplate(), path);
		}
	},
	regex("xpath(\"%s\") =~ \"%s\"") {
		public String create(String path, String operator, Object value) {
			return String.format(getTemplate(), path, value);
		}
	},
	existsRight("xpath(\"%s\") exists") {
		public String create(String path, String operator, Object value) {
			return String.format(getTemplate(), path);
		}
	}, 
	existsLeft("exists xpath(\"%s\")") {
		public String create(String path, String operator, Object value) {
			return String.format(getTemplate(), path);
		}
	}, 
	trueValue("true") {
		public String create(String path, String operator, Object value) {
			return getTemplate();
		} 
	},
	falseValue("false") {
		public String create(String path, String operator, Object value) {
			return getTemplate();
		} 
	}, 
	timeComp("xpath(\"%s\") %s %s"), 
	
	inPred("xpath(\"%s\") in (%s)") {
		public String create(String path, String operator, Object value) {
			Object[] values = (Object[])value;
			
			return String.format(getTemplate(), path, Joiner.on(',').join(values));
		} 
	}
	;
	
	final private String template; 
	private FilterPredicate(String template) {
		this.template = template;
	}
	
	public String create(String path, String operator, Object value) {
		return String.format(template, path, operator, value);
	}
	
	public String getTemplate() {
		return template;
	}
	public String create(String path) {
		return create(path, null);
	}
	
	public String create(String path, Object value) {
		return create(path, null, value);
	}
	
	public String create(String path, String operator){
		return create(path, operator, null);
	}
	
	public String create() {
		return create(null, null);
	}
}
