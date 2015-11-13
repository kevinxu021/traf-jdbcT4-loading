package com.esgyn.jdb;

import org.json.JSONString;

public class NullString implements JSONString {

	@Override
	public String toJSONString() {
		return "";
	}

}
