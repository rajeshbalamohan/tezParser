package com.hw.tez.parser;

import java.util.Map;

public interface IParsable<T> {
  void parse(Map<String, String> map);
}
