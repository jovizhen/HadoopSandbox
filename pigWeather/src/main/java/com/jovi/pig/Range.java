package com.jovi.pig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.jovi.pig.util.WeatherRangeSpecIGRA2;

public class Range {
  private final int start;
  private final int end;

  public Range(int start, int end) {
    this.start = start;
    this.end = end;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }
  
  public String getSubstring(String line) {
    return line.substring(start - 1, end);
  }
  
  @Override
  public int hashCode() {
    return start * 37 + end;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Range)) {
      return false;
    }
    Range other = (Range) obj;
    return this.start == other.start && this.end == other.end;
  }

  public static List<Range> parse(String rangeSpec)
      throws IllegalArgumentException {
    if (rangeSpec.length() == 0) {
      return Collections.emptyList();
    }
    List<Range> ranges = new ArrayList<Range>();
    String[] specs = rangeSpec.split(",");
    for (String spec : specs) {
      spec = WeatherRangeSpecIGRA2.valueOf(spec.toUpperCase().trim()).spec();
      String[] split = spec.split("-");
      try {
        ranges.add(new Range(Integer.parseInt(split[0]), Integer
            .parseInt(split[1])));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
    return ranges;
  }
}
