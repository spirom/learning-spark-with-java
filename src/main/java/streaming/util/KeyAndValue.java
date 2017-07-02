package streaming.util;

/**
 * This utility class complements the CSVFileStreamGenerator providing a way to parse and represent
 * the key value pairs generated in the CSV files
 */
public class KeyAndValue {
  public KeyAndValue(String key, int value) {
    _key = key;
    _value = value;
  }

  /**
   * Parse out of the format found in generated CSV files
   * @param csvLine A comma separated key and integer value
   * @throws IllegalArgumentException
   */
  public KeyAndValue(String csvLine) throws IllegalArgumentException {
    String[] parts = csvLine.split(",");
    if (parts.length != 2) {
      throw new IllegalArgumentException("String is not separated by a single comma: " + csvLine);
    }
    _key = parts[0];
    _value = Integer.valueOf(parts[1]);
  }

  public String getKey() { return _key; }

  public int getValue() { return _value; }

  private String _key;

  private int _value;
}
