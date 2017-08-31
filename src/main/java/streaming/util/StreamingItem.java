package streaming.util;

import java.io.Serializable;
import java.util.Random;

/**
 * This utility class complements the CSVFileStreamGenerator providing a way to parse and represent
 * the key value pairs generated in the CSV files
 */
public class StreamingItem implements Serializable {

  public enum Category { SMALL, MEDIUM, LARGE, HUGE };

  /**
   * Fill in other fields at random
   * @param key
   */
  public StreamingItem(Random random, String key) {
    _key = key;
    _value = random.nextInt();
    int whichCat = random.nextInt(11);
    switch (whichCat) {
      case 0:
      case 1:
        _category = Category.SMALL;
        break;
      case 2:
      case 3:
      case 4:
      case 5:
        _category = Category.MEDIUM;
        break;
      case 6:
      case 7:
      case 8:
        _category = Category.LARGE;
        break;
      default:
        _category = Category.HUGE;
    }
  }


  public StreamingItem(String key, Category category, int value) {
    _key = key;
    _category = category;
    _value = value;
  }

  /**
   * Parse out of the format found in generated CSV files
   * @param csvLine A comma separated key and values
   * @throws IllegalArgumentException
   */
  public StreamingItem(String csvLine) throws IllegalArgumentException {
    String[] parts = csvLine.split(",");
    if (parts.length != 3) {
      throw new IllegalArgumentException("String is not separated by two single commas: " + csvLine);
    }
    _key = parts[0];
    _category = Category.valueOf(parts[1]);
    _value = Integer.valueOf(parts[2]);
  }

  public String getKey() { return _key; }

  public Category getCategory() { return _category; }

  public int getValue() { return _value; }

  public String toString() { return _key + "," + _category + "," + _value; }

  private String _key;

  private Category _category;

  private int _value;
}
