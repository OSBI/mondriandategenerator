package bi.meteorite;

import org.olap4j.impl.ArrayMap;

import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;
import java.util.regex.PatternSyntaxException;

import mondrian.i18n.LocalizingDynamicSchemaProcessor;
import mondrian.olap.Util;

import static bi.meteorite.SchemaManipulation.*;

/**
 * Hello world!
 */
public class MondrianDateDimension extends LocalizingDynamicSchemaProcessor {
  public String filter(String schemaUrl, Util.PropertyList connectInfo, InputStream stream) throws Exception {

    String schema = super.filter(schemaUrl, connectInfo, stream);

    try {


      if (connectInfo.get("StartDate") == null || connectInfo.get("EndDate") == null || connectInfo.get("Cubes") ==
                                                                                        null) {
        throw new Exception("Empty Start or End Date");
      }

      String[] kv = connectInfo.get("cubes").split(",");

      String table;
      String initid = connectInfo.get("InitID");
      if (initid != null) {
        table = createTable(connectInfo.get("StartDate"), connectInfo.get("EndDate"), initid);
      } else {
        table = createTable(connectInfo.get("StartDate"), connectInfo.get("EndDate"), null);
      }

      schema = insertPhysTable(table).apply(schema);

      schema = insertSharedDimension("<Dimension name='Auto Date' table='gendate' key='ID'>\n"
                                     + "<Attributes>\n"
                                     + "<Attribute name='Year' keyColumn='year' hasHierarchy='false'/>\n"
                                     + "<Attribute name='Quarter' hasHierarchy='false'>\n"
                                     + "<Key> <Column name='year'/><Column name='qtr'/></Key>\n"
                                     + "<Name><Column name='qtr'/></Name>\n"
                                     + "</Attribute>\n"
                                     + "<Attribute name='Month' hasHierarchy='false'>\n"
                                     + "<Key> <Column name='year'/><Column name='month'/></Key>\n"
                                     + "<Name><Column name='month'/></Name>\n"
                                     + "</Attribute>\n"
                                     + "<Attribute name='Day' hasHierarchy='false'>\n"
                                     + "<Key> <Column name='id'/></Key>\n"
                                     + "<Name>\n"
                                     + "<Column name='day'/>\n"
                                     + "</Name>"
                                     + "<OrderBy>\n"
                                     + "<Column name='id'/>\n"
                                     + "</OrderBy>"
                                     + "</Attribute>\n"
                                     + "<Attribute name='ID' keyColumn='id' hasHierarchy='false'/>\n"
                                     + "</Attributes>\n"
                                     + "<Hierarchies>\n"
                                     + "<Hierarchy name='Dates' hasAll='true'>\n"
                                     + "<Level attribute='Year'/>\n"
                                     + "<Level attribute='Quarter'/>\n"
                                     + "<Level attribute='Month'/>\n"
                                     + "<Level attribute='Day'/>\n"
                                     + "</Hierarchy>\n"
                                     + "</Hierarchies>\n"
                                     + "</Dimension>").apply(schema);

      for (String s : kv) {
        String[] detail = s.split("=");
        schema = insertDimension(detail[0], "<Dimension source='Auto Date'/>").apply(schema);
        schema = insertDimensionLinks(detail[0],
            ArrayMap.of(detail[0], "<ForeignKeyLink dimension='Auto Date' foreignKeyColumn='" + detail[1] + "'/>"))
            .apply
                (schema);
      }

    } catch (PatternSyntaxException pse) {
      pse.printStackTrace();
    }

    return schema;
  }

  private String createTable(String start, String end, String initid) {

    String t = "Numeric";
    if (initid == null) {
      t = "Date";
    }
    return "<InlineTable alias=\"gendate\">\n"
           + "    <ColumnDefs>\n"
           + "    <ColumnDef name=\"id\" type=\"" + t + "\"/>\n"
           + "    <ColumnDef name=\"year\" type=\"String\"/>\n"
           + "    <ColumnDef name=\"qtr\" type=\"String\"/>\n"
           + "    <ColumnDef name=\"month\" type=\"String\"/>\n"
           + "    <ColumnDef name=\"month name\" type=\"String\"/>\n"
           + "    <ColumnDef name=\"day\" type=\"String\"/>\n"
           + "    </ColumnDefs>\n"
           + "    <Rows>\n"

           + createRow(start, end, initid)
           + "    </Rows>\n"
           + "    </InlineTable>";
  }

  private String createRow(String start, String end, String initid) {

    SimpleDateFormat myFormat = new SimpleDateFormat("yyyyMMdd");
    Date d1 = null;
    Date d2 = null;
    try {
      d1 = myFormat.parse(start);
      d2 = myFormat.parse(end);
    } catch (ParseException e1) {
      e1.printStackTrace();
    }
    long diff = 0;
    if (d2 != null && d1 != null) {
      diff = d2.getTime() - d1.getTime();
    }
    long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
    String r = "";
    for (int i = 0; i < days; i++) {
      Calendar c = new GregorianCalendar();
      int y = Integer.parseInt(start.substring(0, 4));
      int m2 = Integer.parseInt(start.substring(5, 6)) - 1;
      int d = Integer.parseInt(start.substring(7, 8));
      c.set(y, m2, d);
      c.add(Calendar.DAY_OF_YEAR, i);
      String q = "Q" + Integer.toString(c.get(Calendar.MONTH) / 3 + 1);

      int m = c.get(Calendar.MONTH) + 1;

      String monthString;
      switch (m) {
      case 1:
        monthString = "January";
        break;
      case 2:
        monthString = "February";
        break;
      case 3:
        monthString = "March";
        break;
      case 4:
        monthString = "April";
        break;
      case 5:
        monthString = "May";
        break;
      case 6:
        monthString = "June";
        break;
      case 7:
        monthString = "July";
        break;
      case 8:
        monthString = "August";
        break;
      case 9:
        monthString = "September";
        break;
      case 10:
        monthString = "October";
        break;
      case 11:
        monthString = "November";
        break;
      case 12:
        monthString = "December";
        break;
      default:
        monthString = "Invalid month";
        break;
      }
      String key = "";
      if (initid != null) {
        key = Integer.toString(Integer.parseInt(initid) + i);
      } else {
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");

        key = format1.format(c.getTime());
      }
      r += "    <Row>\n";
      r += "    <Value column=\"id\">" + key + "</Value>\n";
      r += "    <Value column=\"year\">" + Integer.toString(c.get(Calendar.YEAR)) + "</Value>\n";
      r += "    <Value column=\"qtr\">" + q + "</Value>\n";
      r += "    <Value column=\"month\">" + Integer.toString(c.get(Calendar.MONTH) + 1) + "</Value>\n";
      r += "    <Value column=\"month name\">" + monthString + "</Value>\n";
      r += "    <Value column=\"day\">" + Integer.toString(c.get(Calendar.DAY_OF_MONTH)) + "</Value>\n";
      r += "    </Row>\n";

    }
    return r;

  }
}
