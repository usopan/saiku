/*  
 *   Copyright 2012 OSBI Ltd
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.saiku.web.rest.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.saiku.olap.dto.resultset.AbstractBaseCell;
import org.saiku.olap.dto.resultset.CellDataSet;
import org.saiku.olap.dto.resultset.DataCell;
import org.saiku.olap.dto.resultset.MemberCell;
import org.saiku.web.rest.objects.resultset.Cell;
import org.saiku.web.rest.objects.resultset.QueryResult;

public class RestUtil {

    private static double[] colTotals;
    private static String[] colTotalsString;
    private static boolean takeToTotal;
    private static int colId;
    private static String[] rowHeaderString;
    private static boolean[] takeRowHeaderTotal;
    private static Map<String, String> distinctRowTotalsString;
    private static Map<String, Double> distinctRowTotalsDouble;
    private static Vector<String> orderedKeys;
    private static boolean writeHeader;
    public static boolean showRowTotals = false;
    public static boolean showColTotals = false;

    public static QueryResult convert(ResultSet rs) {
        return convert(rs, 0);
    }

    public static QueryResult convert(ResultSet rs, int limit) {

        Integer width = 0;
        Integer height = 0;

        Cell[] header = null;
        ArrayList<Cell[]> rows = new ArrayList<Cell[]>();

        // System.out.println("DATASET");
        try {
            while (rs.next() && (limit == 0 || height < limit)) {
                if (height == 0) {
                    width = rs.getMetaData().getColumnCount();
                    header = new Cell[width];
                    for (int s = 0; s < width; s++) {
                        header[s] = new Cell(rs.getMetaData().getColumnName(s + 1), Cell.Type.COLUMN_HEADER);
                    }
                    if (width > 0) {
                        rows.add(header);
                        // System.out.println(" |");
                    }
                }
                Cell[] row = new Cell[width];
                for (int i = 0; i < width; i++) {
                    String content = rs.getString(i + 1);

                    if (content == null) {
                        content = "";
                    }
                    row[i] = new Cell(content, Cell.Type.DATA_CELL);
                }
                rows.add(row);
                height++;
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return new QueryResult(rows, 0, width, height);
    }

    public static QueryResult convert(CellDataSet cellSet) {
        return convert(cellSet, 0);
    }

    public static QueryResult convert(CellDataSet cellSet, int limit) {
        ArrayList<Cell[]> rows = new ArrayList<Cell[]>();

        if (cellSet == null || cellSet.getCellSetBody() == null || cellSet.getCellSetHeaders() == null) {
            return null;
        }
        AbstractBaseCell[][] body = cellSet.getCellSetBody();
        AbstractBaseCell[][] headers = cellSet.getCellSetHeaders();

        colTotals = new double[body[0].length];
        colTotalsString = new String[body[0].length];
        for (int i = 0; i < colTotals.length; i++) {
            colTotals[i] = 0.0D;
            colTotalsString[i] = null;
        }
        takeToTotal = false;

        int len = body[0].length;
        rowHeaderString = new String[len];
        takeRowHeaderTotal = new boolean[len];
        for (int i = 0; i < len; i++) {
            takeRowHeaderTotal[i] = false;
            rowHeaderString[i] = null;
        }

        AbstractBaseCell[] lastHeadRow = headers[(headers.length - 1)];
        for (int i = 0; i < lastHeadRow.length; i++) {
            AbstractBaseCell acell = lastHeadRow[i];
            if ((acell == null)
                    || (!(acell instanceof MemberCell))) {
                continue;
            }
            MemberCell mcell = (MemberCell) acell;
            if (!"row_header_header".equals(mcell.getProperty("__headertype"))) {
                if (headers.length > 1) {
                    if ((headers[(headers.length - 2)][i] instanceof MemberCell)) {
                        MemberCell tempMcell = (MemberCell) headers[(headers.length - 2)][i];
                        if (tempMcell.getFormattedValue() != null) {
                            takeRowHeaderTotal[i] = true;
                            rowHeaderString[i] = mcell.getFormattedValue();
                        } else {
                            takeRowHeaderTotal[i] = false;
                        }
                    } else {
                        takeRowHeaderTotal[i] = false;
                    }
                } else {
                    takeRowHeaderTotal[i] = true;
                    rowHeaderString[i] = mcell.getFormattedValue();
                }
            } else {
                takeRowHeaderTotal[i] = false;
            }

        }

        orderedKeys = new Vector();
        distinctRowTotalsDouble = new HashMap();
        distinctRowTotalsString = new HashMap();
        for (int i = 0; i < rowHeaderString.length; i++) {
//            if (takeRowHeaderTotal[i] != 0) {
            if (takeRowHeaderTotal[i]) {
                String headKey = rowHeaderString[i];
                distinctRowTotalsDouble.put(headKey, Double.valueOf(0.0D));
                distinctRowTotalsString.put(headKey, "");
                if (!orderedKeys.contains(headKey)) {
                    orderedKeys.add(headKey);
                }
            }

        }

        writeHeader = false;
        for (AbstractBaseCell header[] : headers) {
            if (header.equals(lastHeadRow)) {
                writeHeader = true;
            }
            rows.add(convert(header, Cell.Type.COLUMN_HEADER));
        }

        writeHeader = false;
        for (int i = 0; i < body.length && (limit == 0 || i < limit); i++) {
            AbstractBaseCell[] row = body[i];
            rows.add(convert(row, Cell.Type.ROW_HEADER));
        }

        if ((body.length > 0) && (showColTotals)) {
            Cell[] totalRow = new Cell[body[0].length + (showRowTotals ? distinctRowTotalsDouble.keySet().size() : 0)];

            for (int i = 0; i < body[0].length; i++) {
                if (colTotalsString[i] == null) {
                    if ((i < totalRow.length - 1) && (colTotalsString[(i + 1)] != null)) {
                        totalRow[i] = new Cell("Column Totals", Cell.Type.ROW_HEADER_HEADER);
                    } else {
                        totalRow[i] = new Cell("", Cell.Type.ROW_HEADER_HEADER);
                    }
                } else {
                    Cell newCell = new Cell(colTotalsString[i], Cell.Type.ROW_HEADER_HEADER);
                    newCell.getProperties().setProperty("align", "right");
                    totalRow[i] = newCell;
                }
            }

            for (int i = 0; (i < distinctRowTotalsDouble.keySet().size()) && (showRowTotals); i++) {
                totalRow[(body[0].length + i)] = new Cell("", Cell.Type.ROW_HEADER_HEADER);
            }

            rows.add(totalRow);
        }

        QueryResult qr = new QueryResult(rows, cellSet.getRuntime(), cellSet.getWidth(), cellSet.getHeight());
        return qr;

    }

    public static Cell[] convert(AbstractBaseCell[] acells, Cell.Type headertype) {
//        Cell[] cells = new Cell[acells.length];
        Cell[] cells = new Cell[acells.length + (showRowTotals ? distinctRowTotalsDouble.size() : 0)];

        for (String key : distinctRowTotalsDouble.keySet()) {
            distinctRowTotalsDouble.put(key, Double.valueOf(0.0D));
            distinctRowTotalsString.put(key, "");
        }

        for (int i = 0; i < acells.length; i++) {
            colId = i;
            cells[i] = convert(acells[i], headertype);
        }

        for (int i = 0; (i < orderedKeys.size()) && (showRowTotals); i++) {
            if (((String) distinctRowTotalsString.get(orderedKeys.get(i))).equals("")) {
                if (writeHeader) {
                    cells[(acells.length + i)] = new Cell("Total for " + (String) orderedKeys.get(i), Cell.Type.ROW_HEADER_HEADER);
                } else {
                    cells[(acells.length + i)] = new Cell("", Cell.Type.ROW_HEADER_HEADER);
                }
            } else {
                Cell newCell = new Cell((String) distinctRowTotalsString.get(orderedKeys.get(i)), Cell.Type.ROW_HEADER_HEADER);
                newCell.getProperties().setProperty("align", "right");
                cells[(acells.length + i)] = newCell;
            }
        }
        return cells;
    }

    public static Cell convert(AbstractBaseCell acell, Cell.Type headertype) {
        if (acell != null) {
            if (acell instanceof DataCell) {
                DataCell dcell = (DataCell) acell;
                Properties metaprops = new Properties();
                // metaprops.put("color", "" + dcell.getColorValue());
                String position = null;
                for (Integer number : dcell.getCoordinates()) {
                    if (position != null) {
                        position += ":" + number.toString();
                    } else {
                        position = number.toString();
                    }
                }
                if (position != null) {
                    metaprops.put("position", position);
                }

                if (dcell != null && dcell.getRawNumber() != null) {
                    metaprops.put("raw", "" + dcell.getRawNumber());
                }


                if (takeToTotal) {
                    double currentRawNumber = dcell.getRawNumber() != null ? dcell.getRawNumber().doubleValue() : 0.0D;
                    colTotals[colId] += currentRawNumber;
                    try {
                        DecimalFormat myFormatter = new DecimalFormat(dcell.getFormatString());
                        colTotalsString[colId] = myFormatter.format(colTotals[colId]);
                    } catch (Exception e) {
                    }
                }

//                if (takeRowHeaderTotal[colId] != 0) {
                if (takeRowHeaderTotal[colId]) {
                    double currentRawNumber = dcell.getRawNumber() != null ? dcell.getRawNumber().doubleValue() : 0.0D;
                    double tempTotal = ((Double) distinctRowTotalsDouble.get(rowHeaderString[colId])).doubleValue();

                    distinctRowTotalsDouble.put(rowHeaderString[colId], Double.valueOf(currentRawNumber + tempTotal));
                    try {
                        DecimalFormat myFormatter = new DecimalFormat(dcell.getFormatString());
                        distinctRowTotalsString.put(rowHeaderString[colId], myFormatter.format(distinctRowTotalsDouble.get(rowHeaderString[colId])));
                    } catch (Exception e) {
                    }
                }

                metaprops.putAll(dcell.getProperties());

                // TODO no properties  (NULL) for now - 
                return new Cell(dcell.getFormattedValue(), metaprops, Cell.Type.DATA_CELL);
            }
            if (acell instanceof MemberCell) {
                MemberCell mcell = (MemberCell) acell;
//				Properties metaprops = new Properties();
//				metaprops.put("children", "" + mcell.getChildMemberCount());
//				metaprops.put("uniqueName", "" + mcell.getUniqueName());

                Properties props = new Properties();
                if (mcell != null) {
                    if (mcell.getParentDimension() != null) {
                        props.put("dimension", mcell.getParentDimension());
                    }
                    if (mcell.getUniqueName() != null) {
                        props.put("uniquename", mcell.getUniqueName());
                    }
                    if (mcell.getHierarchy() != null) {
                        props.put("hierarchy", mcell.getHierarchy());
                    }
                    if (mcell.getLevel() != null) {
                        props.put("level", mcell.getLevel());
                    }
                    if (mcell.getFormattedValue() == null) {
                        takeToTotal = false;
                    } else {
                        takeToTotal = true;
                    }
                }
//				props.putAll(mcell.getProperties());

                // TODO no properties  (NULL) for now - 
                if ("row_header_header".equals(mcell.getProperty("__headertype"))) {
                    headertype = Cell.Type.ROW_HEADER_HEADER;
                }
                return new Cell("" + mcell.getFormattedValue(), props, headertype);
            }

        }
        return null;
    }
}
