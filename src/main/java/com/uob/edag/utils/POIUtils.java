package com.uob.edag.utils;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.NumberToTextConverter;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGPOIException;
import com.uob.edag.exception.EDAGValidationException;

public class POIUtils {

	private static Logger logger = Logger.getLogger(POIUtils.class);

	public static <P> P getCellContent(Row row, int colNum, Class<P> dataType) throws EDAGValidationException, EDAGPOIException {
		if (!String.class.equals(dataType) && !Integer.class.equals(dataType) && !Double.class.equals(dataType) &&
				!Boolean.class.equals(dataType)) {
			throw new EDAGPOIException(dataType.getName() + " is not supported");
		}
		
		Object result = null;
		Cell cell = row.getCell(colNum);
		CellType cellType;
		if (cell == null || (cellType = cell.getCellTypeEnum()) == CellType.BLANK) {
		  result = String.class.equals(dataType) ? "" :
		  	       Integer.class.equals(dataType) ? Integer.valueOf(0) :
		  	       Double.class.equals(dataType) ? Double.valueOf(0) : Boolean.FALSE;
		} 
		else if(cellType == CellType.ERROR)
		{
			result = "";
		}
		else if (cellType == CellType.STRING) {
				String val = cell.getStringCellValue();
				val = val == null ? null : val.trim();
				result = String.class.equals(dataType) ? val :
					       Integer.class.equals(dataType) ? ("".equals(val) ? 0 : Integer.parseInt(val)) :
					       Double.class.equals(dataType) ? ("".equals(val) ? 0 : Double.parseDouble(val)) 
					      		                           : UobUtils.parseBoolean(val);	 
		} else if (cellType == CellType.NUMERIC) {
			if (DateUtil.isCellDateFormatted(cell)) {
				// DateCell Value can be retrieved as String only
				// The equivalent long (date value) is sent for easy conversions
				result = String.valueOf(cell.getDateCellValue().getTime());
		    } else {
				String val = NumberToTextConverter.toText(cell.getNumericCellValue());
				result = String.class.equals(dataType) ? val :
					       Integer.class.equals(dataType) ? Double.valueOf(val).intValue() :
				         Double.class.equals(dataType) ? Double.valueOf(val) : UobUtils.parseBoolean(Integer.toString(Double.valueOf(val).intValue()));
		    }

		} else if (cellType == CellType.BOOLEAN) {
				boolean val = cell.getBooleanCellValue();
				result = String.class.equals(dataType) ? Boolean.toString(val) :
					       Boolean.class.equals(dataType) ? val : Boolean.FALSE;
		} else if (cellType == CellType.FORMULA) {
			cellType = cell.getCachedFormulaResultTypeEnum();
			if(cellType == CellType.ERROR)
			{
				result = "";
			}
			else if (String.class.equals(dataType)) {
				String enabledStreamFlag = PropertyLoader.getProperty(UobConstants.EXCEL_STREAM_ENABLE_FLAG);
				if(enabledStreamFlag.equalsIgnoreCase("TRUE"))
				{
					try {
						result = NumberToTextConverter.toText(cell.getNumericCellValue());
					} catch (IllegalStateException | NumberFormatException e) {
						if(cell.getStringCellValue().charAt(0) == '"' && cell.getStringCellValue().charAt(cell.getStringCellValue().length()-1) == '"')
								result = cell.getStringCellValue().substring(1, cell.getStringCellValue().length()-1);
						else
							result = cell.getStringCellValue();
					}	
				}
				else {
					try {
						result = cell.getStringCellValue();
					} catch (IllegalStateException e) {
						result = Double.toString(cell.getNumericCellValue());
					}
				}
			} else if (Double.class.equals(dataType)) {
				result = cell.getNumericCellValue();
			} else if (Integer.class.equals(dataType)) {
				result = Double.valueOf(cell.getNumericCellValue()).intValue();
			} else {
				try {
					result = cell.getBooleanCellValue();
				} catch (IllegalStateException e) {
					try {
						result = UobUtils.parseBoolean(cell.getStringCellValue());
					} catch (IllegalStateException e1) {
						result = UobUtils.parseBoolean(Integer.toString(Double.valueOf(cell.getNumericCellValue()).intValue()));
					}
				}
			}	 
		}
		
		//logger.debug("Value of " + row.getSheet().getSheetName() + "[" + Integer.toString(row.getRowNum() + 1) + ", " + 
		             //Integer.toString(colNum + 1) + "] is " + result);
		return result == null ? null : dataType.cast(result);
	}

	/**
	 * This method is used to read the content of a cell from the given row and
	 * column.
	 * @param <P>
	 * @param <P>
	 * 
	 * @param sheet
	 *            The Sheet object from the Excel
	 * @param rowNum
	 *            The Row Number
	 * @param colNum
	 *            The Column Number
	 * @param dataType
	 *            The Data Type of the cell
	 * @return the Cell Value
	 * @throws EDAGValidationException 
	 * @throws  
	 * @throws Exception
	 *             when there is an error reading the cell content
	 */
	public static <P> P getCellContent(Sheet sheet, int rowNum, int colNum, Class<P> dataType) throws EDAGPOIException, EDAGValidationException {
		Row row = sheet.getRow(rowNum);
		return (row == null) ? null : getCellContent(row, colNum, dataType);
	}
}
