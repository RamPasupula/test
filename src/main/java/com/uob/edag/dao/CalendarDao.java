package com.uob.edag.dao;

import java.util.HashMap;
import java.util.List;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.DBMSConnectionFactory;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.mappers.CalendarMapper;
import com.uob.edag.model.CalendarModel;


/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 02/09/2017
 * @Description : The class is used for database operations to update/retrieve details
 *              for Business Calendar Process.
 * 
 */

public class CalendarDao extends AbstractDao {
	
	public static final PredefinedException CANNOT_ROLLOVER_CALENDAR = new PredefinedException(CalendarDao.class, "CANNOT_ROLLOVER_CALENDAR", "Unable to rollover daily calendar: {0}");
	public static final PredefinedException CANNOT_SET_PROC_FLAG_DAILY = new PredefinedException(CalendarDao.class, "CANNOT_SET_PROC_FLAG_DAILY", "Unable to update processed flag daily to Y: {0}");
	public static final PredefinedException CANNOT_UNSET_PROC_FLAG_DAILY = new PredefinedException(CalendarDao.class, "CANNOT_UNSET_PROC_FLAG_DAILY", "Unable to update processed flag daily to N: {0}");
	public static final PredefinedException CANNOT_RETRIEVE_CURRENT_BIZ_DATE = new PredefinedException(CalendarDao.class, "CANNOT_RETRIEVE_CURRENT_BIZ_DATE", 
			                                                                                               "Unable to retrieve current biz date for source system code {0}, country code {1}, frequency {2}: {3}");
	public static final PredefinedException CANNOT_GET_CAL_ENTRIES = new PredefinedException(CalendarDao.class, "CANNOT_GET_CAL_ENTRIES", "Unable to get current calendar entries: {0}");
	public static final PredefinedException CANNOT_UPDATE_MONTHLY_CAL_ENTRY = new PredefinedException(CalendarDao.class, "CANNOT_UPDATE_MONTHLY_CAL_ENTRY", "Unable to update monthly calendar entry to {0}: {1}");
	
  public CalendarDao() {
		super(DBMSConnectionFactory.getFactory());
	}

	/**
   * This method is used to rollover the daily calendar table.
	 * @throws EDAGMyBatisException 
   * @throws UobException 
   * @throws Exception when there is an error updating the calendar entry
   */
  public void rollOverDailyCalendar() throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
  		session.getMapper(CalendarMapper.class).rollOverDailyCalendar();
  		logger.debug("Daily calendar rolled over");
		} catch (PersistenceException e) {
		  throw new EDAGMyBatisException(CalendarDao.CANNOT_ROLLOVER_CALENDAR, e.getMessage());
		}
  }
  
  /**
   * This method is used to update the processing flag for holidays.
   * @throws Exception when there is an error updating the calendar entry
   */
  public void updateProcFlagDailyY() throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(CalendarMapper.class).updateProcFlagDailyY();
      logger.debug("Processed flag daily updated to Y");
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(CalendarDao.CANNOT_SET_PROC_FLAG_DAILY, e.getMessage());
    }
  }
  
  /**
   * This method is used to update the processing flag for working days.
   * @throws Exception when there is an error updating the calendar entry
   */
  public void updateProcFlagDailyN() throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(CalendarMapper.class).updateProcFlagDailyN();
      logger.debug("Processed flag daily updated to N");
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(CalendarDao.CANNOT_UNSET_PROC_FLAG_DAILY, e.getMessage());
    } 
  }
  
  /**
   * This method is used to retrieve the current biz date from the calendar table.
   * @throws EDAGMyBatisException 
   * @throws Exception when there is an error updating the calendar entry
   */
  public CalendarModel retrieveCurrentBizDate(String srcSysCd, String ctryCd, String frequency) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("srcSysCd", srcSysCd);
      params.put("ctryCd", ctryCd);
      params.put("freqCd", frequency);

      CalendarModel result = session.getMapper(CalendarMapper.class).retrieveCurrentBizDate(params);
      logger.debug("Current biz date for source system code " + srcSysCd + ", country code " + ctryCd + 
      		         ", frequency " + frequency + " is " + result);
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(CalendarDao.CANNOT_RETRIEVE_CURRENT_BIZ_DATE, srcSysCd, ctryCd, frequency, e.getMessage());
    } 
  }
  
  /**
   * This method is used to retrieve the current calendar entries.
   * @throws EDAGMyBatisException 
   * @throws Exception when there is an error retrieving the calendar entries
   */
  public List<CalendarModel> getCurrentCalendarEntries() throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	List<CalendarModel> result = session.getMapper(CalendarMapper.class).getCurrentCalendarEntries();
    	logger.debug("Calendar entry count: " + (result == null ? 0 : result.size()));
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(CalendarDao.CANNOT_GET_CAL_ENTRIES, e.getMessage());
    } 
  }
  
  /**
   * This method is used to update the prev and curr biz date for monthly calendar.
   * @throws EDAGMyBatisException 
   * @throws Exception when there is an error updating the calendar entry
   */
  public void updateMonthlyCalendarEntry(CalendarModel model) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(CalendarMapper.class).updateMonthlyCalendarEntry(model);
      logger.debug("Monthly calendar entry updated to " + model);
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(CalendarDao.CANNOT_UPDATE_MONTHLY_CAL_ENTRY, model, e.getMessage());
    } 
  }
}
