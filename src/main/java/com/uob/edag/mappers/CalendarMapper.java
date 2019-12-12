package com.uob.edag.mappers;

import java.util.List;
import java.util.Map;

import com.uob.edag.model.CalendarModel;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 02/09/2017
 * @Description : The class is the MyBatis mapper class for the Business Calendar class.
 */
public interface CalendarMapper {
  
  void rollOverDailyCalendar();
  
  void updateProcFlagDailyY();
  
  void updateProcFlagDailyN();
  
  CalendarModel retrieveCurrentBizDate(Map<String, String> params);
  
  List<CalendarModel> getCurrentCalendarEntries();
  
  void updateMonthlyCalendarEntry(CalendarModel model);
}
