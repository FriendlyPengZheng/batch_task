package com.tbc.batchtask.Utiles;

import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

public class DateUtils {

    public static String format(Date date, String pattern){
        DateFormat dateFormat = new SimpleDateFormat(pattern);
        return dateFormat.format(date);
    }

    /**
     * java8的LocalDateTime.parse方法必须要保证输入的时间，和格式匹配，否则会报错
     * @param text
     * @param pattern yyyy-MM-dd HH:mm:ss VV
     * @return
     * @throws ParseException
     */
    @SuppressWarnings("unused")
    public static Date parse(String text, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime localDate = LocalDateTime.parse(text, formatter);
        return Date.from(localDate.atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * 替代parse方法
     * @param text
     * @param pattern
     * @return
     * @throws ParseException
     */
    public static Date parseString(String text, String pattern) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(text);
    }

    /**
     * 遍历两个日期之间所有的日期
     * @param start
     * @param end
     * @return
     */
    public static List<String> getBetweenDate(String start, String end){
        List<String> list = new ArrayList<>();
        LocalDate startDate = LocalDate.parse(start);
        LocalDate endDate = LocalDate.parse(end);

        long distance = ChronoUnit.DAYS.between(startDate, endDate);
        if (distance < 1) {
            return list;
        }
        Stream.iterate(startDate, d -> {
            return d.plusDays(1);
        }).limit(distance + 1).forEach(f -> {
            list.add(f.toString());
        });
        return list;
    }

    /**
     * 获取unix时间戳,如果time为"",则返回当前时间戳
     * @param time
     * @return
     */
    public static Long getUnixTimestamp(String time){
        //初始化时区对象，北京时间是UTC+8，所以入参为8
        ZoneOffset zoneOffset= ZoneOffset.ofHours(8);
        //初始化LocalDateTime对象
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.now();
        if(StringUtils.isNotBlank(time)){
            localDateTime = LocalDateTime.parse(time,formatter);
        }
        //获取LocalDateTime对象对应时区的Unix时间戳
        return localDateTime.toEpochSecond(zoneOffset);
    }

    /**
     * 获取昨天的日期eg：2020-01-01
     * @return
     */
    public static String getYesterDay(){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime localDateTime = LocalDateTime.now().plusDays(-1);
        return localDateTime.format(formatter);
    }
    /**
     * 获取日期eg：2020-01-01
     * @return
     */
    public static String getDate(Integer n){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime localDateTime = LocalDateTime.now().plusDays(n);
        return localDateTime.format(formatter);
    }
}
