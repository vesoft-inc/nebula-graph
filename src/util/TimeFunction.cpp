
/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/


#include "TimeFunction.h"
#include "time/WallClock.h"
#include <boost/date_time/gregorian/greg_date.hpp>
#include <boost/date_time/gregorian/gregorian_io.hpp>
#include <boost/date_time/gregorian/formatters.hpp>

namespace nebula {
namespace time {

const uint8_t daysOfMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

constexpr int64_t maxTimestamp = std::numeric_limits<int64_t>::max() / 1000000000;

using boost::posix_time::ptime;
using boost::gregorian::date;
using boost::gregorian::days;
using boost::gregorian::weeks;
using boost::posix_time::time_facet;
using boost::posix_time::hours;
using boost::posix_time::minutes;
using boost::posix_time::seconds;
using boost::posix_time::millisec;

// get timezone struct
StatusOr<Timezone> TimeCommon::getTimezone(const std::string &timezoneStr) {
    if (timezoneStr.length() != 6) {
        return Status::Error("Invalid timezone type");
    }

    if ((timezoneStr[0] != '+' && timezoneStr[0] != '-') ||
        (timezoneStr[1] > '1' && timezoneStr[1] < '0') ||
        (timezoneStr[2] > '9' && timezoneStr[2] < '0') ||
        (timezoneStr[4] > '1' && timezoneStr[4] < '0') ||
        (timezoneStr[5] > '9' && timezoneStr[4] < '0') ||
        timezoneStr[3] != ':') {
        return Status::Error("Invalid timezone type");
    }

    auto hour = (timezoneStr[1] - '0') * 10 + (timezoneStr[2] - '0');
    auto minute = (timezoneStr[4] - '0') * 10 + (timezoneStr[5] - '0');

    if (hour > 13 || minute > 59 || (hour == 13 && minute != 0)) {
        return Status::Error("Invalid timezone type");
    }
    if (timezoneStr[0] != '-' && hour == 13) {
        return Status::Error("Invalid timezone type");
    }

    Timezone timezone;
    timezone.eastern = timezoneStr[0] == '-' ? '-' : '+';
    timezone.hour = hour;
    timezone.minute = minute;
    return timezone;
}

// get current datetime
DateTime TimeCommon::getUTCTime(bool decimals) {
    time_t seconds;
    int64_t microSeconds;
    DateTime result;
    if (decimals) {
        auto time = time::WallClock::fastNowInMicroSec();
        microSeconds = time % 1000000;
        seconds = time / 1000000;
        result.microsec = microSeconds;
    } else {
        seconds = time::WallClock::fastNowInSec();
    }

    struct tm utcTime;
    if (0 != gmtime_r(&seconds, &utcTime)) {
        result.year = utcTime.tm_year + 1900;
        result.month = utcTime.tm_mon + 1;
        result.day = utcTime.tm_mday;
        result.hour = utcTime.tm_hour;
        result.minute = utcTime.tm_min;
        result.sec = utcTime.tm_sec;
    }
    return result;
}

// return true if value is Ok
bool TimeCommon::checkDateTime(const DateTime &nTime) {
    if (nTime.year > 9999 || nTime.month > 12 ||
        nTime.minute > 59 || nTime.sec > 59 ||
        nTime.microsec > 999999 || nTime.hour > 23) {
        return false;
    }
    if (nTime.month == 0) {
        return true;
    }
    if (nTime.month != 2) {
        if (nTime.day > daysOfMonth[nTime.month - 1]) {
            return false;
        }
    } else {
        if (isLeapyear(nTime.year) && nTime.day > 29) {
            return false;
        } else if (!isLeapyear(nTime.year) && nTime.day > 28) {
            return false;
        }
    }

    return true;
}

StatusOr<DateTime> TimeCommon::getDate(int64_t date) {
    DateTime result;
    // make it as year
    if (date < 10000) {
        result.year = date;
        return result;
    }

    result.day = date % 100;
    result.month = date / 100 % 100;
    result.year = date / 10000;
    return result;
}

StatusOr<DateTime> TimeCommon::getDateTime(int64_t time) {
    DateTime result;
    int64_t timePart = time % 1000000;
    int64_t dataPart = time / 1000000;

    result.sec = timePart % 100;
    result.minute = timePart / 100 % 100;
    result.hour = timePart / 10000;
    result.day = dataPart % 100;
    result.month = dataPart / 100 % 100;
    result.year = dataPart / 10000;
    return result;
}

StatusOr<DateTime> TimeCommon::getTimestamp(int64_t time) {
    UNUSED(time);
    DateTime result;
    return result;
}

StatusOr<DateTime> TimeCommon::isDate(const std::string &value) {
    if (value.find('-') == std::string::npos ||
        value.find(':') != std::string::npos) {
        return Status::Error("without \'-\' or has \':\'");
    }
    static const std::regex reg("^([1-9]\\d{3})-"
                                "(0[1-9]|1[0-2]|\\d)-"
                                "(0[1-9]|[1-2][0-9]|3[0-1]|\\d)\\s+");
    std::smatch result;
    if (!std::regex_match(value, result, reg)) {
        return Status::Error("Invalid date type");
    }
    DateTime nTime;
    nTime.year = atoi(result[1].str().c_str());
    nTime.month = atoi(result[2].str().c_str());
    nTime.day = atoi(result[3].str().c_str());
    return nTime;
}

StatusOr<DateTime> TimeCommon::isDateTime(const std::string &value) {
    if (value.find("-") == std::string::npos ||
        value.find(":") == std::string::npos) {
        return Status::Error("without \':\' or \'-\'");
    }
    DateTime nTime;
    std::string temp = value;
    auto pos = value.find(".");
    if (pos != std::string::npos) {
        std::string microsec = temp.substr(pos + 1);
        try {
            auto resultInt = folly::to<int64_t>(microsec);
            if (resultInt > 999999) {
                return Status::Error("SecondPart out of rang 999999");
            }
            nTime.microsec = resultInt;
        } catch (std::exception &e) {
            LOG(ERROR) << "Failed to cast Value::Type::STRING to Value::Type::INT: " << e.what();
            return Status::Error("Failed to cast Value::Type::STRING to Value::Type::INT: %s",
                                 e.what());
        }
        temp = temp.substr(0, pos);
    }
    // TODO: regex is inefficient, need to modify
    static const std::regex reg("^([1-9]\\d{3})-"
                                "(0[1-9]|1[0-2]|\\d)-"
                                "(0[1-9]|[1-2][0-9]|3[0-1]|\\d)\\s+"
                                "(20|21|22|23|[0-1]\\d|\\d):"
                                "([0-5]\\d|\\d):"
                                "([0-5]\\d|\\d)$");
    std::smatch result;
    if (!std::regex_match(temp, result, reg)) {
        return Status::Error("Invalid time type");
    }

    nTime.year = atoi(result[1].str().c_str());
    nTime.month = atoi(result[2].str().c_str());
    nTime.day = atoi(result[3].str().c_str());
    nTime.hour = atoi(result[4].str().c_str());
    nTime.minute = atoi(result[5].str().c_str());
    nTime.sec = atoi(result[6].str().c_str());
    return nTime;
}

bool TimeCommon::isLeapyear(int32_t year) {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

// covert 20191201101000 to DateTime
StatusOr<DateTime> TimeCommon::intToDateTime(int64_t value, DateTimeType type) {
    switch (type) {
        case T_Date:
            return getDate(value);
        case T_DateTime:
            return getDateTime(value);
        case T_Timestamp:
            return getTimestamp(value);
        default:
            return Status::Error("Invalid type: %d", type);
    }
}

// covert "20191201101000" to DateTime
StatusOr<DateTime> TimeCommon::strToDateTime1(const std::string &value, DateTimeType type) {
    int64_t intValue = 0;
    try {
        intValue = folly::to<int64_t>(value);
    } catch (std::exception &e) {
        LOG(ERROR) << "Failed to cast STRING to INT: " << e.what();
        return Status::Error("Failed to cast STRING to INT: %s", e.what());
    }

    return intToDateTime(intValue, type);
}

// covert "2019-12-01 10:10:00" to DateTime
StatusOr<DateTime> TimeCommon::strToDateTime2(const std::string &value, DateTimeType type) {
    switch (type) {
        case T_Date:
            return isDate(value);
        case T_DateTime:
            return isDateTime(value);
        default: {
            auto status = isDate(value);
            if (status.ok()) {
                return status.value();
            }
            status = isDateTime(value);
            if (status.ok()) {
                return status.value();
            }
            return Status::Error("Invalid type: %d", type);
        }
    }
}

// covert OptVariantType to DateTime
StatusOr<DateTime> TimeCommon::toDateTimeValue(const Value &value, DateTimeType type) {
    StatusOr<DateTime> status;
    switch (value.type()) {
        case Value::Type::INT:
            status = intToDateTime(value.getInt(), type);
            break;
        case Value::Type::STRING: {
            std::string temp = value.getStr();
            if (temp.find("-") != std::string::npos ||
                temp.find(":") != std::string::npos) {
                status = strToDateTime2(temp, type);
            } else {
                status = strToDateTime1(temp, type);
            }
            break;
        }
        default:
            return Status::Error("Error type: %d", static_cast<int32_t>(value.type()));
    }
    if (!status.ok()) {
        return status.status();
    }
    auto resultValue = status.value();
    if (!checkDatetime(resultValue)) {
        return Status::Error("Wrong time format");
    }
    return resultValue;
}

Status TimeCommon::UpdateDateTime(DateTime &datetime, int32_t dayCount, bool add) {
    boost::gregorian::date newDate;
    try {
        if (add) {
            newDate = boost::gregorian::date(datetime.year, datetime.month, datetime.day) +
                      boost::gregorian::days(dayCount);
        } else {
            newDate = boost::gregorian::date(datetime.year, datetime.month, datetime.day) -
                      boost::gregorian::days(dayCount);
        }
    } catch(std::exception& e) {
        LOG(ERROR) << "Error bad date: " << e.what();
        return Status::Error("Error bad date");
    }
    datetime.year = newDate.year();
    datetime.month = newDate.month();
    datetime.day = newDate.day();
    return Status::OK();
}

Status TimeCommon::dateTimeAddTz(DateTime &datetime, const Timezone &timezone) {
    bool add = true;
    bool updateDate = false;
    int32_t dayCount = 0;
    if (timezone.eastern == '-') {
        if (datetime.minute > timezone.minute) {
            datetime.minute -= timezone.minute;
        } else {
            datetime.hour--;
            datetime.minute = datetime.minute + 60 - timezone.minute;
        }
        if (datetime.hour > timezone.hour) {
            datetime.hour -= timezone.hour;
        } else {
            updateDate = true;
            add = false;
            if (datetime.hour + 24 < timezone.hour) {
                dayCount = 2;
                datetime.hour = datetime.hour + 48 - timezone.hour;
            } else {
                dayCount = 1;
                datetime.hour = datetime.hour + 24 - timezone.hour;
            }
        }
    } else {
        if (datetime.minute + timezone.minute < 60) {
            datetime.minute += timezone.minute;
        } else {
            updateDate = true;
            datetime.hour++;
            datetime.minute = datetime.minute + timezone.minute - 60;
        }
        if (datetime.hour + timezone.hour < 24) {
            datetime.hour += timezone.hour;
        } else if (datetime.hour + timezone.hour >= 48) {
            updateDate = true;
            dayCount = 2;
            datetime.hour = datetime.hour + timezone.hour - 48;
        } else {
            updateDate = true;
            dayCount = 1;
            datetime.hour = datetime.hour + timezone.hour - 24;
        }
    }

    if (updateDate) {
        auto updateStatus = TimeCommon::UpdateDateTime(datetime, dayCount, add);
        if (!updateStatus.ok()) {
            return updateStatus;
        }
    }
    return Status::OK();
}

Status TimeCommon::timestampAddTz(Timestamp &timestamp, const Timezone &timezone) {
    uint32_t seconds = timezone.hour * 3600 + timezone.minute * 60;
    if (timezone.eastern == '-') {
        if (timestamp < seconds) {
            return Status::Error("Out of timestamp");
        }
        timestamp -= seconds;
    } else {
        if (timestamp + seconds > maxTimestamp) {
            return Status::Error("Out of timestamp");
        }
        timestamp += seconds;
    }
    return Status::OK();
}

StatusOr<Timestamp> TimeCommon::dateTimeToTimestamp(const DateTime &datetime) {
    try {
        ptime time(date(datetime.year, datetime.month, datetime.day),
                   hours(datetime.hour) + minutes(datetime.minute) +
                   seconds(datetime.sec) + millisec(datetime.microsec));
        return to_time_t(time);
    } catch(std::exception& e) {
        LOG(ERROR) << "Illegal datetime: " <<  e.what();
        return Status::Error(folly::stringPrintf("Illegal datetime : %s", e.what()));
    }
}

StatusOr<Timestamp> TimeCommon::strToUTCTimestamp(const std::string &str,
                                                  const Timezone &timezone) {
    auto timeStatus = isDateTime(str);
    if (!timeStatus.ok()) {
        return timeStatus.status();
    }
    DateTime datetime = std::move(timeStatus).value();
    auto status = dateTimeAddTz(datetime, timezone);
    if (!status.ok()) {
        return status;
    }
    return dateTimeToTimestamp(datetime);
}

Status AddDateFunc::calcDateTime() {
    if (args_.size() == minArity_) {
        if (args_[1].type() != Value::Type::INT) {
            return Status::Error("Wrong days type");
        }

        return TimeCommon::UpdateDateTime(datetime_, args_[1].getInt(), true);
    } else {
        if (args_[1].type() != Value::Type::INT || args_[2].type() != Value::Type::INT) {
            return Status::Error("Wrong days type");
        }

        auto count = args_[1].getInt();
        IntervalType intervalType = static_cast<IntervalType>(args_[1].getInt());
        if (intervalType == MONTH) {
            if (datetime_.month + count > 12) {
                if (datetime_.year + 1 > 9999) {
                    return Status::Error("Year is out of rang");
                }
                datetime_.year++;
                datetime_.month = datetime_.month + count - 12;
            } else {
                datetime_.month += count;
            }
        } else if (intervalType == YEAR) {
            if (datetime_.year + count > 9999) {
                return Status::Error("Year is out of rang");
            } else {
                datetime_.year += count;
            }

        } else {
            ptime time(date(datetime_.year, datetime_.month, datetime_.day),
                       hours(datetime_.hour) + minutes(datetime_.minute) +
                       seconds(datetime_.sec) + millisec(datetime_.microsec));
            switch (intervalType) {
                case MICROSECOND:
                    time = time + millisec(count);
                    break;
                case SECOND:
                    time = time + seconds(count);
                    break;
                case MINUTE:
                    time = time + minutes(count);
                    break;
                case HOUR:
                    time = time + hours(count);
                    break;
                case DAY:
                    time = time + days(count);
                    break;
                case WEEK:
                    time = time + weeks(count);
                    break;
                default:
                    return Status::Error("Types that cannot be handled");
            }
            ptimeToDateTime(time, datetime_);
        }
        return Status::OK();
    }
}

Status CalcTimeFunc::initDateTime(std::vector<Value> args,
                                  const Timezone *timezone) {
    if (!checkArgs(args)) {
        LOG(ERROR) << "Wrong number args";
        return Status::Error("Wrong number args");
    }
    auto status = TimeCommon::toDateTimeValue(args[0], T_DateTime);
    if (!status.ok()) {
        LOG(ERROR) << status.status();
        return status.status();
    }
    datetime_ = std::move(status).value();
    auto addStatus = TimeCommon::toDateTimeValue(args[1], T_DateTime);
    if (!addStatus.ok()) {
        LOG(ERROR) << addStatus.status();
        return addStatus.status();
    }
    addNebulaTime_ = std::move(addStatus).value();
    if (addNebulaTime_.year != 0 || addNebulaTime_.month != 0 || addNebulaTime_.day != 0) {
        LOG(ERROR) << "Wrong type";
        return Status::Error("Wrong type");
    }
    timezone_ = timezone;
    args_ = std::move(args);
    return Status::OK();
}

Status CalcTimeFunc::calcDateTime() {
    try {
        date date1(datetime_.year, datetime_.month, datetime_.day);
        ptime time1(date1, hours(datetime_.hour) + minutes(datetime_.minute) +
                           seconds(datetime_.sec) + millisec(datetime_.microsec));
        ptime time2;
        if (add_) {
            time2 = time1 + hours(addNebulaTime_.hour) + minutes(addNebulaTime_.minute) +
                    seconds(addNebulaTime_.sec) + millisec(addNebulaTime_.microsec);
        } else {
            time2 = time1 - hours(addNebulaTime_.hour) - minutes(addNebulaTime_.minute) -
                    seconds(addNebulaTime_.sec) - millisec(addNebulaTime_.microsec);
        }

        ptimeToDateTime(time2, datetime_);
        return Status::OK();
    } catch(std::exception& e) {
        LOG(ERROR) << "  Exception: " <<  e.what();
        return Status::Error(folly::stringPrintf("Error : %s", e.what()));
    }
}

Status SubDateFunc::calcDateTime() {
    if (args_[1].type() != Value::Type::INT) {
        return Status::Error("Wrong days type");
    }

    return TimeCommon::UpdateDateTime(datetime_, args_[1].getInt(), false);
}

Status ConvertTzFunc::calcDateTime() {
    if (args_[1].type() != Value::Type::STRING && args_[2].type() != Value::Type::STRING) {
        return Status::Error("Wrong timezone type");
    }
    auto statusTz = TimeCommon::getTimezone(args_[1].getStr());
    if (!statusTz.ok()) {
        LOG(ERROR) << statusTz.status();
        return statusTz.status();
    }
    auto timezone1 = statusTz.value();

    statusTz = TimeCommon::getTimezone(args_[2].getStr());
    if (!statusTz.ok()) {
        return statusTz.status();
    }
    auto timezone2 = statusTz.value();

    Timezone timezone;
    if (timezone1.eastern != timezone2.eastern) {
        timezone.minute = timezone1.minute + timezone2.minute;
        if (timezone.minute >= 60) {
            timezone.minute -= 60;
            timezone.hour++;
        }
        timezone.hour += timezone1.hour + timezone2.hour;
        if (timezone1.eastern == '+') {
            timezone.eastern = '-';
        } else {
            timezone.eastern = '+';
        }
    } else {
        timezone.minute = timezone2.minute - timezone1.minute;
        timezone.hour = timezone2.hour - timezone1.hour;
        if (timezone1.eastern == '+') {
            timezone.eastern = '+';
        } else {
            timezone.eastern = '-';
        }
    }

    auto status = TimeCommon::dateTimeAddTz(datetime_, timezone);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status CurrentDateFunc::calcDateTime() {
    if (timezone_ == nullptr) {
        return Status::Error("Get timezone failed");
    }

    datetime_ = TimeCommon::getUTCTime();
    if (timezone_->eastern == '-') {
        if (datetime_.hour < timezone_->hour) {
            auto updateStatus = TimeCommon::UpdateDateTime(datetime_, 1, false);
            if (!updateStatus.ok()) {
                return updateStatus;
            }
        }
    } else {
        if (datetime_.hour + timezone_->hour >= 24) {
            auto updateStatus = TimeCommon::UpdateDateTime(datetime_, 1, true);
            if (!updateStatus.ok()) {
                return updateStatus;
            }
        }
    }
    return Status::OK();
}

Status CurrentTimeFunc::calcDateTime() {
    if (timezone_ == nullptr) {
        return Status::Error("Get timezone failed");
    }

    datetime_ = TimeCommon::getUTCTime();
    if (timezone_->eastern == '-') {
        if (datetime_.minute > timezone_->minute) {
            datetime_.minute -= timezone_->minute;
        } else {
            datetime_.hour--;
            datetime_.minute = datetime_.minute + 60 - timezone_->minute;
        }
        if (datetime_.hour > timezone_->hour) {
            datetime_.hour -= timezone_->hour;
        } else {
            datetime_.hour = datetime_.hour + 24 - timezone_->hour;
        }
    } else {
        if (datetime_.minute + timezone_->minute < 60) {
            datetime_.minute += timezone_->minute;
        } else {
            datetime_.hour++;
            datetime_.minute = datetime_.minute + timezone_->minute - 60;
        }
        if (datetime_.hour + timezone_->hour < 24) {
            datetime_.hour += timezone_->hour;
        } else {
            datetime_.hour = datetime_.hour + timezone_->hour - 24;
        }
    }
    return Status::OK();
}

StatusOr<Value> YearFunc::getResult() {
    return datetime_.year;
}

StatusOr<Value> MonthFunc::getResult() {
    return datetime_.month;
}


StatusOr<Value> DayFunc::getResult() {
    return datetime_.day;
}

StatusOr<Value> DayOfMonthFunc::getResult() {
    return datetime_.day;
}

StatusOr<Value> DayOfWeekFunc::getResult() {
    boost::gregorian::date inputDate(datetime_.year, datetime_.month, datetime_.day);
    return boost::lexical_cast<std::string>(inputDate.day_of_week());
}

StatusOr<Value> DayOfYearFunc::getResult() {
    boost::gregorian::date inputDate(datetime_.year, datetime_.month, datetime_.day);
    return boost::lexical_cast<int64_t>(inputDate.day_of_year());
}

StatusOr<Value> HourFunc::getResult() {
    return datetime_.hour;
}

StatusOr<Value> MinuteFunc::getResult() {
    return datetime_.minute;
}

StatusOr<Value> SecondFunc::getResult() {
    return datetime_.sec;
}

StatusOr<Value> TimeFormatFunc::getResult() {
    if (args_[1].type() != Value::Type::STRING) {
        return Status::Error("Error format type");
    }

    try {
        date d(datetime_.year, datetime_.month, datetime_.day);
        ptime time(d, hours(datetime_.hour) + minutes(datetime_.minute) +
                      seconds(datetime_.sec) + millisec(datetime_.microsec));
        time_facet *timeFacet = new time_facet(args_[1].getStr().c_str());

        std::stringstream stream;
        stream.imbue(std::locale(stream.getloc(), timeFacet));
        stream << time;
        return stream.str();
    } catch(std::exception& e) {
        LOG(ERROR) << "Fromat Exception: " <<  e.what();
        return Status::Error(folly::stringPrintf("Error : %s", e.what()));
    }
}

StatusOr<Value> TimeToSecFunc::getResult() {
    return datetime_.hour * 3600 + datetime_.minute * 60 + datetime_.sec;
}

Status TimeToSecFunc::calcDateTime() {
    return Status::Error("Does not support");
}

Status MakeDateFunc::calcDateTime() {
    if (args_[1].type() != Value::Type::INT) {
        return Status::Error("Wrong dayofyear type: %d", static_cast<int32_t>(args_[1].type()));
    }
    datetime_.month = 1;
    datetime_.day = 1;
    auto status = TimeCommon::UpdateDateTime(datetime_, args_[1].getInt(), true);
    if (!status.ok()) {
        return status;
    }
    // Minus the initial value
    if (datetime_.month == 1) {
        status = TimeCommon::UpdateDateTime(datetime_, 1, false);
    } else {
        status = TimeCommon::UpdateDateTime(datetime_, 31, false);
    }

    return status;
}

Status MakeTimeFunc::initDateTime(std::vector<Value> args, const Timezone*) {
    if (!checkArgs(args)) {
        return Status::Error("Error args number");
    }
    if (args[0].type() != Value::Type::INT ||
        args[1].type() != Value::Type::INT ||
        args[2].type() != Value::Type::INT) {
        return Status::Error("Wrong time type");
    }
    args_ = std::move(args);
    return Status::OK();
}

Status MakeTimeFunc::calcDateTime() {
    datetime_.hour = args_[0].getInt();
    datetime_.minute = args_[1].getInt();
    datetime_.sec = args_[2].getInt();

    if (datetime_.hour > 23 || datetime_.minute > 59 || datetime_.sec > 59) {
        return Status::Error("Wrong time type");
    }
    return Status::OK();
}

StatusOr<Value> UnixTimestampFunc::getResult() {
    auto status = calcDateTime();
    if (!status.ok()) {
        return status;
    }
    return timestamp_;
}

Status UnixTimestampFunc::calcDateTime() {
    if (args_.size() != 0) {
        auto status = TimeCommon::dateTimeToTimestamp(datetime_);
        if (!status.ok()) {
            return status.status();
        }
        timestamp_ = status.value();
    } else {
        timestamp_ = time::WallClock::fastNowInSec();
    }
    return Status::OK();
}

StatusOr<Value> TimestampFormatFunc::getResult() {
    try {
        if (args_[1].type() != Value::Type::INT) {
            return Status::Error("Wrong type");
        }
        ptime time = boost::posix_time::from_time_t(timestamp_);
        time_facet *timeFacet = new time_facet(args_[1].getStr().c_str());

        std::stringstream stream;
        stream.imbue(std::locale(stream.getloc(), timeFacet));
        stream << time;
        return stream.str();
    } catch (std::exception &e) {
        LOG(ERROR) << "Fromat Exception: " << e.what();
        return Status::Error(folly::stringPrintf("Error : %s", e.what()));
    }
}
}  // namespace time
}  // namespace nebula
