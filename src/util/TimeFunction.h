/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef UTIL_TIMEFUNCTION_H
#define UTIL_TIMEFUNCTION_H

#include "base/Base.h"
#include "base/StatusOr.h"
#include "base/Status.h"
#include "datatypes/Value.h"
#include "TimeType.h"
#include <boost/date_time/posix_time/posix_time.hpp>

/**
* Time functions can calculate from 1400 years, and the ones before 1400 years are not supported
*/

namespace nebula {
namespace time {
class TimeCommon final {
public:
    TimeCommon() = default;
    ~TimeCommon() = default;

public:
    static StatusOr<Timezone> getTimezone(const std::string &timezoneStr);

    // get current dateTime
    static DateTime getUTCTime(bool decimals = false);

    // return true if value is Ok
    static bool checkDateTime(const DateTime &nTime);

    static StatusOr<Date> intToDate(const int64_t value);

    static StatusOr<Date> strToDate(const std::string &value);

    static StatusOr<DateTime> intToDateTime(const int64_t value, DateTimeType type);

    static StatusOr<DateTime> strToDateTime1(const std::string &value, DateTimeType type);

    static StatusOr<DateTime> strToDateTime2(const std::string &value, DateTimeType type);

    static StatusOr<DateTime> intToTimestamp(const int64_t value, DateTimeType type);

    static StatusOr<DateTime> strToTimestamp(const std::string &value, DateTimeType type);

    static bool isLeapyear(int32_t year);

    static Status dateTimeAddTz(DateTime &dateTime, const Timezone &timezone);

    static Status timestampAddTz(Timestamp &timestamp, const Timezone &timezone);

    static StatusOr<Timestamp> dateTimeToTimestamp(const DateTime &dateTime);

    static StatusOr<Timestamp> strToUTCTimestamp(const std::string &str, const Timezone &timezone);

    static Status UpdateDateTime(DateTime &dateTime, int32_t dayCount, bool add);

    static StatusOr<DateTime> toDateTimeValue(const Value &value, DateTimeType type);

    static StatusOr<DateTime> getDate(int64_t date);

    static StatusOr<DateTime> getDateTime(int64_t dateTime);

    static StatusOr<DateTime> getTimestamp(int64_t timestamp);

    static StatusOr<DateTime> isDate(const std::string &value);

    static StatusOr<DateTime> isDateTime(const std::string &value);
};


class TimeBaseFunc {
public:
    explicit TimeBaseFunc(DateTimeType type = T_Null,
                          uint8_t minArity = 0,
                          uint8_t maxArity = 0)
            : type_(type), minArity_(minArity), maxArity_(maxArity) {}

    virtual ~TimeBaseFunc() {}

public:
    virtual Status initDateTime(std::vector<Value> args = {},
                                const Timezone *timezone = nullptr) {
        if (!checkArgs(args)) {
            LOG(ERROR) << "Wrong number of args";
            return Status::Error("Wrong number of args");
        }

        timezone_ = timezone;
        if (minArity_ == 0 && args.empty()) {
            return Status::OK();
        }
        auto status = TimeCommon::toDateTimeValue(args[0], type_);
        if (!status.ok()) {
            LOG(ERROR) << status.status();
            return status.status();
        }
        datetime_ = std::move(status).value();
        args_ = std::move(args);
        return Status::OK();
    }

    virtual StatusOr<Value> getResult() {
        auto status = calcDateTime();
        if (!status.ok()) {
            LOG(ERROR) << status;
            return status;
        }
        return datetime_.toString();
    }

    virtual std::string getFuncName() = 0;

    size_t getMinArity() { return  minArity_; }
    size_t getMaxArity() { return  maxArity_; }


    void ptimeToDateTime(boost::posix_time::ptime & time, DateTime &dateTime) {
        dateTime.year = time.date().year();
        dateTime.month = time.date().month();
        dateTime.day = time.date().day();
        dateTime.hour = time.time_of_day().hours();
        dateTime.minute = time.time_of_day().minutes();
        dateTime.sec = time.time_of_day().seconds();
        auto totalSeconds = time.time_of_day().total_seconds() * 1000000;
        auto totalMicroSeconds = time.time_of_day().total_microseconds();
        auto microSeconds = totalMicroSeconds - totalSeconds;
        if (microSeconds > 0) {
            dateTime.microsec = microSeconds;
        }
    }

protected:
    virtual Status calcDateTime() {
        return Status::OK();
    }

    bool checkArgs(const std::vector<Value> &args) {
        if (args.size() >= minArity_ && args.size() <= maxArity_) {
            return true;
        }
        return false;
    }

protected:
    DateTime                                    datetime_;
    Timestamp                                   timestamp_;
    DateTimeType                                type_{T_Null};
    std::vector<Value>                          args_;
    const Timezone                             *timezone_{nullptr};
    size_t                                      minArity_{0};
    size_t                                      maxArity_{0};
};

class CalcTimeFunc : public TimeBaseFunc {
public:
    explicit CalcTimeFunc(bool add) : TimeBaseFunc(T_DateTime, 2, 2), add_(add) {}
    ~CalcTimeFunc() {}

    std::string getFuncName() override {
        return "CalcTimeFunc";
    }

    Status initDateTime(std::vector<Value> args,
                        const Timezone *timezone = nullptr) override;

    Status calcDateTime() override;

private:
    DateTime            addNebulaTime_;
    bool                add_{true};
};

class AddDateFunc final : public TimeBaseFunc {
public:
    AddDateFunc() : TimeBaseFunc(T_DateTime, 2, 3) {}
    ~AddDateFunc() {}

    std::string getFuncName() override {
        return "AddDateFunc";
    }
protected:
    Status calcDateTime() override;
};

class AddTimeFunc final : public CalcTimeFunc {
public:
    AddTimeFunc() : CalcTimeFunc(true) {}
    ~AddTimeFunc() {}

    std::string getFuncName() override {
        return "AddTimeFunc";
    }
};

class SubDateFunc final : public TimeBaseFunc {
public:
    SubDateFunc() : TimeBaseFunc(T_Null, 2, 2) {}
    ~SubDateFunc() {}

    std::string getFuncName() override {
        return "SubDateFunc";
    }
protected:
    Status calcDateTime() override;
};

class SubTimeFunc final : public CalcTimeFunc {
public:
    SubTimeFunc() : CalcTimeFunc(false) {}
    ~SubTimeFunc() {}

    std::string getFuncName() override {
        return "SubTimeFunc";
    }
};

class ConvertTzFunc final : public TimeBaseFunc {
public:
    ConvertTzFunc() : TimeBaseFunc(T_Null, 3, 3) {}
    ~ConvertTzFunc() {}

    std::string getFuncName() override {
        return "ConvertTzFunc";
    }

protected:
    Status calcDateTime() override;
};


class CurrentDateFunc final : public TimeBaseFunc {
public:
    CurrentDateFunc() : TimeBaseFunc(T_Null, 0, 0) {}
    ~CurrentDateFunc() {}

    std::string getFuncName() override {
        return "CurrentDateFunc";
    }

protected:
    Status calcDateTime() override;
};

class CurrentTimeFunc final : public TimeBaseFunc {
public:
    CurrentTimeFunc() : TimeBaseFunc(T_Null, 0, 0) {}
    ~CurrentTimeFunc() {}

    std::string getFuncName() override {
        return "CurrentTimeFunc";
    }

protected:
    Status calcDateTime() override;
};

class YearFunc final : public TimeBaseFunc {
public:
    YearFunc() : TimeBaseFunc(T_DateTime, 1, 1) {}
    ~YearFunc() {}

    std::string getFuncName() override {
        return "YearFunc";
    }

    StatusOr<Value> getResult() override;
};

class MonthFunc final : public TimeBaseFunc {
public:
    MonthFunc() : TimeBaseFunc(T_DateTime, 1, 1) {}
    ~MonthFunc() {}

    std::string getFuncName() override {
        return "MonthFunc";
    }

    StatusOr<Value> getResult() override;
};

class DayFunc final : public TimeBaseFunc {
public:
    DayFunc() : TimeBaseFunc(T_DateTime, 1, 1) {}
    ~DayFunc() {}

    std::string getFuncName() override {
        return "DayFunc";
    }

    StatusOr<Value> getResult() override;
};

class DayOfMonthFunc final : public TimeBaseFunc {
public:
    DayOfMonthFunc() : TimeBaseFunc(T_Null, 1, 1) {}
    ~DayOfMonthFunc() {}

    std::string getFuncName() override {
        return "DayOfMonthFunc";
    }

    StatusOr<Value> getResult() override;
};

class DayOfWeekFunc final : public TimeBaseFunc {
public:
    DayOfWeekFunc() : TimeBaseFunc(T_Null, 1, 1) {}
    ~DayOfWeekFunc() {}

    std::string getFuncName() override {
        return "DayOfWeekFunc";
    }

    StatusOr<Value> getResult() override;
};

class DayOfYearFunc final : public TimeBaseFunc {
public:
    DayOfYearFunc() : TimeBaseFunc(T_DateTime, 1, 1) {}
    ~DayOfYearFunc() {}

    std::string getFuncName() override {
        return "DayOfYearFunc";
    }

    StatusOr<Value> getResult() override;
};

class HourFunc final : public TimeBaseFunc {
public:
    HourFunc() : TimeBaseFunc(T_DateTime, 1, 1) {}
    ~HourFunc() {}

    std::string getFuncName() override {
        return "HourFunc";
    }

    StatusOr<Value> getResult() override;
};

class MinuteFunc final : public TimeBaseFunc {
public:
    MinuteFunc() : TimeBaseFunc(T_DateTime, 1, 1) {}
    ~MinuteFunc() {}

    std::string getFuncName() override {
        return "MinuteFunc";
    }

    StatusOr<Value> getResult() override;
};

class SecondFunc final : public TimeBaseFunc {
public:
    SecondFunc() : TimeBaseFunc(T_DateTime, 1, 1) {}
    ~SecondFunc() {}

    std::string getFuncName() override {
        return "SecondFunc";
    }

    StatusOr<Value> getResult() override;
};

class TimeFormatFunc final : public TimeBaseFunc {
public:
    TimeFormatFunc() : TimeBaseFunc(T_Null, 2, 2) {}
    ~TimeFormatFunc() {}

    std::string getFuncName() override {
        return "TimeFormatFunc";
    }

    StatusOr<Value> getResult() override;
};

class TimeToSecFunc final : public TimeBaseFunc {
public:
    TimeToSecFunc() : TimeBaseFunc(T_DateTime, 1, 1) {}
    ~TimeToSecFunc() {}

    std::string getFuncName() override {
        return "TimeToSecFunc";
    }
    StatusOr<Value> getResult() override;

protected:
    Status calcDateTime() override;
};

class MakeDateFunc final : public TimeBaseFunc {
public:
    MakeDateFunc() : TimeBaseFunc(T_Date, 2, 2) {}
    ~MakeDateFunc() {}

    std::string getFuncName() override {
        return "MakeDateFunc";
    }

protected:
    Status calcDateTime() override;
};

class MakeTimeFunc final : public TimeBaseFunc {
public:
    MakeTimeFunc() : TimeBaseFunc(T_Null, 3, 3) {}
    ~MakeTimeFunc() {}

    Status initDateTime(std::vector<Value> args,
                        const Timezone *timezone = nullptr) override;

    std::string getFuncName() override {
        return "MakeTimeFunc";
    }

protected:
    Status calcDateTime() override;
};

class UTCDateFunc final : public TimeBaseFunc {
public:
    UTCDateFunc() : TimeBaseFunc(T_Null, 0, 0) {}
    ~UTCDateFunc() {}

    std::string getFuncName() override {
        return "UTCDateFunc";
    }
};

class UTCTimeFunc final : public TimeBaseFunc {
public:
    UTCTimeFunc() : TimeBaseFunc(T_Null, 0, 0) {}
    ~UTCTimeFunc() {}

    std::string getFuncName() override {
        return "UTCTimeFunc";
    }
};

class UTCTimestampFunc final : public TimeBaseFunc {
public:
    UTCTimestampFunc() : TimeBaseFunc(T_Null, 0, 0) {}
    ~UTCTimestampFunc() {}

    std::string getFuncName() override {
        return "UTCTimestampFunc";
    }
};

class UnixTimestampFunc final : public TimeBaseFunc {
public:
    UnixTimestampFunc() : TimeBaseFunc(T_DateTime, 0, 1) {}
    ~UnixTimestampFunc() {}

    std::string getFuncName() override {
        return "UTCTimestampFunc";
    }
    StatusOr<Value> getResult() override;

protected:
    Status calcDateTime() override;

private:
    Timestamp  timestamp_{0};
};

class TimestampFormatFunc final : public TimeBaseFunc {
public:
    TimestampFormatFunc() : TimeBaseFunc(T_Timestamp, 2, 2) {}
    ~TimestampFormatFunc() {}

    std::string getFuncName() override {
        return "TimestampFormatFunc";
    }

    StatusOr<Value> getResult() override;
};

static std::unordered_map<std::string,
std::function<std::shared_ptr<TimeBaseFunc>()>> timeFuncVec = {
        { "adddate", []() -> auto { return std::make_shared<AddDateFunc>();} },
        { "addtime", []() -> auto { return std::make_shared<AddTimeFunc>();} },
        { "subDate", []() -> auto { return std::make_shared<SubDateFunc>();} },
        { "subtime", []() -> auto { return std::make_shared<SubTimeFunc>();} },
        { "converttz", []() -> auto { return std::make_shared<ConvertTzFunc>();} },
        { "currentdate", []() -> auto { return std::make_shared<CurrentDateFunc>();} },
        { "currenttime", []() -> auto { return std::make_shared<CurrentTimeFunc>();} },
        { "year", []() -> auto { return std::make_shared<YearFunc>();} },
        { "month", []() -> auto { return std::make_shared<MonthFunc>();} },
        { "day", []() -> auto { return std::make_shared<DayFunc>();} },
        { "dayofmonth", []() -> auto { return std::make_shared<DayOfMonthFunc>();} },
        { "dayofweek", []() -> auto { return std::make_shared<DayOfWeekFunc>();} },
        { "dayofyear", []() -> auto { return std::make_shared<DayOfYearFunc>();} },
        { "hour", []() -> auto { return std::make_shared<HourFunc>();} },
        { "minute", []() -> auto { return std::make_shared<MinuteFunc>();} },
        { "sec", []() -> auto { return std::make_shared<SecondFunc>();} },
        { "timeformat", []() -> auto { return std::make_shared<TimeFormatFunc>();} },
        { "timetosec", []() -> auto { return std::make_shared<TimeToSecFunc>();} },
        { "utcdate", []() -> auto { return std::make_shared<UTCDateFunc>();} },
        { "utctime", []() -> auto { return std::make_shared<UTCTimeFunc>();} },
        { "utctimestamp", []() -> auto { return std::make_shared<UTCTimestampFunc>();} },
        { "unixtimestamp", []() -> auto { return std::make_shared<UnixTimestampFunc>();} },
        { "timestampformat", []() -> auto { return std::make_shared<TimestampFormatFunc>();}} };

}  // namespace time
}  // namespace nebula
#endif  //  UTIL_TIMEFUNCTION_H
