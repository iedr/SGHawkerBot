from datetime import datetime, timedelta


def get_date_today() -> datetime:
    datetime_today = datetime.today()
    year_today = datetime_today.year
    month_today = datetime_today.month
    day_today = datetime_today.day
    date_today = datetime(year=year_today, month=month_today, day=day_today)
    # date_today = datetime(year=2021, month=3, day=10)
    return date_today


def get_date_today_str(fmt="%d/%m/%Y") -> str:
    return get_date_today().strftime(fmt)


def get_date_range_in_weeks(start_week, end_week) -> (datetime, datetime):
    date_today = get_date_today()
    start_date = date_today + timedelta(weeks=start_week)
    end_date = date_today + timedelta(weeks=end_week)
    return start_date, end_date
