import datetime
import zoneinfo

import pytest
from dateutil import relativedelta
from django.conf import settings
from django.core import serializers

from tests.models import models
from xocto import localtime, ranges


pytestmark = pytest.mark.django_db


class TestFiniteDateRangeField:
    def test_roundtrip(self):
        finite_date_range = ranges.FiniteDateRange(
            start=datetime.date(2024, 1, 10), end=datetime.date(2024, 2, 9)
        )
        obj = models.FiniteDateRangeModel.objects.create(
            finite_date_range=finite_date_range
        )
        queried = models.FiniteDateRangeModel.objects.get(pk=obj.pk)
        assert queried.finite_date_range == finite_date_range

    def test_nullable(self):
        obj = models.FiniteDateRangeModel.objects.create(
            finite_date_range=ranges.FiniteDateRange(
                start=datetime.date(2024, 1, 10), end=datetime.date(2024, 2, 9)
            ),
            finite_date_range_nullable=None,
        )
        queried = models.FiniteDateRangeModel.objects.get(pk=obj.pk)
        assert queried.finite_date_range_nullable is None

    def test_query(self):
        finite_date_range = ranges.FiniteDateRange(
            start=datetime.date(2024, 1, 10), end=datetime.date(2024, 2, 9)
        )
        models.FiniteDateRangeModel.objects.create(finite_date_range=finite_date_range)
        assert models.FiniteDateRangeModel.objects.filter(
            finite_date_range=finite_date_range
        ).exists()
        assert models.FiniteDateRangeModel.objects.filter(
            finite_date_range__overlap=ranges.FiniteDateRange(
                start=datetime.date(2024, 1, 1), end=datetime.date(2024, 1, 15)
            )
        ).exists()
        assert models.FiniteDateRangeModel.objects.filter(
            finite_date_range__contains=ranges.FiniteDateRange(
                start=datetime.date(2024, 1, 11), end=datetime.date(2024, 1, 15)
            )
        ).exists()
        assert not models.FiniteDateRangeModel.objects.filter(
            finite_date_range__contains=ranges.FiniteDateRange(
                start=datetime.date(2024, 1, 5), end=datetime.date(2024, 1, 15)
            )
        ).exists()

    def test_query_single_date(self):
        finite_date_range = ranges.FiniteDateRange(
            start=datetime.date(2024, 1, 10), end=datetime.date(2024, 2, 9)
        )
        models.FiniteDateRangeModel.objects.create(finite_date_range=finite_date_range)
        assert models.FiniteDateRangeModel.objects.filter(
            finite_date_range__contains=finite_date_range.start
        ).exists()
        assert models.FiniteDateRangeModel.objects.filter(
            finite_date_range__contains=finite_date_range.end
        ).exists()
        assert not models.FiniteDateRangeModel.objects.filter(
            finite_date_range__contains=finite_date_range.start
            - relativedelta.relativedelta(days=1)
        ).exists()
        assert not models.FiniteDateRangeModel.objects.filter(
            finite_date_range__contains=finite_date_range.end
            + relativedelta.relativedelta(days=1)
        ).exists()

    def test_query_does_not_allow_tuple_values(self):
        with pytest.raises(
            TypeError,
            match="FiniteDateRangeField may only accept FiniteDateRange or date objects",
        ):
            models.FiniteDateRangeModel.objects.filter(
                finite_date_range=(
                    datetime.date(2024, 1, 10),
                    datetime.date(2024, 2, 9),
                )
            )
        with pytest.raises(
            TypeError,
            match="FiniteDateRangeField may only accept FiniteDateRange or date objects",
        ):
            models.FiniteDateRangeModel.objects.filter(
                finite_date_range__overlap=(
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 15),
                )
            )
        with pytest.raises(
            TypeError,
            match="FiniteDateRangeField may only accept FiniteDateRange or date objects",
        ):
            models.FiniteDateRangeModel.objects.filter(
                finite_date_range__contains=(
                    datetime.date(2024, 1, 11),
                    datetime.date(2024, 1, 15),
                )
            )

    def test_serialization(self):
        obj = models.FiniteDateRangeModel.objects.create(
            finite_date_range=ranges.FiniteDateRange(
                start=datetime.date(2024, 1, 10), end=datetime.date(2024, 2, 9)
            )
        )
        dumped = serializers.serialize("json", [obj])
        loaded = list(serializers.deserialize("json", dumped))
        loaded_obj = loaded[0].object
        assert obj == loaded_obj
        assert obj.finite_date_range == loaded_obj.finite_date_range
        assert obj.finite_date_range_nullable == loaded_obj.finite_date_range_nullable


class TestFiniteDateTimeRangeField:
    def test_roundtrip(self):
        finite_datetime_range = ranges.FiniteDatetimeRange(
            start=localtime.datetime(2024, 1, 10), end=localtime.datetime(2024, 2, 9)
        )
        obj = models.FiniteDateTimeRangeModel.objects.create(
            finite_datetime_range=finite_datetime_range
        )
        queried = models.FiniteDateTimeRangeModel.objects.get(pk=obj.pk)
        assert queried.finite_datetime_range == finite_datetime_range

    def test_nullable(self):
        obj = models.FiniteDateTimeRangeModel.objects.create(
            finite_datetime_range=ranges.FiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 10),
                end=localtime.datetime(2024, 2, 9),
            ),
            finite_datetime_range_nullable=None,
        )
        queried = models.FiniteDateTimeRangeModel.objects.get(pk=obj.pk)
        assert queried.finite_datetime_range_nullable is None

    def test_query(self):
        finite_datetime_range = ranges.FiniteDatetimeRange(
            start=localtime.datetime(2024, 1, 10), end=localtime.datetime(2024, 2, 9)
        )
        models.FiniteDateTimeRangeModel.objects.create(
            finite_datetime_range=finite_datetime_range
        )
        assert models.FiniteDateTimeRangeModel.objects.filter(
            finite_datetime_range=finite_datetime_range
        ).exists()
        assert models.FiniteDateTimeRangeModel.objects.filter(
            finite_datetime_range__overlap=ranges.FiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 1),
                end=localtime.datetime(2024, 1, 15),
            )
        ).exists()
        assert models.FiniteDateTimeRangeModel.objects.filter(
            finite_datetime_range__contains=ranges.FiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 11),
                end=localtime.datetime(2024, 1, 15),
            )
        ).exists()
        assert not models.FiniteDateTimeRangeModel.objects.filter(
            finite_datetime_range__contains=ranges.FiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 5),
                end=localtime.datetime(2024, 1, 15),
            )
        ).exists()

    def test_query_single_datetime(self):
        finite_datetime_range = ranges.FiniteDatetimeRange(
            start=localtime.datetime(2024, 1, 10), end=localtime.datetime(2024, 2, 9)
        )
        models.FiniteDateTimeRangeModel.objects.create(
            finite_datetime_range=finite_datetime_range
        )
        assert models.FiniteDateTimeRangeModel.objects.filter(
            finite_datetime_range__contains=finite_datetime_range.start
        ).exists()
        assert not models.FiniteDateTimeRangeModel.objects.filter(
            finite_datetime_range__contains=finite_datetime_range.end
        ).exists()
        assert not models.FiniteDateTimeRangeModel.objects.filter(
            finite_datetime_range__contains=finite_datetime_range.start
            - relativedelta.relativedelta(microseconds=1)
        ).exists()

    def test_query_does_not_allow_tuple_values(self):
        with pytest.raises(
            TypeError,
            match="FiniteDateTimeRangeField may only accept FiniteDatetimeRange or datetime objects",
        ):
            models.FiniteDateTimeRangeModel.objects.filter(
                finite_datetime_range=(
                    localtime.datetime(2024, 1, 10),
                    localtime.datetime(2024, 2, 9),
                )
            )
        with pytest.raises(
            TypeError,
            match="FiniteDateTimeRangeField may only accept FiniteDatetimeRange or datetime objects",
        ):
            models.FiniteDateTimeRangeModel.objects.filter(
                finite_datetime_range__overlap=(
                    localtime.datetime(2024, 1, 1),
                    localtime.datetime(2024, 1, 15),
                )
            )
        with pytest.raises(
            TypeError,
            match="FiniteDateTimeRangeField may only accept FiniteDatetimeRange or datetime objects",
        ):
            models.FiniteDateTimeRangeModel.objects.filter(
                finite_datetime_range__contains=(
                    localtime.datetime(2024, 1, 11),
                    localtime.datetime(2024, 1, 15),
                )
            )

    def test_serialization(self):
        obj = models.FiniteDateTimeRangeModel.objects.create(
            finite_datetime_range=ranges.FiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 10),
                end=localtime.datetime(2024, 2, 9),
            )
        )
        dumped = serializers.serialize("json", [obj])
        loaded = list(serializers.deserialize("json", dumped))
        loaded_obj = loaded[0].object
        assert obj == loaded_obj
        assert obj.finite_datetime_range == loaded_obj.finite_datetime_range
        assert (
            obj.finite_datetime_range_nullable
            == loaded_obj.finite_datetime_range_nullable
        )

    def test_timezone_conversions(self):
        """
        Timezones are converted correctly when round tripping.
        """
        TZ_UTC = zoneinfo.ZoneInfo("UTC")
        TZ_MELB = zoneinfo.ZoneInfo("Australia/Melbourne")
        TZ_DEFAULT = zoneinfo.ZoneInfo(settings.TIME_ZONE)

        finite_datetime_range_melb = ranges.FiniteDatetimeRange(
            start=datetime.datetime(2024, 1, 10, tzinfo=TZ_MELB),
            end=datetime.datetime(2024, 2, 9, tzinfo=TZ_MELB),
        )
        obj = models.FiniteDateTimeRangeModel.objects.create(
            finite_datetime_range=finite_datetime_range_melb,
            finite_datetime_range_utc=finite_datetime_range_melb,
        )
        finite_datetime_range_london = ranges.FiniteDatetimeRange(
            start=localtime.as_localtime(finite_datetime_range_melb.start),
            end=localtime.as_localtime(finite_datetime_range_melb.end),
        )
        finite_datetime_range_utc = ranges.FiniteDatetimeRange(
            start=localtime.as_utc(finite_datetime_range_melb.start),
            end=localtime.as_utc(finite_datetime_range_melb.end),
        )
        obj.refresh_from_db()
        assert (
            obj.finite_datetime_range
            == obj.finite_datetime_range_utc
            == finite_datetime_range_london
            == finite_datetime_range_melb
            == finite_datetime_range_utc
        )
        assert obj.finite_datetime_range.start.tzinfo == TZ_DEFAULT
        assert obj.finite_datetime_range.start.tzinfo != TZ_MELB
        assert obj.finite_datetime_range_utc.start.tzinfo == TZ_UTC
        assert obj.finite_datetime_range.start.tzinfo != TZ_MELB

    def test_timezone_conversions_and_dst_issue(self):
        TZ_UTC = zoneinfo.ZoneInfo("UTC")

        dst_missing_hour = ranges.FiniteDatetimeRange(
            start=datetime.datetime(2021, 10, 31, 0, tzinfo=TZ_UTC),
            end=datetime.datetime(2021, 10, 31, 1, tzinfo=TZ_UTC),
        )
        utc_obj = models.FiniteDateTimeRangeUTCModel.objects.create(
            finite_datetime_range=dst_missing_hour,
        )
        local_obj = models.FiniteDateTimeRangeModel.objects.create(
            finite_datetime_range=dst_missing_hour,
        )

        # No issue getting this object as the range is configurated as UTC
        utc_obj.refresh_from_db()

        # Unable to get this object because the datetime (stored as UTC) is converted to a DST timezone
        # and then both start and end == datetime.datetime(2021, 10, 31, 1,) raising a ValueError
        with pytest.raises(ValueError):
            local_obj.refresh_from_db()


class TestHalfFiniteDateTimeRangeField:
    def test_roundtrip(self):
        half_finite_datetime_range = ranges.HalfFiniteDatetimeRange(
            start=localtime.datetime(2024, 1, 10), end=None
        )
        obj = models.HalfFiniteDateTimeRangeModel.objects.create(
            half_finite_datetime_range=half_finite_datetime_range
        )
        queried = models.HalfFiniteDateTimeRangeModel.objects.get(pk=obj.pk)
        assert queried.half_finite_datetime_range == half_finite_datetime_range

    def test_nullable(self):
        obj = models.HalfFiniteDateTimeRangeModel.objects.create(
            half_finite_datetime_range=ranges.HalfFiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 10),
                end=None,
            ),
            half_finite_datetime_range_nullable=None,
        )
        queried = models.HalfFiniteDateTimeRangeModel.objects.get(pk=obj.pk)
        assert queried.half_finite_datetime_range_nullable is None

    def test_query(self):
        half_finite_datetime_range = ranges.HalfFiniteDatetimeRange(
            start=localtime.datetime(2024, 1, 10), end=None
        )
        models.HalfFiniteDateTimeRangeModel.objects.create(
            half_finite_datetime_range=half_finite_datetime_range
        )
        assert models.HalfFiniteDateTimeRangeModel.objects.filter(
            half_finite_datetime_range=half_finite_datetime_range
        ).exists()
        assert models.HalfFiniteDateTimeRangeModel.objects.filter(
            half_finite_datetime_range__overlap=ranges.HalfFiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 1),
                end=localtime.datetime(2024, 1, 15),
            )
        ).exists()
        assert models.HalfFiniteDateTimeRangeModel.objects.filter(
            half_finite_datetime_range__overlap=ranges.HalfFiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 1),
                end=None,
            )
        ).exists()
        assert models.HalfFiniteDateTimeRangeModel.objects.filter(
            half_finite_datetime_range__contains=ranges.HalfFiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 11),
                end=localtime.datetime(2024, 1, 15),
            )
        ).exists()
        assert models.HalfFiniteDateTimeRangeModel.objects.filter(
            half_finite_datetime_range__contains=ranges.HalfFiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 11),
                end=None,
            )
        ).exists()
        assert not models.HalfFiniteDateTimeRangeModel.objects.filter(
            half_finite_datetime_range__contains=ranges.HalfFiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 5),
                end=localtime.datetime(2024, 1, 15),
            )
        ).exists()
        assert not models.HalfFiniteDateTimeRangeModel.objects.filter(
            half_finite_datetime_range__contains=ranges.HalfFiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 5),
                end=None,
            )
        ).exists()

    def test_query_single_datetime(self):
        half_finite_datetime_range = ranges.HalfFiniteDatetimeRange(
            start=localtime.datetime(2024, 1, 10), end=None
        )
        models.HalfFiniteDateTimeRangeModel.objects.create(
            half_finite_datetime_range=half_finite_datetime_range
        )
        assert models.HalfFiniteDateTimeRangeModel.objects.filter(
            half_finite_datetime_range__contains=half_finite_datetime_range.start
        ).exists()
        assert not models.HalfFiniteDateTimeRangeModel.objects.filter(
            half_finite_datetime_range__contains=half_finite_datetime_range.start
            - relativedelta.relativedelta(microseconds=1)
        ).exists()

    def test_query_does_not_allow_tuple_values(self):
        with pytest.raises(
            TypeError,
            match="HalfFiniteDateTimeRangeField may only accept HalfFiniteDateTimeRangeField or datetime objects",
        ):
            models.HalfFiniteDateTimeRangeModel.objects.filter(
                half_finite_datetime_range=(
                    localtime.datetime(2024, 1, 10),
                    None,
                )
            )
        with pytest.raises(
            TypeError,
            match="HalfFiniteDateTimeRangeField may only accept HalfFiniteDateTimeRangeField or datetime objects",
        ):
            models.HalfFiniteDateTimeRangeModel.objects.filter(
                half_finite_datetime_range__overlap=(
                    localtime.datetime(2024, 1, 1),
                    localtime.datetime(2024, 1, 15),
                )
            )
        with pytest.raises(
            TypeError,
            match="HalfFiniteDateTimeRangeField may only accept HalfFiniteDateTimeRangeField or datetime objects",
        ):
            models.HalfFiniteDateTimeRangeModel.objects.filter(
                half_finite_datetime_range__overlap=(
                    localtime.datetime(2024, 1, 1),
                    None,
                )
            )
        with pytest.raises(
            TypeError,
            match="HalfFiniteDateTimeRangeField may only accept HalfFiniteDateTimeRangeField or datetime objects",
        ):
            models.HalfFiniteDateTimeRangeModel.objects.filter(
                half_finite_datetime_range__contains=(
                    localtime.datetime(2024, 1, 11),
                    localtime.datetime(2024, 1, 15),
                )
            )
        with pytest.raises(
            TypeError,
            match="HalfFiniteDateTimeRangeField may only accept HalfFiniteDateTimeRangeField or datetime objects",
        ):
            models.HalfFiniteDateTimeRangeModel.objects.filter(
                half_finite_datetime_range__contains=(
                    localtime.datetime(2024, 1, 11),
                    None,
                )
            )

    def test_serialization(self):
        obj = models.HalfFiniteDateTimeRangeModel.objects.create(
            half_finite_datetime_range=ranges.HalfFiniteDatetimeRange(
                start=localtime.datetime(2024, 1, 10),
                end=localtime.datetime(2024, 2, 9),
            )
        )
        dumped = serializers.serialize("json", [obj])
        loaded = list(serializers.deserialize("json", dumped))
        loaded_obj = loaded[0].object
        assert obj == loaded_obj
        assert obj.half_finite_datetime_range == loaded_obj.half_finite_datetime_range
        assert (
            obj.half_finite_datetime_range_nullable
            == loaded_obj.half_finite_datetime_range_nullable
        )

    def test_timezone_conversions(self):
        """
        Timezones are converted correctly when round tripping.
        """
        TZ_UTC = zoneinfo.ZoneInfo("UTC")
        TZ_MELB = zoneinfo.ZoneInfo("Australia/Melbourne")
        TZ_DEFAULT = zoneinfo.ZoneInfo(settings.TIME_ZONE)

        half_finite_datetime_range_melb = ranges.HalfFiniteDatetimeRange(
            start=datetime.datetime(2024, 1, 10, tzinfo=TZ_MELB),
            end=None,
        )
        obj = models.HalfFiniteDateTimeRangeModel.objects.create(
            half_finite_datetime_range=half_finite_datetime_range_melb,
            half_finite_datetime_range_utc=half_finite_datetime_range_melb,
        )
        half_finite_datetime_range_london = ranges.FiniteDatetimeRange(
            start=localtime.as_localtime(half_finite_datetime_range_melb.start),
            end=None,
        )
        half_finite_datetime_range_utc = ranges.FiniteDatetimeRange(
            start=localtime.as_utc(half_finite_datetime_range_melb.start),
            end=None,
        )
        obj.refresh_from_db()
        assert (
            obj.half_finite_datetime_range
            == obj.half_finite_datetime_range_utc
            == half_finite_datetime_range_london
            == half_finite_datetime_range_melb
            == half_finite_datetime_range_utc
        )
        assert obj.half_finite_datetime_range.start.tzinfo == TZ_DEFAULT
        assert obj.half_finite_datetime_range.start.tzinfo != TZ_MELB
        assert obj.half_finite_datetime_range_utc.start.tzinfo == TZ_UTC
        assert obj.half_finite_datetime_range_utc.start.tzinfo != TZ_MELB

    def test_timezone_conversions_and_dst_issue(self):
        TZ_UTC = zoneinfo.ZoneInfo("UTC")

        dst_missing_hour = ranges.HalfFiniteDatetimeRange(
            start=datetime.datetime(2021, 10, 31, 0, tzinfo=TZ_UTC),
            end=datetime.datetime(2021, 10, 31, 1, tzinfo=TZ_UTC),
        )
        utc_obj = models.HalfFiniteDateTimeRangeUTCModel.objects.create(
            half_finite_datetime_range=dst_missing_hour,
        )
        local_obj = models.HalfFiniteDateTimeRangeModel.objects.create(
            half_finite_datetime_range=dst_missing_hour,
        )

        # No issue getting this object as the range is configurated as UTC
        utc_obj.refresh_from_db()

        # Unable to get this object because the datetime (stored as UTC) is converted to a DST timezone
        # and then both start and end == datetime.datetime(2021, 10, 31, 1,) raising a ValueError
        with pytest.raises(ValueError):
            local_obj.refresh_from_db()
