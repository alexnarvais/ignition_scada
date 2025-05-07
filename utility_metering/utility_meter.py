# from abc import ABC, abstractmethod
import abc
import calendar
import math
from datetime import datetime, date
from collections import OrderedDict, defaultdict

# Base Class
class Meter(object):
    """Abstract Base Class Python 2.7

    Once Python 3.4 or greater is used then update and remove the following:

    1. uncomment the import statement (from abc import ABC, abstractmethod) and remove 'import abc'
    2. replace the arguement 'object' with 'ABC' in 'Meter(object)' class above
    3. remove __metasclass__
    4. replace every reference of '@abc.abstractmethod' with '@abstractmethod'
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        # Internal protected variables, intended for subclass use
        self._db = "Ignition_Main"
        self._db_mtr_tags_tbl = None
        self._db_mtr_monthly_tbl = None
        self._db_mtr_daily_tbl = None
        self._db_mtr_log_tbl = "metering_logs"
        self._excl_mtrs = []  # Put meter ids here that are decommissioned, inactive, or out of service.
        self._site_meter_map = {}
        self._to_email_list = ["site_automation_asset_monitoring@alcon.com"]

    def __str__(self):
        return "Base Meter Class"

    @abc.abstractmethod
    def get_meter_id_name(self):
        pass

    @abc.abstractmethod
    def calculate_usage(self):
        pass

    @abc.abstractmethod
    def get_monthly_usage(self):
        pass

    @abc.abstractmethod
    def get_monthly_site_usage(self):
        pass

    @abc.abstractmethod
    def get_raw_historical_data(self):
        pass

    @abc.abstractmethod
    def find_missing_data(self):
        pass

    @abc.abstractmethod
    def insert_missing_data(self):
        pass

    @abc.abstractmethod
    def daily_report(self):
        pass

    @abc.abstractmethod
    def monthly_report(self):
        pass

    # db property
    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, new_db):
        if not isinstance(new_db, str):
            raise ValueError("Database name must be a string")
        self._db = new_db

    # tags_table property
    @property
    def db_mtr_tags_tbl(self):
        return self._db_mtr_tags_tbl

    @db_mtr_tags_tbl.setter
    def db_mtr_tags_tbl(self, val):
        self._db_mtr_tags_tbl = val

    # monthly_table property
    @property
    def db_mtr_monthly_tbl(self):
        return self._db_mtr_monthly_tbl

    @db_mtr_monthly_tbl.setter
    def db_mtr_monthly_tbl(self, val):
        self._db_mtr_monthly_tbl = val

    # daily_table property
    @property
    def db_mtr_daily_tbl(self):
        return self._db_mtr_daily_tbl

    @db_mtr_daily_tbl.setter
    def db_mtr_daily_tbl(self, val):
        self._db_mtr_daily_tbl = val

    # log_table property
    @property
    def db_mtr_log_tbl(self):
        return self._db_mtr_log_tbl

    @db_mtr_log_tbl.setter
    def db_mtr_log_tbl(self, val):
        self._db_mtr_log_tbl = val

    # excl_mtrs property
    @property
    def excl_mtrs(self):
        return self._excl_mtrs

    @excl_mtrs.setter
    def excl_mtrs(self, new_excl_mtrs):
        if not isinstance(new_excl_mtrs, list):
            raise ValueError("Excluded meters must be a list")
        self._excl_mtrs = new_excl_mtrs

    # site_meter_map property
    @property
    def site_meter_map(self):
        return self._site_meter_map

    @site_meter_map.setter
    def site_meter_map(self, val):
        if not isinstance(val, dict):
            raise ValueError("Site meter mapping must be a dictionary")
        self._site_meter_map = val

    # to_email_list property
    @property
    def to_email_list(self):
        return self._to_email_list

    @to_email_list.setter
    def to_email_list(self, new_to_email_list):
        if not isinstance(new_to_email_list, list):
            raise ValueError("to_email_list must be a list of recipient(s)")
        self._to_email_list = new_to_email_list

    @staticmethod
    def dataset_row_len_check(header, ds):
        """Normalize dataset rows to match header length by padding with zeros.

        Validates and adjusts each row in the dataset to ensure it has the same length
        as the header. If a row is shorter than the header, it is padded with zeros
        (0.0) at the end until it matches the header length.
        """
        for row in ds:
            if len(row) != len(header):
                number_zeros_to_pad = len(header) - len(row)
                row.extend([0.0] * number_zeros_to_pad)
        return ds

    @staticmethod
    def calculate_usage_helper(*data):
        end_date_smpls = data[0]
        # In a normal situation you would return the same {key:value} pair for 'end_date_max_smpl_by_time' and 'end_date_max_smpl_by_val'
        # which would validate that the largest usage is the last sample of the day.
        end_date_max_smpl_by_time = max(end_date_smpls, key=lambda x: x["time"])
        end_date_max_smpl_by_val = max(end_date_smpls, key=lambda x: x["usage"])
        end_date_min_smpl_by_time = min(end_date_smpls, key=lambda x: x["time"])
        end_date_min_smpl_by_val = min(end_date_smpls, key=lambda x: x["usage"])
        end_date_last_smpl_is_max = end_date_max_smpl_by_time == end_date_max_smpl_by_val
        end_date_first_smpl_is_min = end_date_min_smpl_by_time == end_date_min_smpl_by_val

        # The 'multi_rlovrs' list holds values where multi-rollovers happen, which is everywhere that (element+1 < element).
        # Example: [100, 200, 300, 10, 20, 30, 1, 2, 3] = [30] = two rollovers happend at index 3 (10<300) and 6 (1<3).
        # Only the second rollover value will be added to the multi_rlovrs list, the first rollover value
        # will be used in finding the partial usage from the meters largest value.
        multi_rlovrs = [crnt for crnt, nxt in zip(end_date_smpls, end_date_smpls[1:]) if
                        nxt["usage"] < crnt["usage"] and nxt["usage"] != end_date_max_smpl_by_val["usage"]]
        # Find largest value the meter can pssoble hold by converting from float>int>str then counting the number characters in the string.
        len_end_date_lgst_val = len(str(int(end_date_max_smpl_by_val["usage"])))
        # Take the length of the largest value and multiply by 9 then convert back to int.
        mtr_lgst_val = int("9" * len_end_date_lgst_val)
        usage = 0
        # If the length of the data is 1 then only the end_date samples are looked at.
        if len(data) == 1:
            if end_date_last_smpl_is_max:
                usage = end_date_max_smpl_by_val["usage"] - end_date_min_smpl_by_val["usage"]
            else:
                # A rollover exist if this code is reached.
                # The first sample of the end_date is used in calculating the partial usage before the rollover
                # because the min sample will be the smallest value after the rollover.
                rlovr_usage = mtr_lgst_val - end_date_max_smpl_by_val["usage"]
                usage = rlovr_usage + (end_date_max_smpl_by_val["usage"] - end_date_smpls[0]["usage"]) + sum(
                    multi_rlovrs) + end_date_smpls[-1]["usage"]
        # If the length of the data=2 then start_date and end_date samples can be compared.
        elif len(data) == 2:
            start_date_smpls = data[1]

            start_date_max_smpl_by_time = max(start_date_smpls, key=lambda x: x["time"])
            start_date_max_smpl_by_val = max(start_date_smpls, key=lambda x: x["usage"])
            start_date_min_smpl_by_time = min(start_date_smpls, key=lambda x: x["time"])
            start_date_min_smpl_by_val = min(start_date_smpls, key=lambda x: x["usage"])
            start_date_last_smpl_is_max = start_date_max_smpl_by_time == start_date_max_smpl_by_val
            start_date_first_smpl_is_min = start_date_min_smpl_by_time == start_date_min_smpl_by_val

            # If no conditions here are met then usage=0
            if end_date_last_smpl_is_max and start_date_last_smpl_is_max and end_date_max_smpl_by_val["usage"] > \
                    start_date_max_smpl_by_val["usage"]:
                usage = end_date_max_smpl_by_val["usage"] - start_date_max_smpl_by_val["usage"]
            elif end_date_last_smpl_is_max and start_date_last_smpl_is_max and end_date_max_smpl_by_val["usage"] < \
                    start_date_max_smpl_by_val[
                        "usage"] or end_date_last_smpl_is_max and not start_date_last_smpl_is_max:
                usage = end_date_max_smpl_by_val["usage"] - end_date_min_smpl_by_val["usage"]
            elif not end_date_last_smpl_is_max and start_date_last_smpl_is_max and end_date_max_smpl_by_val["usage"] > \
                    start_date_max_smpl_by_val["usage"]:
                # A rollover exist if this code is reached.
                # The sum of the mulit-rollovers list is always added to the usage summation because when the list is empty (indicating no rollovers)
                # then zero will be returned, no exception will be raised.

                # Safety check to make sure that the end_date largest value was within range
                # of the possible max number the meter can hold, this will help with better
                # accuracy of detecting a true rollover.
                if len_end_date_lgst_val >= len("999999999"):
                    rlovr_usage = mtr_lgst_val - end_date_max_smpl_by_val["usage"]
                else:
                    rlovr_usage = 0
                usage = rlovr_usage + (end_date_max_smpl_by_val["usage"] - start_date_max_smpl_by_val["usage"]) + sum(
                    multi_rlovrs) + end_date_smpls[-1]["usage"]
            elif not end_date_last_smpl_is_max and not start_date_last_smpl_is_max:
                # A rollover exist if this code is reached.
                # The first sample of the end_date is used in calculating the partial usage before the rollover
                # because the min sample will be the smallest value after the rollover.
                rlovr_usage = mtr_lgst_val - end_date_max_smpl_by_val["usage"]
                usage = rlovr_usage + (end_date_max_smpl_by_val["usage"] - end_date_smpls[0]["usage"]) + sum(
                    multi_rlovrs) + end_date_smpls[-1]["usage"]

        return usage

    def log_message(self, gtwy_addr, logger_name, log_msg, log_time, level="info"):
        """Helper to log a message in the logger table."""

        logger = system.util.getLogger(logger_name)

        # Ignition logging
        level = level.lower()
        if level == "info":
            logger.info(log_msg)
        elif level == "error":
            logger.error(log_msg)
        else:
            logger.warn(log_msg)

        # Database logging
        insert_mtr_logs_query = "INSERT INTO {0} (server, logger, log, time_stamp) VALUES (?, ?, ?, ?)".format(
            self.db_mtr_log_tbl)
        args = [gtwy_addr, logger_name, log_msg, system.date.toMillis(log_time)]
        system.db.runPrepUpdate(query=insert_mtr_logs_query, args=args, database=self.db)


class Electrical(Meter):
    def __init__(self):
        # Python 3.x and greater syntax for superclass constructor.
        # super().__init__()
        # Python 2.7 syntax for superclass constructor.
        super(Electrical, self).__init__()
        self.db_mtr_tags_tbl = "electrical_usage_tags"
        self.db_mtr_daily_tbl = "electrical_daily_usage"
        self.db_mtr_monthly_tbl = "electrical_monthly_usage"
        self.excl_mtrs = [7, 8]  # Put meter ids here that are decommissioned, inactive, or out of service.
        self.site_meter_map = {
            1000: {'site_name': 'PLANT_1', 'site_meter_ids': [1, 2, 3]},
            1001: {'site_name': 'PLANT_2', 'site_meter_ids': [4, 5, 6]},
            1002: {'site_name': 'PLANT_3', 'site_meter_ids': [9, 10, 11, 12]},
            1003: {'site_name': 'PLANT_4', 'site_meter_ids': [13, 14, 15, 16]},
            1004: {'site_name': 'PUMP_HOUSE', 'site_meter_ids': [17]},
            1005: {'site_name': 'QA', 'site_meter_ids': [18]},
            1006: {'site_name': 'PD', 'site_meter_ids': [19, 20]},
            1007: {'site_name': 'MFG', 'site_meter_ids': [21, 22, 23, 24]},
            1008: {'site_name': 'ADMIN', 'site_meter_ids': [25]},
            1009: {'site_name': 'CONNER', 'site_meter_ids': [26, 27, 28]},
            1010: {'site_name': 'DIST', 'site_meter_ids': [29]},
            1011: {'site_name': 'CAFE_OLD', 'site_meter_ids': [30]},
            1012: {'site_name': 'CAFE_NEW', 'site_meter_ids': [31]},
            1013: {'site_name': 'ASPEX', 'site_meter_ids': [32, 33, 34]},
            1014: {'site_name': 'WDC', 'site_meter_ids': [35, 36]},
            1015: {'site_name': 'TOWER_A', 'site_meter_ids': [37]},
            1016: {'site_name': 'TOWER_B', 'site_meter_ids': [38]},
            1017: {'site_name': 'TOWER_C', 'site_meter_ids': [41]},
            1018: {'site_name': 'YG', 'site_meter_ids': [42]},
            1019: {'site_name': 'SBSTN', 'site_meter_ids': [39, 40]}
        }
        # Site id of the substation meters, this is used as the reference to calculate each sites total usage.
        # Private to the Electrical subclass
        self.__site_ref_id = 1019

    def __str__(self):
        return "Electrical Meter Class"

    @property
    def site_ref_id(self):
        # Getter for the site reference id.
        return self.__site_ref_id

    @site_ref_id.setter
    def site_ref_id(self, new_site_ref_id):
        # Setter for the site reference id.
        if not isinstance(new_site_ref_id, int):
            raise ValueError("Site reference id must be an int")
        self.__site_ref_id = new_site_ref_id

    def get_meter_id_name(self):
        # A comma separated string of excluded meter ids.
        excl_mtrs_str = ", ".join([str(mtr) for mtr in self.excl_mtrs])
        query = "SELECT tag_id, meter_name FROM {0} WHERE tag_id NOT IN ({1})".format(self.db_mtr_tags_tbl,
                                                                                      excl_mtrs_str)
        mtr_id_ds = system.db.runQuery(query=query, database=self.db)

        # Return a list of meter ids.
        meter_id_list = mtr_id_ds.getColumnAsList(0)

        # Create a {meter id: meter name} dictionary for all active meters
        meter_id_name_dict = {meter_id_list[i]: mtr_id_ds.getValueAt(i, 1) for i in range(mtr_id_ds.getRowCount())}
        # Add the site meter map
        meter_id_name_dict.update(self.site_meter_map)

        return meter_id_name_dict

    def calculate_usage(self, tag_providers, start_date=None, end_date=None, activate_insert_query=False):
        # tag_provider_exist = ""
        if tag_providers:
            tag_providers_str = ", ".join(["'{0}'".format(provider) for provider in tag_providers])
            tags_in_prvdr_query = "SELECT tag_id, tag_path FROM {0} WHERE tag_provider in ({1}) and meter_active".format(
                self.db_mtr_tags_tbl, tag_providers_str)
        else:
            raise TagProviderError("At least one tag provider needs to be provided.")

        record_exist_query = "SELECT count(tag_id) FROM {0} WHERE tag_id = ? AND t_stamp = ?".format(
            self.db_mtr_daily_tbl)
        insert_usage_query = "INSERT INTO {0} (tag_id, t_stamp, daily_usage) VALUES (?, ?, ?)".format(
            self.db_mtr_daily_tbl)

        # Returns a Pydataset of the tag_id and tag_path
        pyds_tags_in_prvdr = system.db.runQuery(query=tags_in_prvdr_query, database=self.db)

        # Month from 0 - 11 and day from 1 - 31
        #
        # The start and end date need to be provided else the defaults are:
        #	start_date = the day before yesterday
        #	end_date = yesterday
        #
        if not start_date or not end_date:
            start_date = system.date.addDays(system.date.midnight(system.date.now()), -2)
            end_date = system.date.addMillis(system.date.midnight(system.date.now()), -1)

        daily_usage_dict = {}

        for tag in pyds_tags_in_prvdr:
            tag_id = tag[0]
            # meter name (v) will be mapped to the tag_id (k) on the first occurance
            meter_name = next((v for k, v in self.get_meter_id_name().items() if k == tag_id))
            tag_path = tag[1]
            pyds_tag_hist = system.dataset.toPyDataSet(
                system.tag.queryTagHistory([tag_path], startDate=start_date, endDate=end_date))

            # Organize samples by date & time, checking for duplicate (time_only, tag_value) combinations
            usage_unique_dict = {}
            usage_time_dict = {}
            for hist_data in pyds_tag_hist:
                timestamp = hist_data[0]
                tag_value = hist_data[1]
                date_only = system.date.midnight(timestamp)
                time_only = system.date.format(timestamp, "HH:mm:ss")

                # Check for values that are not None or 0
                if tag_value:
                    usage_unique_dict.setdefault(date_only, set()).add((time_only, tag_value))

            # Reconstruct the data in named key:value pairs and sorted by time.
            #			usage_time_dict = {date_only: [{"time": time, "usage": value} for time, value in sorted(time_val_set, key=lambda x: x[0])] for date_only, time_val_set in usage_unique_dict.items()}

            # Explicitly remove duplicates by usage, picking the usage with the latest time.
            for date_only, time_usage_set in usage_unique_dict.items():
                temp_usage_time_dict = {}
                # Explicitly iterate the set and use time comparison directly.
                for t, u in time_usage_set:
                    if (u not in temp_usage_time_dict) or (t > temp_usage_time_dict[u]):
                        temp_usage_time_dict[u] = t

                # Reconstruct into sorted dict by the timestamp
                usage_time_dict[date_only] = sorted(
                    [{"time": t, "usage": u} for u, t in temp_usage_time_dict.items()],
                    key=lambda x: x["time"]
                )

            # Get all the samples for yesterday and the day before yesterday or return an empty list.
            start_date_smpls = usage_time_dict.get(start_date, [])
            end_date_smpls = usage_time_dict.get(system.date.midnight(end_date), [])

            # The static helper function (calculate_usage_helper) will assist
            # with providing the usage between the start and end date.
            if end_date_smpls and start_date_smpls:
                # Order matters here, set the end date samples as the first argument in function.
                total_usage = self.calculate_usage_helper(end_date_smpls, start_date_smpls)
            elif end_date_smpls and not start_date_smpls:
                total_usage = self.calculate_usage_helper(end_date_smpls)
            else:
                total_usage = 0

            daily_usage_dict[tag_id] = {
                "meter_name": str(meter_name),
                "timestamp_millis": system.date.toMillis(system.date.midnight(end_date)),
                "timestamp_date": system.date.midnight(end_date),
                "usage": total_usage
            }

        for tag_id, meta_data in daily_usage_dict.items():
            meter_name = meta_data["meter_name"]
            timestamp_millis = meta_data["timestamp_millis"]
            timestamp_date = meta_data["timestamp_date"]
            kwh_usage = meta_data["usage"]

            # Safe check before inserting record(s) into table
            record_exist = \
            system.db.runPrepQuery(query=record_exist_query, args=[tag_id, timestamp_millis], database=self.db)[0][0]
            if not record_exist:
                if activate_insert_query:
                    system.db.runPrepUpdate(query=insert_usage_query, args=[tag_id, timestamp_millis, kwh_usage],
                                            database=self.db)
                else:
                    print(
                        "The following records(s) will not be inserted into the db table until 'calculate_usage(activate_insert_query=True)'!")
                print(
                    "insert record [tag_id={0}, meter_name={1}, timestamp_date={2}, timestamp_millis={3}, usage={4}]\n".format(
                        tag_id, meter_name, timestamp_date, timestamp_millis, kwh_usage))
            else:
                print(
                    "The record exist [tag_id={0}, meter_name={1}, timestamp_date={2}, timestamp_millis={3}, usage={4}]\n".format(
                        tag_id, meter_name, timestamp_date, timestamp_millis, kwh_usage))

        return daily_usage_dict

    def get_monthly_usage(self, month_year=None, time_span_months=18):

        meter_id_name_dict = self.get_meter_id_name()

        # Ignore the site meter ids/keys
        meter_ids = [mtr_id for mtr_id in meter_id_name_dict if mtr_id not in self.site_meter_map.keys()]
        meter_ids_str = ", ".join([str(i) for i in meter_ids])

        # Set end_date based on month_year or current date
        if month_year:
            end_date = month_year
        else:
            end_date = system.date.format(system.date.now(), "MMM-yyyy")

        if time_span_months > 0:
            start_date = system.date.format(
                system.date.addMonths(system.date.parse(end_date, "MMM-yyyy"), -time_span_months), "MMM-yyyy")
            query = """
				SELECT * FROM {0} WHERE STR_TO_DATE(date, '%b-%Y') >= STR_TO_DATE(?, '%b-%Y') AND 
				STR_TO_DATE(date, '%b-%Y') <= STR_TO_DATE(?, '%b-%Y') AND tag_id IN ({1})
			""".format(self.db_mtr_monthly_tbl, meter_ids_str)
            args = [start_date, end_date]
        # If time_span_months is zero or negative, query just one month
        else:
            query = "SELECT * FROM {0} WHERE date = ? AND tag_id IN ({1})".format(self.db_mtr_monthly_tbl,
                                                                                  meter_ids_str)
            args = [end_date]

        monthly_usage_pyds = system.db.runPrepQuery(query=query, args=args, database=self.db)
        monthly_usage_dict = {}

        # Add the meter names to the monthly_usage
        for row in monthly_usage_pyds:
            meter_id = row[0]
            meter_name = meter_id_name_dict.get(meter_id)
            date = row[1]
            usage = row[2]
            meter_entry = monthly_usage_dict.setdefault(meter_id, {"meter_name": meter_name, "meter_usage": []})
            meter_entry["meter_usage"].append({"date": date, "usage_kwh": usage})

        # Sort by date
        for mtr_entry in monthly_usage_dict.values():
            mtr_entry["meter_usage"].sort(key=lambda x: datetime.strptime(x["date"], '%b-%Y'))

        return monthly_usage_dict

    def get_monthly_site_usage(self, monthly_usage):
        site_usage_dict = {}

        for site_id, site_meta in self.site_meter_map.items():
            site_meter_ids = site_meta["site_meter_ids"]
            site_name = site_meta["site_name"]
            site_entry = site_usage_dict.setdefault(site_id, {
                "site_name": site_name,
                "site_meter_ids": site_meter_ids,
                "site_usage": []
            })

            usage_by_date = {}  # Temporary dict to help in aggregation
            for meter_id in site_meter_ids:
                if meter_id in monthly_usage.keys():
                    meter_name = monthly_usage[meter_id]["meter_name"]
                    meter_usage = monthly_usage[meter_id]["meter_usage"]
                    for i in meter_usage:
                        date = i["date"]
                        usage_kwh = i["usage_kwh"]
                        if date not in usage_by_date:
                            usage_by_date[date] = {"usage_kwh": 0.0, "meter_usage": []}

                        usage_by_date[date]["usage_kwh"] += usage_kwh

                        usage_by_date[date]["meter_usage"].append({
                            "meter_id": meter_id,
                            "meter_name": meter_name,
                            "usage_kwh": usage_kwh
                        })

            for date, usage_meta in usage_by_date.items():
                site_entry["site_usage"].append({
                    "date": date,
                    "usage_kwh": usage_meta["usage_kwh"],
                    "usage_pct": 0.0,
                    "meter_usage": usage_meta["meter_usage"]

                })

            site_usage_dict[site_id] = site_entry

        ref_usage_by_date = {i["date"]: i["usage_kwh"] for i in
                             site_usage_dict.get(self.site_ref_id, {}).get("site_usage", [])}

        # This where we calculate the total in percent the site used with respect to the substation total
        for site_id, site_meta in site_usage_dict.items():
            for i in site_meta["site_usage"]:
                if site_id == self.site_ref_id:
                    i["usage_pct"] = -1
                else:
                    date = i["date"]
                    usage_kwh = i["usage_kwh"]
                    i["usage_pct"] = round((usage_kwh / ref_usage_by_date.get(date) * 100), 1) if ref_usage_by_date.get(
                        date) > 0 else 0.0

        return site_usage_dict

    def find_missing_data(self, tag_ids=[1], month_year=None):

        if not month_year:
            month_year = system.date.format(system.date.now(), "MMM-yyyy")

        if not tag_ids:
            raise ValueError("tag_ids list cannot be empty")

        parsed_date = datetime.strptime(month_year, "%b-%Y")

        month = parsed_date.month
        year = parsed_date.year
        days_in_month = calendar.monthrange(year, month)[1]
        start_date = system.date.getDate(year, month - 1, 1)
        end_date = system.date.getDate(year, month - 1, days_in_month)
        date_iter = start_date
        missing_days = []
        zero_usage_days = []
        abnormal_usage_days = []
        recorded_usage_days = []

        tag_ids_str = ", ".join([str(i) for i in tag_ids])

        month_year_query = """
		             	   SELECT * FROM {0} WHERE tag_id IN ({1}) AND 
		             	   DATE_FORMAT(FROM_UNIXTIME(t_stamp / 1000), '%b-%Y') = '{2}'
					 	   """.format(self.db_mtr_daily_tbl, tag_ids_str, month_year)

        # The pydataset returned for all the days in the selected month and year
        pyds_month_year = system.db.runQuery(query=month_year_query, database=self.db)

        meter_prvdr_query = """
							SELECT tag_id, meter_name, tag_provider FROM {0} WHERE tag_id in ({1}) and meter_active
							""".format(self.db_mtr_tags_tbl, tag_ids_str)
        pyds_meter_prvdr = system.db.runQuery(query=meter_prvdr_query, database=self.db)

        for i in pyds_meter_prvdr:
            recorded_usage_days.append(
                "The following is all the electrical usage recorded during {0} for [meter_id: {1}, meter_name: {2}]:".format(
                    month_year, i[0], i[1]))
            for j in pyds_month_year:
                if j[0] == i[0]:
                    recorded_usage_days.append([j[0], j[1], system.date.fromMillis(j[2])])

        daily_usage_by_date = {system.date.fromMillis(i[2]): i[1] for i in system.dataset.toPyDataSet(pyds_month_year)}

        if daily_usage_by_date.values():
            mean_usage = sum(daily_usage_by_date.values()) / float(len(daily_usage_by_date.values()))
            variance = sum((val - mean_usage) ** 2 for val in daily_usage_by_date.values()) / float(
                len(daily_usage_by_date.values()))
            std_usage = math.sqrt(variance)
            lower_threshold = mean_usage - 2 * std_usage
            upper_threshold = mean_usage + 2 * std_usage
        else:
            lower_threshold = upper_threshold = None

        # Find missing days in the Month:
        while date_iter <= end_date:
            if date_iter not in daily_usage_by_date.keys():
                missing_days.append(date_iter)
            else:
                usage = daily_usage_by_date[date_iter]
                if usage == 0:
                    zero_usage_days.append({"date": date_iter, "usage": usage})

                if lower_threshold is not None and (usage < lower_threshold or usage > upper_threshold):
                    abnormal_usage_days.append({
                        "date": date_iter,
                        "usage": usage,
                        "reason": "usage outside {:.2f} to {:.2f}".format(lower_threshold, upper_threshold)
                    })

            date_iter = system.date.addDays(date_iter, 1)

        return {
            "recorded_usage_days": recorded_usage_days,
            "missing_days": missing_days,
            "zero_usage_days": zero_usage_days,
            "abnormal_usage_days": abnormal_usage_days
        }

    def insert_missing_data(self, start_date, end_date, tag_id_usage={}, activate_insert_query=False):

        start_date = system.date.midnight(start_date)
        end_date = system.date.midnight(end_date)

        if not tag_id_usage:
            print("A {tag_id:usage} map needs to be created using parameter 'tag_id_usage', it can't be null")
            return

        tag_ids_str = ", ".join([str(i) for i in tag_id_usage.keys()])
        get_tag_path_query = "SELECT tag_path FROM {0} WHERE tag_id in ({1})".format(self.db_mtr_tags_tbl, tag_ids_str)
        # tag_path = system.db.runQuery(get_tag_path_query)[0][0]
        # print("Tag Id:{0} maps to Tag Path:{1}\n".format(tag_id, tag_path))

        insert_query = "INSERT INTO {0} (tag_id, daily_usage, t_stamp) VALUES (?, ?, ?)".format(self.db_mtr_daily_tbl)
        update_query = "UPDATE {0} SET daily_usage = ? WHERE t_stamp = ? and tag_id = ?".format(self.db_mtr_daily_tbl)
        # The amount of days between the start date + 1 and end date
        days_between = system.date.daysBetween(start_date, end_date)
        # print("The number of days between '{0}' and '{1}' is: {2}\n".format(start_date, end_date, days_between))

        # Safe check before inserting record(s) into table
        if not activate_insert_query: print(
            "The record(s) will not be inserted into the db table until 'insert_missing_data(activate_insert_query=True)'!")
        for tag_id, usage in tag_id_usage.items():
            daily_usage = round(float(usage) / days_between, 1) if usage else 0
            for i in range(days_between):
                date = system.date.addDays(end_date, -i)
                date_millis = system.date.toMillis(date)
                if activate_insert_query:
                    try:
                        system.db.runPrepUpdate(query=insert_query, args=[tag_id, daily_usage, date_millis],
                                                database=self.db)
                    except:
                        system.db.runPrepUpdate(query=update_query, args=[daily_usage, date_millis, tag_id],
                                                database=self.db)
                else:
                    print(tag_id, daily_usage, date)

    def get_raw_historical_data(self, tag_ids=[1], start_date=None, end_date=None):
        tag_ids_str = ", ".join([str(i) for i in tag_ids])
        query = "SELECT tag_path FROM {0} WHERE tag_id in ({1})".format(self.db_mtr_tags_tbl, tag_ids_str)
        tag_path = system.db.runQuery(query)[0][0]
        # print("Retreiving raw historical data for [tag_id:{0}, tag_path:{1}]\n".format(tag_ids, tag_path))

        if not start_date or not end_date:
            end_date = system.date.addMillis(
                system.date.getDate(system.date.getYear(system.date.now()), system.date.getMonth(system.date.now()), 1),
                -1)
            start_date = system.date.addMonths(end_date, -1)

        tag_hist_data = system.tag.queryTagHistory(paths=[tag_path], startDate=start_date, endDate=end_date)

        # Returns a ignition basic dataset, use the system.dataset.toXXX() functions to convert to another type of dataset such as a PyDataSet
        return tag_hist_data

    def daily_report(self, send_email=True):

        tstamp_date = system.date.addDays(system.date.midnight(system.date.now()), -1)
        tstamp_millis = system.date.toMillis(tstamp_date)
        tstamp_date_format = system.date.format(tstamp_date, "d-MMM-yyyy")

        usage_query = "SELECT tag_id, daily_usage FROM {0} WHERE t_stamp = {1}".format(self.db_mtr_daily_tbl,
                                                                                       tstamp_millis)
        comms_cmnts_query = "SELECT tag_id, comm_tag_path, comments FROM {0} WHERE meter_active".format(
            self.db_mtr_tags_tbl, tstamp_millis)
        loggers = ", ".join(["'{0}'".format(i) for i in ["electrical_daily_usage", "electrical_usage_email"]])
        mtr_logs_query = """
			SELECT * FROM {0} WHERE DATE(FROM_UNIXTIME(time_stamp/1000)) = STR_TO_DATE('{1}', '%Y-%m-%d') AND
			logger in ({2})
		""".format(self.db_mtr_log_tbl, date.today(), loggers)

        meter_id_name_dict = self.get_meter_id_name()
        # Ignore the site meter ids/keys
        meter_ids = [mtr_id for mtr_id in meter_id_name_dict if mtr_id not in self.site_meter_map.keys()]

        usage_ds = system.db.runQuery(usage_query, database=self.db)
        comms_cmnts_ds = system.db.runQuery(comms_cmnts_query, database=self.db)
        mtr_logs_ds = system.db.runQuery(mtr_logs_query, database=self.db)

        # Convert usage_lkup and comms_cmnts_lkup into an efficient lookup dictionary
        usage_lkup = {row[0]: row[1] for row in usage_ds}
        comms_cmnts_lkup = {row[0]: {"comm_status": row[1], "comment": row[2]} for row in comms_cmnts_ds}

        html_base = """
			<!DOCTYPE html>
			<html lang="en">
			<head>
				<meta charset="utf-8">
				<title>Electrical Daily Usage and Logs</title>
				<style>
					table {{ border-color: white ;border-collapse: collapse; }}
					th, td {{ border: 1px solid #555555; padding: 8px; }}
				</style>
			</head>
			<body>
			<h2>Daily Usage</h2>
			<div id="daily_usage">
				<table border="1">
					<tr align="center">
						<th>Meter Id</th>
						<th>Meter Name</th>
						<th>Communication Status</th>
						<th>Usage (kWh)</th>
						<th>Timestamp Date</th>
						<th>Timestamp Millis</th>
						<th>Comments</th>
					</tr>
					{usage_data}
				</table>
			</div>
			<h2>Daily Logs</h2>
			<div id="script_log">
				<table border="1">
					<tr align="center">
						<th>Gateway</th>
						<th>Logger</th>
						<th>Message</th>
						<th>Timestamp</th>
					</tr>
					{log_data}
				</table>
			</div>
			</body>
			</html>
	 	"""

        usage_rows = []

        for mtr_id in meter_ids:
            mtr_name = meter_id_name_dict.get(mtr_id, "Unknown Meter")
            comment = comms_cmnts_lkup[mtr_id]["comment"]
            comm_status = "Online" if system.tag.readBlocking([comms_cmnts_lkup[mtr_id]["comm_status"]])[
                0].value else "Offline"
            comm_style = ' style="color: {0};"'.format("green" if comm_status == "Online" else "red")
            if mtr_id in usage_lkup.keys():
                usage = usage_lkup[mtr_id]
                usage_style = ' style="color: {0};"'.format("" if usage else "red")

            else:
                usage = "No Usage Reported"
                usage_style = comm_style = ' style="color: red;"'

            row_data = """
    			<tr align="center">
					<td>{mtr_id}</td>
					<td>{mtr_name}</td>
					<td{comm_style}>{comm_status}</td>
					<td{usage_style}>{usage}</td>
					<td>{date}</td>
					<td>{millis}</td>
					<td>{comment}</td>
				</tr>
			""".format(
                mtr_id=mtr_id,
                mtr_name=mtr_name,
                comm_status=comm_status,
                usage=usage,
                comm_style=comm_style,
                usage_style=usage_style,
                date=tstamp_date_format,
                millis=tstamp_millis,
                comment=comment
            )

            usage_rows.append(row_data)

        log_rows = []

        for row in mtr_logs_ds:
            gateway = row[1]
            logger = row[2]
            msg = row[3]
            timestamp = system.date.fromMillis(row[4])

            row_data = """
				<tr align="center">
					<td>{gateway}</td>
					<td>{logger}</td>
					<td>{msg}</td>
					<td>{tstamp}</td>
				</tr>
			""".format(
                gateway=gateway,
                logger=logger,
                msg=msg,
                tstamp=timestamp
            )

            log_rows.append(row_data)

        full_html_report = html_base.format(
            usage_data="".join(usage_rows),
            log_data="".join(log_rows)
        )

        if send_email:
            alcon.email.send_email(
                to_list=self.to_email_list,
                subject="Electrical Usage and Logs {0}".format(tstamp_date_format),
                body=full_html_report,
                html=True,
                from_addr="data-service@siteautomation.net",
                attachment_names=None,
                attachment_data=None
            )

        return full_html_report

    def monthly_report(self, report_path="Electrical/Electrical Usage", month_year=None, time_span_months=18):
        # For sites with a single meter
        #
        # Format: Creates a list of [date, usage] pairs for each usage entry where:
        # - usage_entry["date"] gives the month (e.g., "Jan-2025")
        # - usage_entry["usage_kton"] gives the total usage in kilotons
        # Example output: [["Jan-2025", 150.5], ["Feb-2025", 140.2], ...]

        # Set the month year to the current month year if there's not one selected.
        if not month_year:
            month_year = system.date.format(system.date.now(), "MMM-yyy")

        if time_span_months <= 0:
            time_span_months = 18

        monthly_usage = self.get_monthly_usage(month_year=month_year, time_span_months=time_span_months)
        monthly_site_usage = self.get_monthly_site_usage(monthly_usage)

        sbstn_site_data = monthly_site_usage.get(1019, {})
        sbstn_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"]] for usage_entry in sbstn_site_data["site_usage"]]
        sbstn_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        sbstn_headers = ['Month'] + [meter["meter_name"] for meter in
                                     sbstn_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh']
        sbstn_ds = system.dataset.toDataSet(sbstn_headers, sbstn_usage)

        plant1_site_data = monthly_site_usage.get(1000, {})
        plant1_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in plant1_site_data["site_usage"]]
        plant1_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant1_headers = ['Month'] + [meter["meter_name"] for meter in
                                      plant1_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        plant1_ds = system.dataset.toDataSet(plant1_headers, plant1_usage)

        plant2_site_data = monthly_site_usage.get(1001, {})
        plant2_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in plant2_site_data["site_usage"]]
        plant2_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant2_headers = ['Month'] + [meter["meter_name"] for meter in
                                      plant2_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        plant2_ds = system.dataset.toDataSet(plant2_headers, plant2_usage)

        plant3_site_data = monthly_site_usage.get(1002, {})
        plant3_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in plant3_site_data["site_usage"]]
        plant3_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant3_headers = ['Month'] + [meter["meter_name"] for meter in
                                      plant3_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        plant3_ds = system.dataset.toDataSet(plant3_headers, plant3_usage)

        plant4_site_data = monthly_site_usage.get(1003, {})
        plant4_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in plant4_site_data["site_usage"]]
        plant4_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant4_headers = ['Month'] + [meter["meter_name"] for meter in
                                      plant4_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        plant4_ds = system.dataset.toDataSet(plant4_headers, plant4_usage)

        pmp_hse_site_data = monthly_site_usage.get(1004, {})
        pmp_hse_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in pmp_hse_site_data["site_usage"]]
        pmp_hse_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        pmp_hse_headers = ['Month'] + [meter["meter_name"] for meter in
                                       pmp_hse_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        pmp_hse_ds = system.dataset.toDataSet(pmp_hse_headers, pmp_hse_usage)

        qa_site_data = monthly_site_usage.get(1005, {})
        qa_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in qa_site_data["site_usage"]]
        qa_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        qa_headers = ['Month'] + [meter["meter_name"] for meter in qa_site_data["site_usage"][0]["meter_usage"]] + [
            'Total kWh', 'Total %']
        qa_ds = system.dataset.toDataSet(qa_headers, qa_usage)

        pd_site_data = monthly_site_usage.get(1006, {})
        pd_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in pd_site_data["site_usage"]]
        pd_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        pd_headers = ['Month'] + [meter["meter_name"] for meter in pd_site_data["site_usage"][0]["meter_usage"]] + [
            'Total kWh', 'Total %']
        pd_ds = system.dataset.toDataSet(pd_headers, pd_usage)

        mfg_site_data = monthly_site_usage.get(1007, {})
        mfg_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in mfg_site_data["site_usage"]]
        mfg_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        mfg_headers = ['Month'] + [meter["meter_name"] for meter in mfg_site_data["site_usage"][0]["meter_usage"]] + [
            'Total kWh', 'Total %']
        mfg_ds = system.dataset.toDataSet(mfg_headers, mfg_usage)

        admin_site_data = monthly_site_usage.get(1008, {})
        admin_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in admin_site_data["site_usage"]]
        admin_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        admin_headers = ['Month'] + [meter["meter_name"] for meter in
                                     admin_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        admin_ds = system.dataset.toDataSet(admin_headers, admin_usage)

        conner_site_data = monthly_site_usage.get(1009, {})
        conner_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in conner_site_data["site_usage"]]
        conner_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        conner_headers = ['Month'] + [meter["meter_name"] for meter in
                                      conner_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        conner_ds = system.dataset.toDataSet(conner_headers, conner_usage)

        dist_site_data = monthly_site_usage.get(1010, {})
        dist_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in dist_site_data["site_usage"]]
        dist_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        dist_headers = ['Month'] + [meter["meter_name"] for meter in dist_site_data["site_usage"][0]["meter_usage"]] + [
            'Total kWh', 'Total %']
        dist_ds = system.dataset.toDataSet(dist_headers, dist_usage)

        old_cafe_site_data = monthly_site_usage.get(1011, {})
        old_cafe_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in old_cafe_site_data["site_usage"]]
        old_cafe_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        old_cafe_headers = ['Month'] + [meter["meter_name"] for meter in
                                        old_cafe_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        old_cafe_ds = system.dataset.toDataSet(old_cafe_headers, old_cafe_usage)

        new_cafe_site_data = monthly_site_usage.get(1012, {})
        new_cafe_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in new_cafe_site_data["site_usage"]]
        new_cafe_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        new_cafe_headers = ['Month'] + [meter["meter_name"] for meter in
                                        new_cafe_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        new_cafe_ds = system.dataset.toDataSet(new_cafe_headers, new_cafe_usage)

        aspex_site_data = monthly_site_usage.get(1013, {})
        aspex_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in aspex_site_data["site_usage"]]
        aspex_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        aspex_headers = ['Month'] + [meter["meter_name"] for meter in
                                     aspex_site_data["site_usage"][0]["meter_usage"]] + ['Total kWh', 'Total %']
        aspex_ds = system.dataset.toDataSet(aspex_headers, aspex_usage)

        wdc_site_data = monthly_site_usage.get(1014, {})
        wdc_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in wdc_site_data["site_usage"]]
        wdc_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        wdc_headers = ['Month'] + [meter["meter_name"] for meter in wdc_site_data["site_usage"][0]["meter_usage"]] + [
            'Total kWh', 'Total %']
        wdc_ds = system.dataset.toDataSet(wdc_headers, wdc_usage)

        twrA_site_data = monthly_site_usage.get(1015, {})
        twrA_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in twrA_site_data["site_usage"]]
        twrA_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        twrA_headers = ['Month'] + [meter["meter_name"] for meter in twrA_site_data["site_usage"][0]["meter_usage"]] + [
            'Total kWh', 'Total %']
        twrA_ds = system.dataset.toDataSet(twrA_headers, twrA_usage)

        twrB_site_data = monthly_site_usage.get(1016, {})
        twrB_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in twrB_site_data["site_usage"]]
        twrB_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        twrB_headers = ['Month'] + [meter["meter_name"] for meter in twrB_site_data["site_usage"][0]["meter_usage"]] + [
            'Total kWh', 'Total %']
        twrB_ds = system.dataset.toDataSet(twrB_headers, twrB_usage)

        twrC_site_data = monthly_site_usage.get(1017, {})
        twrC_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in twrC_site_data["site_usage"]]
        twrC_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        twrC_headers = ['Month'] + [meter["meter_name"] for meter in twrC_site_data["site_usage"][0]["meter_usage"]] + [
            'Total kWh', 'Total %']
        twrC_ds = system.dataset.toDataSet(twrC_headers, twrC_usage)

        yg_site_data = monthly_site_usage.get(1018, {})
        yg_usage = [[usage_entry["date"]] + [meter["usage_kwh"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kwh"], usage_entry["usage_pct"]] for usage_entry in yg_site_data["site_usage"]]
        yg_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        yg_headers = ['Month'] + [meter["meter_name"] for meter in yg_site_data["site_usage"][0]["meter_usage"]] + [
            'Total kWh', 'Total %']
        yg_ds = system.dataset.toDataSet(yg_headers, yg_usage)

        # Build the usage summary sheet for the requested month-year
        site_sumry_usage = []
        for site_id, site_meta in monthly_site_usage.items():
            if site_id != self.site_ref_id:
                site_name = site_meta["site_name"]
                for site_usage in site_meta["site_usage"]:
                    if site_usage["date"] == month_year:
                        sumry_entry = [site_name] + [site_usage["usage_kwh"]] + [site_usage["usage_pct"]]
                        site_sumry_usage.append(sumry_entry)

        site_sumry_headers = ["Building", "Total kWh", "Total %"]
        site_sumry_usage.sort(key=lambda x: x[1], reverse=True)
        site_sumry_ds = system.dataset.toDataSet(site_sumry_headers, site_sumry_usage)

        wb_ds = [
            site_sumry_ds, sbstn_ds, plant1_ds, plant2_ds, plant3_ds, plant4_ds, pmp_hse_ds, qa_ds, pd_ds, mfg_ds,
            admin_ds,
            conner_ds, dist_ds, old_cafe_ds, new_cafe_ds, aspex_ds, wdc_ds, twrA_ds, twrB_ds, twrC_ds, yg_ds
        ]

        sheet_names = [
            "Site Summary {0}".format(month_year), "Substation", "Plant 1 North", "Plant 2 Cogen", "Plant 3 South",
            "Plant 4 Conner G",
            "Pump House", "Quality Assurance", "Process Development", "Manufacturing", "Administration", "Conner",
            "Distribution",
            "Old Cafeteria", "New Cafeteria", "Aspex", "West Data Center", "Tower A", "Tower B", "Tower C",
            "Yards Grounds",
        ]

        excel_byte_array = system.dataset.toExcel(
            showHeaders=True,
            dataset=wb_ds,
            nullsEmpty=False,
            sheetNames=sheet_names
        )

        rpt_byte_array = system.report.executeReport(
            path=report_path, project="Alcon",
            parameters={"date_selection": month_year, "time_span_months": time_span_months},
            fileType="pdf"
        )

        file_timestamp = system.date.format(system.date.now(), "ddMMMyyyy_Hmm")
        attachment_names = ["Electrical_{0}.pdf".format(file_timestamp), "Electrical_{0}.xlsx".format(file_timestamp)]
        attachment_data = [rpt_byte_array, excel_byte_array]

        alcon.email.send_email(
            to_list=self.to_email_list,
            subject="Electrical Usage Reports {0}".format(month_year),
            body="See attached electrical usage report files.",
            from_addr="data-service@siteautomation.net",
            attachment_names=attachment_names,
            attachment_data=attachment_data
        )


class ChilledWater(Meter):
    def __init__(self):
        super(ChilledWater, self).__init__()
        self.db_mtr_tags_tbl = "chilled_water_meters"
        self.db_mtr_daily_tbl = "chilled_water_daily_usage"
        self.db_mtr_monthly_tbl = "chilled_water_monthly_usage"
        # Excluded meters
        self.excl_mtrs = []  # Put meter ids here that are decommissioned, inactive, or out of service.
        # Muliplier of (1 or -1)
        self.site_meter_map = {
            1000: {'site_name': 'ADMIN', 'site_meter_ids': {6: 1}},
            1001: {'site_name': 'ASPEX', 'site_meter_ids': {2: 1, 3: 1}},
            1002: {'site_name': 'AUD', 'site_meter_ids': {8: 1}},
            1003: {"site_name": "CONNER", "site_meter_ids": {15: 1, 17: 1}},
            1004: {'site_name': 'DIST', 'site_meter_ids': {2: -1, 3: -1, 4: 1}},
            1005: {'site_name': 'MFG', 'site_meter_ids': {1: 1, 5: 1, 6: -1, 7: -1}},
            1006: {'site_name': 'CAFE_OLD', 'site_meter_ids': {12: 1}},
            1007: {'site_name': 'PD', 'site_meter_ids': {13: 1}},
            1008: {'site_name': 'PLANT_2_WEST', 'site_meter_ids': {5: 1}},
            1009: {'site_name': 'PLANT_2_NORTH', 'site_meter_ids': {14: 1}},
            1010: {'site_name': 'PLANT_3', 'site_meter_ids': {4: 1}},
            1011: {'site_name': 'QA', 'site_meter_ids': {16: 1}},
            1012: {'site_name': 'TOWER_A', 'site_meter_ids': {9: 1}},
            1013: {'site_name': 'TOWER_B', 'site_meter_ids': {10: 1}},
            1014: {'site_name': 'TOWER_C', 'site_meter_ids': {11: 1}},
            1015: {'site_name': 'WDC', 'site_meter_ids': {7: 1}},
        }

    def __str__(self):
        return "Chilled Water Meter Class"

    def get_meter_id_name(self):
        # A comma separated string of excluded meter ids.
        if self.excl_mtrs:
            excl_mtrs_str = ", ".join([str(mtr) for mtr in self.excl_mtrs])
            query = "SELECT id, name FROM {0} WHERE id NOT IN ({1})".format(self.db_mtr_tags_tbl, excl_mtrs_str)
        else:
            query = "SELECT id, name FROM {0}".format(self.db_mtr_tags_tbl)

        mtr_id_ds = system.db.runQuery(query=query, database=self.db)

        # Return a list of meter ids.
        meter_id_list = mtr_id_ds.getColumnAsList(0)

        # Create a {meter id: meter name} dictionary for all active meters
        meter_id_name_dict = {meter_id_list[i]: mtr_id_ds.getValueAt(i, 1) for i in range(mtr_id_ds.getRowCount())}
        # Add the site meter map
        meter_id_name_dict.update(self.site_meter_map)

        return meter_id_name_dict

    def calculate_usage(self, tag_providers, start_date=None, end_date=None, activate_insert_query=False,
                        meter_largest_value=65534):
        if not tag_providers:
            raise TagProviderError("At least one tag provider must be provided.")

        if any(not tag_provider for tag_provider in tag_providers):
            raise TagProviderError("Tag provider list contains one or more empty strings.")

        tag_providers_str = ", ".join(["'{0}'".format(provider) for provider in tag_providers])
        tags_in_prvdr_query = "SELECT id, tag_path FROM {0} WHERE tag_provider in ({1}) and meter_active".format(
            self.db_mtr_tags_tbl, tag_providers_str)

        # Returns a Pydataset of the meter_id and tag_path
        pyds_tags_in_prvdr = system.db.runQuery(query=tags_in_prvdr_query, database=self.db)

        if len(pyds_tags_in_prvdr) <= 0:
            raise TagProviderError(
                "No meter tag path could be found from the list of tag providers: {0}. Invalid tag provider name syntax.".format(
                    tag_providers))

        # Month from 0 - 11 and day from 1 - 31
        #
        # If start and end date are not provided then defaults are:
        #	start_date = the day before yesterday
        #	end_date = yesterday
        if not start_date or not end_date:
            # Zero out the time by using the system.date.midnight() of the current time.
            # Returns:
            #	example start_date: Sun May 04 00:00:00 CDT 2025
            #	example end_date: Mon May 05 23:59:59 CDT 2025
            start_date = system.date.addDays(system.date.midnight(system.date.now()), -2)
            end_date = system.date.addMillis(system.date.midnight(system.date.now()), -1)

        daily_usage_dict = {}

        for tag in pyds_tags_in_prvdr:
            tag_id = tag[0]
            # meter name (v) will be mapped to the tag_id (k) on the first occurance
            meter_name = next((v for k, v in self.get_meter_id_name().items() if k == tag_id))
            tag_path = tag[1].split(".")[0]
            pyds_tag_hist = system.dataset.toPyDataSet(system.tag.queryTagHistory(
                paths=[tag_path],
                startDate=start_date,
                endDate=end_date))

            # Organize samples by date & time, checking for duplicate (time_only, tag_value) combinations.
            usage_unique_dict = {}
            usage_time_dict = {}
            for hist_data in pyds_tag_hist:
                timestamp = hist_data[0]
                tag_value = hist_data[1]
                date_only = system.date.midnight(timestamp)
                time_only = system.date.format(timestamp, "HH:mm:ss")

                # Check for values that are NOT (None or 0).
                if tag_value:
                    # Build a dictionary that consist of date_only key and set of tuples as the value.
                    usage_unique_dict.setdefault(date_only, set()).add((time_only, tag_value))

            # Explicitly remove duplicates by usage, picking the usage with the latest time.
            for date_only, time_usage_set in usage_unique_dict.items():
                # {date_only: time_usage_set} = {Wed Apr 30 00:00:00 CDT 2025: {('19:31:05', -4150.1337890625)...,)}
                #
                # Temporary dictionary to store the unique {uage: time, ...} pairs.
                temp_usage_time_dict = {}
                # Iterate through the time_usage_set, if two times have the same usage pick the usage with the later time.
                for t, u in time_usage_set:
                    if (u not in temp_usage_time_dict) or (t > temp_usage_time_dict[u]):
                        temp_usage_time_dict[u] = t

                # Create a new dictionary that consist of a list of dictionaries which are {key:value} pairs of {time:usage}
                # for a start and end date sorted by time.
                usage_time_dict[date_only] = sorted(
                    [{"time": t, "usage": u} for u, t in temp_usage_time_dict.items()],
                    key=lambda x: x["time"]
                )

            # Separate the samples for the start and end date for comparison.
            start_date_smpls = usage_time_dict.get(start_date, [])
            end_date_smpls = usage_time_dict.get(system.date.midnight(end_date), [])

            # The static helper function (calculate_usage_helper) will assist
            # with providing the usage between the start and end date.
            if end_date_smpls and start_date_smpls:
                # Order matters here, set the end date samples as the first argument in function.
                total_usage = self.calculate_usage_helper(end_date_smpls, start_date_smpls)
            elif end_date_smpls and not start_date_smpls:
                total_usage = self.calculate_usage_helper(end_date_smpls)
            else:
                total_usage = 0

            daily_usage_dict[tag_id] = {
                "meter_name": str(meter_name),
                "timestamp_millis": system.date.toMillis(system.date.midnight(end_date)),
                "timestamp_date": system.date.midnight(end_date),
                "usage": total_usage
            }

        record_exist_query = "SELECT count(meter_id) FROM {0} WHERE meter_id = ? AND time_stamp = ?".format(
            self.db_mtr_daily_tbl)
        insert_usage_query = "INSERT INTO {0} (meter_id, time_stamp, daily_usage) VALUES (?, ?, ?)".format(
            self.db_mtr_daily_tbl)

        for tag_id, meta_data in daily_usage_dict.items():
            meter_name = meta_data["meter_name"]
            timestamp_millis = meta_data["timestamp_millis"]
            timestamp_date = meta_data["timestamp_date"]
            kton_usage = meta_data["usage"]

            # Safe check before inserting record(s) into table
            record_exist = \
            system.db.runPrepQuery(query=record_exist_query, args=[tag_id, timestamp_millis], database=self.db)[0][0]
            if not record_exist:
                if activate_insert_query:
                    system.db.runPrepUpdate(query=insert_usage_query, args=[tag_id, timestamp_millis, kton_usage],
                                            database=self.db)
                else:
                    print(
                        "The following records(s) will not be inserted into the db table until 'calculate_usage(activate_insert_query=True)'!")
                print(
                    "insert record [tag_id={0}, meter_name={1}, timestamp_date={2}, timestamp_millis={3}, usage={4}]\n".format(
                        tag_id, meter_name, timestamp_date, timestamp_millis, kton_usage))
            else:
                print(
                    "The record exist [tag_id={0}, meter_name={1}, timestamp_date={2}, timestamp_millis={3}, usage={4}]\n".format(
                        tag_id, meter_name, timestamp_date, timestamp_millis, kton_usage))

        return daily_usage_dict

    def get_monthly_usage(self, month_year=None, time_span_months=18):

        meter_id_name_dict = self.get_meter_id_name()

        # Ignore the site meter ids/keys
        meter_ids = [mtr_id for mtr_id in meter_id_name_dict if mtr_id not in self.site_meter_map.keys()]
        meter_ids_str = ", ".join([str(i) for i in meter_ids])

        # Set end_date based on month_year or current date
        if month_year:
            end_date = month_year
        else:
            end_date = system.date.format(system.date.now(), "MMM-yyyy")

        if time_span_months > 0:
            start_date = system.date.format(
                system.date.addMonths(system.date.parse(end_date, "MMM-yyyy"), -time_span_months), "MMM-yyyy")
            query = """
				SELECT * FROM {0} WHERE STR_TO_DATE(month_year, '%b-%Y') >= STR_TO_DATE(?, '%b-%Y') AND 
				STR_TO_DATE(month_year, '%b-%Y') <= STR_TO_DATE(?, '%b-%Y') AND meter_id IN ({1})
			""".format(self.db_mtr_monthly_tbl, meter_ids_str)
            args = [start_date, end_date]
        # If time_span_months is zero or negative, query just one month
        else:
            query = "SELECT * FROM {0} WHERE month_year = ? AND meter_id IN ({1})".format(self.db_mtr_monthly_tbl,
                                                                                          meter_ids_str)
            args = [end_date]

        monthly_usage_pyds = system.db.runPrepQuery(query=query, args=args, database=self.db)
        monthly_usage_dict = {}

        # Add the meter names to the monthly_usage
        for row in monthly_usage_pyds:
            meter_id = row[0]
            meter_name = meter_id_name_dict.get(meter_id)
            usage = row[1]
            date = row[2]
            meter_entry = monthly_usage_dict.setdefault(meter_id, {"meter_name": meter_name, "meter_usage": []})
            meter_entry["meter_usage"].append({"date": date, "usage_kton": usage})

        # Sort by date
        for mtr_entry in monthly_usage_dict.values():
            mtr_entry["meter_usage"].sort(key=lambda x: datetime.strptime(x["date"], '%b-%Y'))

        return monthly_usage_dict

    def get_monthly_site_usage(self, monthly_usage):
        site_usage_dict = {}

        for site_id, site_meta in self.site_meter_map.items():
            #
            site_meter_ids = site_meta["site_meter_ids"]
            site_name = site_meta["site_name"]
            site_entry = site_usage_dict.setdefault(site_id, {
                "site_name": site_name,
                "site_meter_ids": site_meter_ids.keys(),
                "site_usage": []
            })

            usage_by_date = {}  # Temporary dict to help in aggregation
            for meter_id, multiplier in site_meter_ids.items():
                if meter_id in monthly_usage.keys():
                    meter_name = monthly_usage[meter_id]["meter_name"]
                    meter_usage = monthly_usage[meter_id]["meter_usage"]
                    for i in meter_usage:
                        date = i["date"]
                        usage_kton = round(i["usage_kton"], 1) * multiplier if i["usage_kton"] else 0.0
                        if date not in usage_by_date:
                            usage_by_date[date] = {"usage_kton": 0.0, "meter_usage": []}

                        usage_by_date[date]["usage_kton"] += usage_kton

                        usage_by_date[date]["meter_usage"].append({
                            "meter_id": meter_id,
                            "meter_name": meter_name,
                            "usage_kton": usage_kton
                        })

            for date, usage_meta in usage_by_date.items():
                site_entry["site_usage"].append({
                    "date": date,
                    "usage_kton": round(usage_meta["usage_kton"], 1),
                    "meter_usage": usage_meta["meter_usage"]
                })

            site_usage_dict[site_id] = site_entry

        return site_usage_dict

    def monthly_report(self, report_path="Chilled Water/Chilled Water", month_year=None, time_span_months=18):
        # For sites with a single meter
        #
        # Format: Creates a list of [date, usage] pairs for each usage entry where:
        # - usage_entry["date"] gives the month (e.g., "Jan-2025")
        # - usage_entry["usage_kton"] gives the total usage in kilotons
        # Example output: [["Jan-2025", 150.5], ["Feb-2025", 140.2], ...]

        # Set the month year to the current month year if there's not one selected.
        if not month_year:
            month_year = system.date.format(system.date.now(), "MMM-yyy")

        if time_span_months <= 0:
            time_span_months = 18

        monthly_usage = self.get_monthly_usage(month_year=month_year, time_span_months=time_span_months)
        monthly_site_usage = self.get_monthly_site_usage(monthly_usage)

        plant2_west_site_data = monthly_site_usage.get(1000, {})
        plant2_west_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                             plant2_west_site_data["site_usage"]]
        plant2_west_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant2_west_headers = ["Month-Year"] + ["Total kTon"]
        plant2_west_ds = system.dataset.toDataSet(plant2_west_headers, plant2_west_usage)

        plant2_north_site_data = monthly_site_usage.get(1001, {})
        plant2_north_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                              plant2_north_site_data["site_usage"]]
        plant2_north_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant2_north_headers = ["Month-Year"] + ["Total kTon"]
        plant2_north_ds = system.dataset.toDataSet(plant2_north_headers, plant2_north_usage)

        plant3_site_data = monthly_site_usage.get(1002, {})
        plant3_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        plant3_site_data["site_usage"]]
        plant3_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant3_headers = ["Month-Year"] + ["Total kTon"]
        plant3_ds = system.dataset.toDataSet(plant3_headers, plant3_usage)

        admin_site_data = monthly_site_usage.get(1003, {})
        admin_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                       admin_site_data["site_usage"]]
        admin_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        admin_headers = ["Month-Year"] + ["Total kTon"]
        admin_ds = system.dataset.toDataSet(admin_headers, admin_usage)

        aud_site_data = monthly_site_usage.get(1004, {})
        aud_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in aud_site_data["site_usage"]]
        aud_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        aud_headers = ["Month-Year"] + ["Total kTon"]
        aud_ds = system.dataset.toDataSet(aud_headers, aud_usage)

        cafe_old_site_data = monthly_site_usage.get(1005, {})
        cafe_old_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                          cafe_old_site_data["site_usage"]]
        cafe_old_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        cafe_old_headers = ["Month-Year"] + ["Total kTon"]
        cafe_old_ds = system.dataset.toDataSet(cafe_old_headers, cafe_old_usage)

        towerA_site_data = monthly_site_usage.get(1006, {})
        towerA_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        towerA_site_data["site_usage"]]
        towerA_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        towerA_headers = ["Month-Year"] + ["Total kTon"]
        towerA_ds = system.dataset.toDataSet(towerA_headers, towerA_usage)

        towerB_site_data = monthly_site_usage.get(1007, {})
        towerB_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        towerB_site_data["site_usage"]]
        towerB_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        towerB_headers = ["Month-Year"] + ["Total kTon"]
        towerB_ds = system.dataset.toDataSet(towerB_headers, towerB_usage)

        towerC_site_data = monthly_site_usage.get(1008, {})
        towerC_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        towerC_site_data["site_usage"]]
        towerC_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        towerC_headers = ["Month-Year"] + ["Total kTon"]
        towerC_ds = system.dataset.toDataSet(towerC_headers, towerC_usage)

        wdc_site_data = monthly_site_usage.get(1009, {})
        wdc_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in wdc_site_data["site_usage"]]
        wdc_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        wdc_headers = ["Month-Year"] + ["Total kTon"]
        wdc_ds = system.dataset.toDataSet(wdc_headers, wdc_usage)

        pd_site_data = monthly_site_usage.get(1010, {})
        pd_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in pd_site_data["site_usage"]]
        pd_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        pd_headers = ["Month-Year"] + ["Total kTon"]
        pd_ds = system.dataset.toDataSet(pd_headers, pd_usage)

        aspex_site_data = monthly_site_usage.get(1011, {})
        aspex_usage = [[usage_entry["date"]] + [meter["usage_kton"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kton"]] for usage_entry in aspex_site_data["site_usage"]]
        aspex_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        aspex_headers = ["Month-Year"] + [meter["meter_name"] for meter in
                                          aspex_site_data["site_usage"][0]["meter_usage"]] + ["Total kTon"]
        aspex_ds = system.dataset.toDataSet(aspex_headers, aspex_usage)

        conner_site_data = monthly_site_usage.get(1012, {})
        conner_usage = [[usage_entry["date"]] + [meter["usage_kton"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kton"]] for usage_entry in conner_site_data["site_usage"]]
        conner_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        conner_headers = ["Month-Year"] + [meter["meter_name"] for meter in
                                           conner_site_data["site_usage"][0]["meter_usage"]] + ["Total kTon"]
        conner_ds = system.dataset.toDataSet(conner_headers, conner_usage)

        dist_site_data = monthly_site_usage.get(1013, {})
        dist_usage = [[usage_entry["date"]] + [meter["usage_kton"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kton"]] for usage_entry in dist_site_data["site_usage"]]
        dist_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        dist_headers = ["Month-Year"] + [meter["meter_name"] for meter in
                                         dist_site_data["site_usage"][0]["meter_usage"]] + ["Total kTon"]
        dist_ds = system.dataset.toDataSet(dist_headers, dist_usage)

        mfg_site_data = monthly_site_usage.get(1014, {})
        mfg_usage = [[usage_entry["date"]] + [meter["usage_kton"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kton"]] for usage_entry in mfg_site_data["site_usage"]]
        mfg_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        mfg_headers = ["Month-Year"] + [meter["meter_name"] for meter in
                                        mfg_site_data["site_usage"][0]["meter_usage"]] + ["Total kTon"]
        mfg_ds = system.dataset.toDataSet(mfg_headers, mfg_usage)

        # Build the usage summary sheet for the requested month-year
        site_sumry_usage = []
        for site_id, site_meta in monthly_site_usage.items():
            site_name = site_meta["site_name"]
            for site_usage in site_meta["site_usage"]:
                if site_usage["date"] == month_year:
                    sumry_entry = [site_name] + [site_usage["usage_kton"]]
                    site_sumry_usage.append(sumry_entry)

        site_sumry_headers = ["Building", "Total kTon"]
        site_sumry_usage.sort(key=lambda x: x[1], reverse=True)
        site_sumry_ds = system.dataset.toDataSet(site_sumry_headers, site_sumry_usage)

        wb_ds = [
            site_sumry_ds, plant2_west_ds, plant2_north_ds, plant3_ds, admin_ds, aud_ds,
            cafe_old_ds, towerA_ds, towerB_ds, towerC_ds, wdc_ds, pd_ds, aspex_ds,
            conner_ds, dist_ds, mfg_ds
        ]

        sheet_names = [
            "Site Summary {0}".format(month_year), "Plant 2 West", "Plant 2 North",
            "Plant 3", "Administration", "Auditorium", "Cafe Old", "Tower A", "Tower B",
            "Tower C", "West Data Center", "Process Development", "Aspex", "Conner",
            "Distribution", "Manufacturing"
        ]

        excel_byte_array = system.dataset.toExcel(
            showHeaders=True,
            dataset=wb_ds,
            nullsEmpty=False,
            sheetNames=sheet_names
        )

        rpt_byte_array = system.report.executeReport(
            path=report_path, project="Alcon",
            parameters={"date_selection": month_year, "time_span_months": time_span_months},
            fileType="pdf"
        )

        file_timestamp = system.date.format(system.date.now(), "ddMMMyyyy_Hmm")
        attachment_names = ["ChilledWater_{0}.pdf".format(file_timestamp),
                            "ChilledWater_{0}.xlsx".format(file_timestamp)]
        attachment_data = [rpt_byte_array, excel_byte_array]

        alcon.email.send_email(
            to_list=self.to_email_list,
            subject="Chilled Water Usage Reports {0}".format(month_year),
            body="See attached chilled water usage report files.",
            from_addr="data-service@siteautomation.net",
            attachment_names=attachment_names,
            attachment_data=attachment_data
        )

    def daily_report(self):
        pass

    def find_missing_data(self):
        pass

    def insert_missing_data(self):
        pass

    def get_raw_historical_data(self):
        pass


class DomesticWater(Meter):
    def __init__(self):
        super(DomesticWater, self).__init__()
        self.db_mtr_tags_tbl = "domestic_water_meters"
        self.db_mtr_daily_tbl = "domestic_water_daily_usage"
        self.db_mtr_monthly_tbl = "domestic_water_monthly_usage"
        # Excluded meters
        self.excluded_meters = [25, 26]  # Put meter ids here that are decommissioned, inactive, or out of service.
        self.excl_mtrs = []  # Put meter ids here that are decommissioned, inactive, or out of service.
        # Muliplier of (1 or -1)
        self.site_meter_map = {
            1000: {'site_name': 'ADMIN', 'site_meter_ids': {8: 1}},
            1001: {'site_name': 'ASPEX', 'site_meter_ids': {10: 1, 11: 1, 12: 1, 13: 1}},
            1002: {'site_name': 'AUD', 'site_meter_ids': {5: 1}},
            1003: {'site_name': 'DIST', 'site_meter_ids': {24: 1}},
            1004: {'site_name': 'MFG', 'site_meter_ids': {20: 1, 21: 1, 22: 1, 23: 1}},
            1005: {'site_name': 'CAFE_NEW', 'site_meter_ids': {25: 1}},
            1006: {'site_name': 'CAFE_OLD', 'site_meter_ids': {2: 1}},
            1007: {'site_name': 'PD', 'site_meter_ids': {27: 1}},
            1008: {'site_name': 'PLANT_1', 'site_meter_ids': {3: 1, 4: 1}},
            1009: {'site_name': 'PLANT_2', 'site_meter_ids': {14: 1}},
            1010: {'site_name': 'PLANT_3', 'site_meter_ids': {1: 1}},
            1011: {'site_name': 'PLANT_4', 'site_meter_ids': {15: 1, 16: 1, 17: 1, 18: 1}},
            1012: {'site_name': 'QA', 'site_meter_ids': {19: 1}},
            1013: {'site_name': 'TOWER_A', 'site_meter_ids': {6: 1}},
            1014: {'site_name': 'TOWER_B', 'site_meter_ids': {7: 1}},
            1015: {'site_name': 'TOWER_C', 'site_meter_ids': {9: 1}},
            1016: {'site_name': 'WDC', 'site_meter_ids': {26: 1}},
        }

    def __str__(self):
        return "Domestic Water Meter Class"

    def get_meter_id_name(self):
        # A comma separated string of excluded meter ids.
        if self.excl_mtrs:
            excl_mtrs_str = ", ".join([str(mtr) for mtr in self.excl_mtrs])
            query = "SELECT id, name FROM {0} WHERE id NOT IN ({1})".format(self.db_mtr_tags_tbl, excl_mtrs_str)
        else:
            query = "SELECT id, name FROM {0}".format(self.db_mtr_tags_tbl)

        mtr_id_ds = system.db.runQuery(query=query, database=self.db)

        # Return a list of meter ids.
        meter_id_list = mtr_id_ds.getColumnAsList(0)

        # Create a {meter id: meter name} dictionary for all active meters
        meter_id_name_dict = {meter_id_list[i]: mtr_id_ds.getValueAt(i, 1) for i in range(mtr_id_ds.getRowCount())}
        # Add the site meter map
        meter_id_name_dict.update(self.site_meter_map)

        return meter_id_name_dict

    def calculate_usage(self, tag_providers, start_date=None, end_date=None, activate_insert_query=False):
        pass

    def get_monthly_usage(self, month_year=None, time_span_months=18):

        meter_id_name_dict = self.get_meter_id_name()

        # Ignore the site meter ids/keys
        meter_ids = [mtr_id for mtr_id in meter_id_name_dict if mtr_id not in self.site_meter_map.keys()]
        meter_ids_str = ", ".join([str(i) for i in meter_ids])

        # Set end_date based on month_year or current date
        if month_year:
            end_date = month_year
        else:
            end_date = system.date.format(system.date.now(), "MMM-yyyy")

        if time_span_months > 0:
            start_date = system.date.format(
                system.date.addMonths(system.date.parse(end_date, "MMM-yyyy"), -time_span_months), "MMM-yyyy")
            query = """
				SELECT * FROM {0} WHERE STR_TO_DATE(month_year, '%b-%Y') >= STR_TO_DATE(?, '%b-%Y') AND 
				STR_TO_DATE(month_year, '%b-%Y') <= STR_TO_DATE(?, '%b-%Y') AND meter_id IN ({1})
			""".format(self.db_mtr_monthly_tbl, meter_ids_str)
            args = [start_date, end_date]
        # If time_span_months is zero or negative, query just one month
        else:
            query = "SELECT * FROM {0} WHERE month_year = ? AND meter_id IN ({1})".format(self.db_mtr_monthly_tbl,
                                                                                          meter_ids_str)
            args = [end_date]

        monthly_usage_pyds = system.db.runPrepQuery(query=query, args=args, database=self.db)
        monthly_usage_dict = {}

        # Add the meter names to the monthly_usage
        for row in monthly_usage_pyds:
            meter_id = row[0]
            meter_name = meter_id_name_dict.get(meter_id)
            usage = row[1]
            date = row[2]
            meter_entry = monthly_usage_dict.setdefault(meter_id, {"meter_name": meter_name, "meter_usage": []})
            meter_entry["meter_usage"].append({"date": date, "usage_kgal": usage})

        # Sort by date
        for mtr_entry in monthly_usage_dict.values():
            mtr_entry["meter_usage"].sort(key=lambda x: datetime.strptime(x["date"], '%b-%Y'))

        return monthly_usage_dict

    def get_monthly_site_usage(self, monthly_usage):
        site_usage_dict = {}

        for site_id, site_meta in self.site_meter_map.items():
            #
            site_meter_ids = site_meta["site_meter_ids"]
            site_name = site_meta["site_name"]
            site_entry = site_usage_dict.setdefault(site_id, {
                "site_name": site_name,
                "site_meter_ids": site_meter_ids.keys(),
                "site_usage": []
            })

            usage_by_date = {}  # Temporary dict to help in aggregation
            for meter_id, multiplier in site_meter_ids.items():
                if meter_id in monthly_usage.keys():
                    meter_name = monthly_usage[meter_id]["meter_name"]
                    meter_usage = monthly_usage[meter_id]["meter_usage"]
                    for i in meter_usage:
                        date = i["date"]
                        usage_kgal = round(i["usage_kgal"], 1) * multiplier if i["usage_kgal"] else 0.0
                        if date not in usage_by_date:
                            usage_by_date[date] = {"usage_kgal": 0.0, "meter_usage": []}

                        usage_by_date[date]["usage_kgal"] += usage_kgal

                        usage_by_date[date]["meter_usage"].append({
                            "meter_id": meter_id,
                            "meter_name": meter_name,
                            "usage_kgal": usage_kgal
                        })

            for date, usage_meta in usage_by_date.items():
                site_entry["site_usage"].append({
                    "date": date,
                    "usage_kgal": round(usage_meta["usage_kgal"], 1),
                    "meter_usage": usage_meta["meter_usage"]
                })

            site_usage_dict[site_id] = site_entry

        return site_usage_dict

    def monthly_report(self, report_path="Domestic Water/Domestic Water", month_year=None, time_span_months=18):
        # For sites with a single meter
        #
        # Format: Creates a list of [date, usage] pairs for each usage entry where:
        # - usage_entry["date"] gives the month (e.g., "Jan-2025")
        # - usage_entry["usage_kgal"] gives the total usage in kilotons
        # Example output: [["Jan-2025", 150.5], ["Feb-2025", 140.2], ...]

        # Set the month year to the current month year if there's not one selected.
        if not month_year:
            month_year = system.date.format(system.date.now(), "MMM-yyy")

        if time_span_months <= 0:
            time_span_months = 18

        monthly_usage = self.get_monthly_usage(month_year=month_year, time_span_months=time_span_months)
        monthly_site_usage = self.get_monthly_site_usage(monthly_usage)

        admin_site_data = monthly_site_usage.get(1000, {})
        admin_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in
                       admin_site_data["site_usage"]]
        admin_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        admin_headers = ["Month-Year"] + ["Total kGal"]
        admin_ds = system.dataset.toDataSet(admin_headers, admin_usage)

        aspex_site_data = monthly_site_usage.get(1001, {})
        aspex_usage = [[usage_entry["date"]] + [meter["usage_kgal"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kgal"]] for usage_entry in aspex_site_data["site_usage"]]
        aspex_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        aspex_headers = ["Month-Year"] + [meter["meter_name"] for meter in
                                          aspex_site_data["site_usage"][0]["meter_usage"]] + ["Total kGal"]
        aspex_ds = system.dataset.toDataSet(aspex_headers, aspex_usage)

        aud_site_data = monthly_site_usage.get(1002, {})
        aud_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in aud_site_data["site_usage"]]
        aud_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        aud_headers = ["Month-Year"] + ["Total kGal"]
        aud_ds = system.dataset.toDataSet(aud_headers, aud_usage)

        dist_site_data = monthly_site_usage.get(1003, {})
        dist_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in
                      dist_site_data["site_usage"]]
        dist_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        dist_headers = ["Month-Year"] + ["Total kGal"]
        dist_ds = system.dataset.toDataSet(dist_headers, dist_usage)

        mfg_site_data = monthly_site_usage.get(1004, {})
        mfg_usage = [[usage_entry["date"]] + [meter["usage_kgal"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kgal"]] for usage_entry in mfg_site_data["site_usage"]]
        mfg_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        mfg_headers = ["Month-Year"] + [meter["meter_name"] for meter in
                                        mfg_site_data["site_usage"][0]["meter_usage"]] + ["Total kGal"]
        mfg_usage = self.dataset_row_len_check(mfg_headers, mfg_usage)
        mfg_ds = system.dataset.toDataSet(mfg_headers, mfg_usage)

        cafe_new_site_data = monthly_site_usage.get(1005, {})
        cafe_new_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in
                          cafe_new_site_data["site_usage"]]
        cafe_new_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        cafe_new_headers = ["Month-Year"] + ["Total kGal"]
        cafe_new_ds = system.dataset.toDataSet(cafe_new_headers, cafe_new_usage)

        cafe_old_site_data = monthly_site_usage.get(1006, {})
        cafe_old_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in
                          cafe_old_site_data["site_usage"]]
        cafe_old_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        cafe_old_headers = ["Month-Year"] + ["Total kGal"]
        cafe_old_ds = system.dataset.toDataSet(cafe_old_headers, cafe_old_usage)

        pd_site_data = monthly_site_usage.get(1007, {})
        pd_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in pd_site_data["site_usage"]]
        pd_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        pd_headers = ["Month-Year"] + ["Total kGal"]
        pd_ds = system.dataset.toDataSet(pd_headers, pd_usage)

        plant1_site_data = monthly_site_usage.get(1008, {})
        plant1_usage = [[usage_entry["date"]] + [meter["usage_kgal"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kgal"]] for usage_entry in plant1_site_data["site_usage"]]
        plant1_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant1_headers = ["Month-Year"] + [meter["meter_name"] for meter in
                                           plant1_site_data["site_usage"][0]["meter_usage"]] + ["Total kGal"]
        plant1_ds = system.dataset.toDataSet(plant1_headers, plant1_usage)

        plant2_site_data = monthly_site_usage.get(1009, {})
        plant2_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in
                        plant2_site_data["site_usage"]]
        plant2_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant2_headers = ["Month-Year"] + ["Total kGal"]
        plant2_ds = system.dataset.toDataSet(plant2_headers, plant2_usage)

        plant3_site_data = monthly_site_usage.get(1010, {})
        plant3_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in
                        plant3_site_data["site_usage"]]
        plant3_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant3_headers = ["Month-Year"] + ["Total kGal"]
        plant3_ds = system.dataset.toDataSet(plant3_headers, plant3_usage)

        plant4_site_data = monthly_site_usage.get(1011, {})
        plant4_usage = [[usage_entry["date"]] + [meter["usage_kgal"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kgal"]] for usage_entry in plant4_site_data["site_usage"]]
        plant4_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant4_headers = ["Month-Year"] + [meter["meter_name"] for meter in
                                           plant4_site_data["site_usage"][0]["meter_usage"]] + ["Total kGal"]
        plant4_ds = system.dataset.toDataSet(plant4_headers, plant4_usage)

        qa_site_data = monthly_site_usage.get(1012, {})
        qa_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in qa_site_data["site_usage"]]
        qa_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        qa_headers = ["Month-Year"] + ["Total kGal"]
        qa_ds = system.dataset.toDataSet(qa_headers, qa_usage)

        towerA_site_data = monthly_site_usage.get(1013, {})
        towerA_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in
                        towerA_site_data["site_usage"]]
        towerA_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        towerA_headers = ["Month-Year"] + ["Total kGal"]
        towerA_ds = system.dataset.toDataSet(towerA_headers, towerA_usage)

        towerB_site_data = monthly_site_usage.get(1014, {})
        towerB_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in
                        towerB_site_data["site_usage"]]
        towerB_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        towerB_headers = ["Month-Year"] + ["Total kGal"]
        towerB_ds = system.dataset.toDataSet(towerB_headers, towerB_usage)

        towerC_site_data = monthly_site_usage.get(1015, {})
        towerC_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in
                        towerC_site_data["site_usage"]]
        towerC_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        towerC_headers = ["Month-Year"] + ["Total kGal"]
        towerC_ds = system.dataset.toDataSet(towerC_headers, towerC_usage)

        wdc_site_data = monthly_site_usage.get(1016, {})
        wdc_usage = [[usage_entry["date"]] + [usage_entry["usage_kgal"]] for usage_entry in wdc_site_data["site_usage"]]
        wdc_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        wdc_headers = ["Month-Year"] + ["Total kGal"]
        wdc_ds = system.dataset.toDataSet(wdc_headers, wdc_usage)

        # Build the usage summary sheet for the requested month-year
        site_sumry_usage = []
        for site_id, site_meta in monthly_site_usage.items():
            site_name = site_meta["site_name"]
            for site_usage in site_meta["site_usage"]:
                if site_usage["date"] == month_year:
                    sumry_entry = [site_name] + [site_usage["usage_kgal"]]
                    site_sumry_usage.append(sumry_entry)

        site_sumry_headers = ["Building", "Total kGal"]
        site_sumry_usage.sort(key=lambda x: x[1], reverse=True)
        site_sumry_ds = system.dataset.toDataSet(site_sumry_headers, site_sumry_usage)

        wb_ds = [
            site_sumry_ds, admin_ds, aspex_ds, aud_ds, dist_ds, mfg_ds, cafe_new_ds,
            cafe_old_ds, pd_ds, plant1_ds, plant2_ds, plant3_ds, plant4_ds,
            qa_ds, towerA_ds, towerB_ds, towerC_ds, wdc_ds
        ]

        sheet_names = [
            "Site Summary {0}".format(month_year), "Administration", "Aspex", "Auditorium",
            "Distribution", "Manufacturing", "Cafe New", "Cafe Old", "Process Development",
            "Plant 1", "Plant 2", "Plant 3", "Plant 4", "Quality Assurance",
            "Tower A", "Tower B", "Tower C", "West Data Center"
        ]

        excel_byte_array = system.dataset.toExcel(
            showHeaders=True,
            dataset=wb_ds,
            nullsEmpty=False,
            sheetNames=sheet_names
        )

        rpt_byte_array = system.report.executeReport(
            path=report_path, project="Alcon",
            parameters={"date_selection": month_year, "time_span_months": time_span_months},
            fileType="pdf"
        )

        file_timestamp = system.date.format(system.date.now(), "ddMMMyyyy_Hmm")
        attachment_names = ["DomesticWater_{0}.pdf".format(file_timestamp),
                            "DomesticWater_{0}.xlsx".format(file_timestamp)]
        attachment_data = [rpt_byte_array, excel_byte_array]

        alcon.email.send_email(
            to_list=self.to_email_list,
            subject="Domestic Water Usage Reports {0}".format(month_year),
            body="See attached domestic water usage report files.",
            from_addr="data-service@siteautomation.net",
            attachment_names=attachment_names,
            attachment_data=attachment_data
        )

    def daily_report(self):
        pass

    def find_missing_data(self):
        pass

    def insert_missing_data(self):
        pass

    def get_raw_historical_data(self):
        pass


class Steam(Meter):
    def __init__(self):
        # form should be new_id: {meter_id: multiplier}; start new_id at 100
        # form should be meter_id: name (this name will show in the round-up table)
        super(Steam, self).__init__()
        self.db_mtr_tags_tbl = "steam_meters"
        self.db_mtr_daily_tbl = "steam_daily_usage"
        self.db_mtr_monthly_tbl = "steam_monthly_usage"
        # Excluded meters
        self.excluded_meters = []  # Put meter ids here that are decommissioned, inactive, or out of service.
        self.excl_mtrs = []  # Put meter ids here that are decommissioned, inactive, or out of service.
        # Muliplier of (1 or -1)
        self.site_meter_map = {
            1000: {'site_name': 'ASPEX', 'site_meter_ids': {7: 1}},
            1001: {'site_name': 'AUD', 'site_meter_ids': {8: 1}},
            1002: {'site_name': 'MFG', 'site_meter_ids': {2: 1, 4: 1}},
            1003: {'site_name': 'CAFE_OLD', 'site_meter_ids': {5: 1}},
            1004: {'site_name': 'PLANT_1', 'site_meter_ids': {6: 1}},
            1005: {'site_name': 'PLANT_2', 'site_meter_ids': {4: 1}},
            1006: {'site_name': 'PLANT_3', 'site_meter_ids': {3: 1}},
            1007: {'site_name': 'QA', 'site_meter_ids': {12: 1}},
            1008: {'site_name': 'TOWER_A', 'site_meter_ids': {9: 1}},
            1009: {'site_name': 'TOWER_B', 'site_meter_ids': {10: 1}},
            1010: {'site_name': 'TOWER_C', 'site_meter_ids': {11: 1}},
        }

    def __str__(self):
        return "Steam Meter Class"

    def get_meter_id_name(self):
        # A comma separated string of excluded meter ids.
        if self.excl_mtrs:
            excl_mtrs_str = ", ".join([str(mtr) for mtr in self.excl_mtrs])
            query = "SELECT id, name FROM {0} WHERE id NOT IN ({1})".format(self.db_mtr_tags_tbl, excl_mtrs_str)
        else:
            query = "SELECT id, name FROM {0}".format(self.db_mtr_tags_tbl)

        mtr_id_ds = system.db.runQuery(query=query, database=self.db)

        # Return a list of meter ids.
        meter_id_list = mtr_id_ds.getColumnAsList(0)

        # Create a {meter id: meter name} dictionary for all active meters
        meter_id_name_dict = {meter_id_list[i]: mtr_id_ds.getValueAt(i, 1) for i in range(mtr_id_ds.getRowCount())}
        # Add the site meter map
        meter_id_name_dict.update(self.site_meter_map)

        return meter_id_name_dict

    def calculate_usage(self, tag_providers, start_date=None, end_date=None, activate_insert_query=False):
        pass

    def get_monthly_usage(self, month_year=None, time_span_months=18):

        meter_id_name_dict = self.get_meter_id_name()

        # Ignore the site meter ids/keys
        meter_ids = [mtr_id for mtr_id in meter_id_name_dict if mtr_id not in self.site_meter_map.keys()]
        meter_ids_str = ", ".join([str(i) for i in meter_ids])

        # Set end_date based on month_year or current date
        if month_year:
            end_date = month_year
        else:
            end_date = system.date.format(system.date.now(), "MMM-yyyy")

        if time_span_months > 0:
            start_date = system.date.format(
                system.date.addMonths(system.date.parse(end_date, "MMM-yyyy"), -time_span_months), "MMM-yyyy")
            query = """
				SELECT * FROM {0} WHERE STR_TO_DATE(month_year, '%b-%Y') >= STR_TO_DATE(?, '%b-%Y') AND 
				STR_TO_DATE(month_year, '%b-%Y') <= STR_TO_DATE(?, '%b-%Y') AND meter_id IN ({1})
			""".format(self.db_mtr_monthly_tbl, meter_ids_str)
            args = [start_date, end_date]
        # If time_span_months is zero or negative, query just one month
        else:
            query = "SELECT * FROM {0} WHERE month_year = ? AND meter_id IN ({1})".format(self.db_mtr_monthly_tbl,
                                                                                          meter_ids_str)
            args = [end_date]

        monthly_usage_pyds = system.db.runPrepQuery(query=query, args=args, database=self.db)
        monthly_usage_dict = {}

        # Add the meter names to the monthly_usage
        for row in monthly_usage_pyds:
            meter_id = row[0]
            meter_name = meter_id_name_dict.get(meter_id)
            usage = row[1]
            date = row[2]
            meter_entry = monthly_usage_dict.setdefault(meter_id, {"meter_name": meter_name, "meter_usage": []})
            meter_entry["meter_usage"].append({"date": date, "usage_kton": usage})

        # Sort by date
        for mtr_entry in monthly_usage_dict.values():
            mtr_entry["meter_usage"].sort(key=lambda x: datetime.strptime(x["date"], '%b-%Y'))

        return monthly_usage_dict

    def get_monthly_site_usage(self, monthly_usage):
        site_usage_dict = {}

        for site_id, site_meta in self.site_meter_map.items():
            #
            site_meter_ids = site_meta["site_meter_ids"]
            site_name = site_meta["site_name"]
            site_entry = site_usage_dict.setdefault(site_id, {
                "site_name": site_name,
                "site_meter_ids": site_meter_ids.keys(),
                "site_usage": []
            })

            usage_by_date = {}  # Temporary dict to help in aggregation
            for meter_id, multiplier in site_meter_ids.items():
                if meter_id in monthly_usage.keys():
                    meter_name = monthly_usage[meter_id]["meter_name"]
                    meter_usage = monthly_usage[meter_id]["meter_usage"]
                    for i in meter_usage:
                        date = i["date"]
                        usage_kton = round(i["usage_kton"], 1) * multiplier if i["usage_kton"] else 0.0
                        if date not in usage_by_date:
                            usage_by_date[date] = {"usage_kton": 0.0, "meter_usage": []}

                        usage_by_date[date]["usage_kton"] += usage_kton

                        usage_by_date[date]["meter_usage"].append({
                            "meter_id": meter_id,
                            "meter_name": meter_name,
                            "usage_kton": usage_kton
                        })

            for date, usage_meta in usage_by_date.items():
                site_entry["site_usage"].append({
                    "date": date,
                    "usage_kton": round(usage_meta["usage_kton"], 1),
                    "meter_usage": usage_meta["meter_usage"]
                })

            site_usage_dict[site_id] = site_entry

        return site_usage_dict

    def monthly_report(self, report_path="Steam/Steam", month_year=None, time_span_months=18):
        # For sites with a single meter
        #
        # Format: Creates a list of [date, usage] pairs for each usage entry where:
        # - usage_entry["date"] gives the month (e.g., "Jan-2025")
        # - usage_entry["usage_kton"] gives the total usage in kilotons
        # Example output: [["Jan-2025", 150.5], ["Feb-2025", 140.2], ...]

        # Set the month year to the current month year if there's not one selected.
        if not month_year:
            month_year = system.date.format(system.date.now(), "MMM-yyy")

        if time_span_months <= 0:
            time_span_months = 18

        monthly_usage = self.get_monthly_usage(month_year=month_year, time_span_months=time_span_months)
        monthly_site_usage = self.get_monthly_site_usage(monthly_usage)

        aspex_site_data = monthly_site_usage.get(1000, {})
        aspex_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                       aspex_site_data["site_usage"]]
        aspex_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        aspex_headers = ["Month-Year"] + ["Total kTon"]
        aspex_ds = system.dataset.toDataSet(aspex_headers, aspex_usage)

        aud_site_data = monthly_site_usage.get(1001, {})
        aud_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in aud_site_data["site_usage"]]
        aud_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        aud_headers = ["Month-Year"] + ["Total kTon"]
        aud_ds = system.dataset.toDataSet(aud_headers, aud_usage)

        mfg_site_data = monthly_site_usage.get(1002, {})
        mfg_usage = [[usage_entry["date"]] + [meter["usage_kton"] for meter in usage_entry["meter_usage"]] + [
            usage_entry["usage_kton"]] for usage_entry in mfg_site_data["site_usage"]]
        mfg_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        mfg_headers = ["Month-Year"] + [meter["meter_name"] for meter in
                                        mfg_site_data["site_usage"][0]["meter_usage"]] + ["Total kTon"]
        mfg_usage = self.dataset_row_len_check(mfg_headers, mfg_usage)
        mfg_ds = system.dataset.toDataSet(mfg_headers, mfg_usage)

        cafe_old_site_data = monthly_site_usage.get(1003, {})
        cafe_old_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                          cafe_old_site_data["site_usage"]]
        cafe_old_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        cafe_old_headers = ["Month-Year"] + ["Total kTon"]
        cafe_old_ds = system.dataset.toDataSet(cafe_old_headers, cafe_old_usage)

        plant1_site_data = monthly_site_usage.get(1004, {})
        plant1_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        plant1_site_data["site_usage"]]
        plant1_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant1_headers = ["Month-Year"] + ["Total kTon"]
        plant1_ds = system.dataset.toDataSet(plant1_headers, plant1_usage)

        plant2_site_data = monthly_site_usage.get(1005, {})
        plant2_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        plant2_site_data["site_usage"]]
        plant2_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant2_headers = ["Month-Year"] + ["Total kTon"]
        plant2_ds = system.dataset.toDataSet(plant2_headers, plant2_usage)

        plant3_site_data = monthly_site_usage.get(1006, {})
        plant3_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        plant3_site_data["site_usage"]]
        plant3_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        plant3_headers = ["Month-Year"] + ["Total kTon"]
        plant3_ds = system.dataset.toDataSet(plant3_headers, plant3_usage)

        qa_site_data = monthly_site_usage.get(1007, {})
        qa_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in qa_site_data["site_usage"]]
        qa_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        qa_headers = ["Month-Year"] + ["Total kTon"]
        qa_ds = system.dataset.toDataSet(qa_headers, qa_usage)

        towerA_site_data = monthly_site_usage.get(1008, {})
        towerA_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        towerA_site_data["site_usage"]]
        towerA_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        towerA_headers = ["Month-Year"] + ["Total kTon"]
        towerA_ds = system.dataset.toDataSet(towerA_headers, towerA_usage)

        towerB_site_data = monthly_site_usage.get(1009, {})
        towerB_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        towerB_site_data["site_usage"]]
        towerB_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        towerB_headers = ["Month-Year"] + ["Total kTon"]
        towerB_ds = system.dataset.toDataSet(towerB_headers, towerB_usage)

        towerC_site_data = monthly_site_usage.get(1010, {})
        towerC_usage = [[usage_entry["date"]] + [usage_entry["usage_kton"]] for usage_entry in
                        towerC_site_data["site_usage"]]
        towerC_usage.sort(key=lambda date: datetime.strptime(date[0], '%b-%Y'), reverse=True)
        towerC_headers = ["Month-Year"] + ["Total kTon"]
        towerC_ds = system.dataset.toDataSet(towerC_headers, towerC_usage)

        # Build the usage summary sheet for the requested month-year
        site_sumry_usage = []
        for site_id, site_meta in monthly_site_usage.items():
            site_name = site_meta["site_name"]
            for site_usage in site_meta["site_usage"]:
                if site_usage["date"] == month_year:
                    sumry_entry = [site_name] + [site_usage["usage_kton"]]
                    site_sumry_usage.append(sumry_entry)

        site_sumry_headers = ["Building", "Total kTon"]
        site_sumry_usage.sort(key=lambda x: x[1], reverse=True)
        site_sumry_ds = system.dataset.toDataSet(site_sumry_headers, site_sumry_usage)

        wb_ds = [
            site_sumry_ds, aspex_ds, aud_ds, mfg_ds, cafe_old_ds,
            plant1_ds, plant2_ds, plant3_ds, qa_ds, towerA_ds,
            towerB_ds, towerC_ds
        ]

        sheet_names = [
            "Site Summary {0}".format(month_year), "Aspex", "Auditorium", "Manufacturing",
            "Cafe Old", "Plant 1", "Plant 2", "Plant 3", "Quality Assurance", "Tower A",
            "Tower B", "Tower C"
        ]

        excel_byte_array = system.dataset.toExcel(
            showHeaders=True,
            dataset=wb_ds,
            nullsEmpty=False,
            sheetNames=sheet_names
        )

        rpt_byte_array = system.report.executeReport(
            path=report_path, project="Alcon",
            parameters={"date_selection": month_year, "time_span_months": time_span_months},
            fileType="pdf"
        )

        file_timestamp = system.date.format(system.date.now(), "ddMMMyyyy_Hmm")
        attachment_names = ["Steam_{0}.pdf".format(file_timestamp), "Steam_{0}.xlsx".format(file_timestamp)]
        attachment_data = [rpt_byte_array, excel_byte_array]

        alcon.email.send_email(
            to_list=self.to_email_list,
            subject="Steam Usage Reports {0}".format(month_year),
            body="See attached steam usage report files.",
            from_addr="data-service@siteautomation.net",
            attachment_names=attachment_names,
            attachment_data=attachment_data
        )

    def daily_report(self):
        pass

    def find_missing_data(self):
        pass

    def insert_missing_data(self):
        pass

    def get_raw_historical_data(self):
        pass


class Condensate(Meter):
    def __init__(self):
        # form should be new_id: {meter_id: multiplier}; start new_id at 100
        # form should be meter_id: name (this name will show in the round-up table)
        super(Condensate, self).__init__()
        self.db_mtr_tags_tbl = "condensate_meters"
        self.db_mtr_daily_tbl = "condensate_daily_usage"
        self.db_mtr_monthly_tbl = "condensate_monthly_usage"
        self.excluded_meters = []  # put meter ids here that arent in a building combination but you want to exclude from the round-up table
        # Muliplier of (1 or -1)
        self.site_meter_map = {}


#		self.site_meter_map = {
#			1000: {'site_name': 'Aspex', 'site_meter_ids': {2: 1, 3: 1}},
#		}


class MeterError(Exception):
    """
    Base exception for all exceptions raised by any utility meter.
    Used as fallback or exception of last resort when no other specific meter exception applies.
    """
    pass


class CommError(MeterError):
    """
    For exceptions raised during a metering going offline.
    """
    # raise CommError("Meter is offline") from err
    pass


class TagProviderError(MeterError):
    """
    For exceptions raised when a tag provider is in valid or not provided.
    """
    pass


class ZeroUsageError(MeterError):
    """
    For exceptions raised when a utility meter has reported zero usage.
    """
    pass