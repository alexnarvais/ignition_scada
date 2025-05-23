# Ignition Designer Console Scripts
The following console scripts are used to check the utility metering reports.  
Copy and paste the following code into a console to run the scripts
and comment the appropriate code for the meter you are checking.  
The gateway communication in the designer will need to be changed to full read/write.  
___
1. [Electrical Meters](#electrical-meters)
2. [Chilled Water Meters](#chilled-water-meters)
3. [Domestic Water Meters](#domestic-water-meters)
4. [Steam Meters](#steam-meters)
5. [Fuel Meters](#fuel-meters)

___
## Electrical Meters
```python
start_date = system.date.getDate(2025, 3, 2)
end_date = system.date.setTime(system.date.getDate(2025, 3, 30), 23, 59, 59)
#start_date = None
#end_date = None
month_year = "Apr-2025"

tag_providers = ["QA"]
tag_ids = [18]


electrical_meter = alcon.energy_metering.Electrical()

# Returns the object's instance variables and their corresponding values
#for k, v in electrical_meter.__dict__.items():
#	print(k, v)
#print("-"*100)

# Returns a simple list of the object's attributes and methods names
#dir(electrical_meter)
#print("-"*100)

# Gives detailed interactive documentation of the object, including methods, attributes, parameters, and usage examples (if provided via docstrings)
#help(electrical_meter)
#print("-"*100)

#meter_ids_names = electrical_meter.get_meter_id_name()
#for k, v in meter_ids_names.items():
#	print k, v
#print("-"*100)

#missing_data = electrical_meter.find_missing_data(tag_ids=tag_ids, month_year=month_year)
#for i in missing_data["recorded_usage_days"]:
#	print i
#print("-"*100)
#
#print("The following is all the days in {0} with no recorded electrical usage:".format(month_year))
#if missing_data["missing_days"]:
#	for i in missing_data["missing_days"]:
#		print(i)
#else:
#	print("Usage has been reported for every day in the month.")
#print("-"*100)
#
#print("The following is all the days in {0} that recorded zero electrical usage:".format(month_year))
#if missing_data["zero_usage_days"]:
#	for i in missing_data["zero_usage_days"]:
#		print(i)
#else:
#	print("There are no days with zero usage.")
#print("-"*100)
#
#print("The following is all the days in {0} that recorded possible abnormal electrical usage:".format(month_year))
#if missing_data["abnormal_usage_days"]:
#	for i in missing_data["abnormal_usage_days"]:
#		print i
#else:
#	print("No abnormal usage reported")
#print("-"*100)

#calculate_usage = electrical_meter.calculate_usage(
#					tag_providers=tag_providers, 
#					start_date=start_date, 
#					end_date=end_date, 
#					activate_insert_query=False
#				)
#print("-"*100)

#tag_id_usage = {18: 183770.0}
#insert_data = electrical_meter.insert_missing_data(
#				start_date=start_date, 
#				end_date=end_date, 
#				tag_id_usage=tag_id_usage, 
#				activate_insert_query=False
#			)
#print("-"*100)

#raw_data = electrical_meter.get_raw_historical_data(tag_ids=tag_ids, start_date=start_date, end_date=end_date)
#for i in system.dataset.toPyDataSet(raw_data):
#	print i[:]
#print("-"*100)


#monthly_usage = electrical_meter.get_monthly_usage(start_date=None, end_date=None, single_month=True, month_year=month_year)
#site_monthly_usage = electrical_meter.get_monthly_site_usage(monthly_usage)
#for k, v in monthly_usage.items():
#	print k, v
#print("-"*100)
#
#for k, v in site_monthly_usage.items():
#	print k, v
#print("-"*100)

#electrical_meter.to_email_list=["alex.narvais@alcon.com"]
#send_monthly_report_email = electrical_meter.monthly_report(month_year=month_year, time_span_months=18)
#send_daily_report = electrical_meter.daily_report()
```
___

## Chilled Water Meters
```python
start_date = system.date.getDate(2025, 3, 2)
end_date = system.date.setTime(system.date.getDate(2025, 3, 30), 23, 59, 59)
#start_date = None
#end_date = None
month_year = "Apr-2025"

tag_providers = ["QA"]
tag_ids = [18]


chilled_water_meter = alcon.energy_metering.ChilledWater()

# Returns the object's instance variables and their corresponding values
#for k, v in chilled_water_meter.__dict__.items():
#	print(k, v)
#print("-"*100)

# Returns a simple list of the object's attributes and methods names
#dir(chilled_water_meter)
#print("-"*100)

# Gives detailed interactive documentation of the object, including methods, attributes, parameters, and usage examples (if provided via docstrings)
#help(chilled_water_meter)
#print("-"*100)

#meter_ids_names = chilled_water_meter.get_meter_id_name()
#for k, v in meter_ids_names.items():
#	print k, v
#print("-"*100)

#missing_data = chilled_water_meter.find_missing_data(tag_ids=tag_ids, month_year=month_year)
#for i in missing_data["recorded_usage_days"]:
#	print i
#print("-"*100)
#
#print("The following is all the days in {0} with no recorded ChilledWater usage:".format(month_year))
#if missing_data["missing_days"]:
#	for i in missing_data["missing_days"]:
#		print(i)
#else:
#	print("Usage has been reported for every day in the month.")
#print("-"*100)
#
#print("The following is all the days in {0} that recorded zero ChilledWater usage:".format(month_year))
#if missing_data["zero_usage_days"]:
#	for i in missing_data["zero_usage_days"]:
#		print(i)
#else:
#	print("There are no days with zero usage.")
#print("-"*100)
#
#print("The following is all the days in {0} that recorded possible abnormal ChilledWater usage:".format(month_year))
#if missing_data["abnormal_usage_days"]:
#	for i in missing_data["abnormal_usage_days"]:
#		print i
#else:
#	print("No abnormal usage reported")
#print("-"*100)

#calculate_usage = chilled_water_meter.calculate_usage(
#					tag_providers=tag_providers, 
#					start_date=start_date, 
#					end_date=end_date, 
#					activate_insert_query=False
#				)
#print("-"*100)

#tag_id_usage = {18: 183770.0}
#insert_data = chilled_water_meter.insert_missing_data(
#				start_date=start_date, 
#				end_date=end_date, 
#				tag_id_usage=tag_id_usage, 
#				activate_insert_query=False
#			)
#print("-"*100)

#raw_data = chilled_water_meter.get_raw_historical_data(tag_ids=tag_ids, start_date=start_date, end_date=end_date)
#for i in system.dataset.toPyDataSet(raw_data):
#	print i[:]
#print("-"*100)


#monthly_usage = chilled_water_meter.get_monthly_usage(month_year=month_year, time_span_months=18)
#site_monthly_usage = chilled_water_meter.get_monthly_site_usage(monthly_usage)
#for k, v in monthly_usage.items():
#	print k, v
#print("-"*100)
#
#for k, v in site_monthly_usage.get(1011).items():
#	print k, v
#print("-"*100)

#chilled_water_meter.to_email_list=["alex.narvais@alcon.com"]
#send_monthly_report_email = chilled_water_meter.monthly_report(month_year=month_year, time_span_months=18)
#send_daily_report = chilled_water_meter.daily_report()
```
___

## Domestic Water Meters
```python
start_date = system.date.getDate(2025, 3, 2)
end_date = system.date.setTime(system.date.getDate(2025, 3, 30), 23, 59, 59)
#start_date = None
#end_date = None
month_year = "Apr-2025"

tag_providers = ["QA"]
tag_ids = [18]


domestic_water_meter = alcon.energy_metering.DomesticWater()

# Returns the object's instance variables and their corresponding values
#for k, v in domestic_water_meter.__dict__.items():
#	print(k, v)
#print("-"*100)

# Returns a simple list of the object's attributes and methods names
#dir(domestic_water_meter)
#print("-"*100)

# Gives detailed interactive documentation of the object, including methods, attributes, parameters, and usage examples (if provided via docstrings)
#help(domestic_water_meter)
#print("-"*100)

#meter_ids_names = domestic_water_meter.get_meter_id_name()
#for k, v in meter_ids_names.items():
#	print k, v
#print("-"*100)

#missing_data = domestic_water_meter.find_missing_data(tag_ids=tag_ids, month_year=month_year)
#for i in missing_data["recorded_usage_days"]:
#	print i
#print("-"*100)
#
#print("The following is all the days in {0} with no recorded ChilledWater usage:".format(month_year))
#if missing_data["missing_days"]:
#	for i in missing_data["missing_days"]:
#		print(i)
#else:
#	print("Usage has been reported for every day in the month.")
#print("-"*100)
#
#print("The following is all the days in {0} that recorded zero ChilledWater usage:".format(month_year))
#if missing_data["zero_usage_days"]:
#	for i in missing_data["zero_usage_days"]:
#		print(i)
#else:
#	print("There are no days with zero usage.")
#print("-"*100)
#
#print("The following is all the days in {0} that recorded possible abnormal ChilledWater usage:".format(month_year))
#if missing_data["abnormal_usage_days"]:
#	for i in missing_data["abnormal_usage_days"]:
#		print i
#else:
#	print("No abnormal usage reported")
#print("-"*100)

#calculate_usage = domestic_water_meter.calculate_usage(
#					tag_providers=tag_providers, 
#					start_date=start_date, 
#					end_date=end_date, 
#					activate_insert_query=False
#				)
#print("-"*100)

#tag_id_usage = {18: 183770.0}
#insert_data = domestic_water_meter.insert_missing_data(
#				start_date=start_date, 
#				end_date=end_date, 
#				tag_id_usage=tag_id_usage, 
#				activate_insert_query=False
#			)
#print("-"*100)

#raw_data = domestic_water_meter.get_raw_historical_data(tag_ids=tag_ids, start_date=start_date, end_date=end_date)
#for i in system.dataset.toPyDataSet(raw_data):
#	print i[:]
#print("-"*100)


#monthly_usage = domestic_water_meter.get_monthly_usage(month_year=month_year, time_span_months=18)
#site_monthly_usage = domestic_water_meter.get_monthly_site_usage(monthly_usage)
#for k, v in monthly_usage.items():
#	print k, v
#print("-"*100)
#
#for k, v in site_monthly_usage.items():
#	print k, v
#print("-"*100)

# domestic_water_meter.to_email_list=["alex.narvais@alcon.com"]
# send_monthly_report_email = domestic_water_meter.monthly_report(month_year=month_year, time_span_months=18)
# send_daily_report = domestic_water_meter.daily_report()
```
___

## Steam Meters
```python
start_date = system.date.getDate(2025, 3, 2)
end_date = system.date.setTime(system.date.getDate(2025, 3, 30), 23, 59, 59)
#start_date = None
#end_date = None
month_year = "Apr-2025"

tag_providers = ["QA"]
tag_ids = [18]


steam_meter = alcon.energy_metering.Steam()

# Returns the object's instance variables and their corresponding values
#for k, v in steam_meter.__dict__.items():
#	print(k, v)
#print("-"*100)

# Returns a simple list of the object's attributes and methods names
#dir(steam_meter)
#print("-"*100)

# Gives detailed interactive documentation of the object, including methods, attributes, parameters, and usage examples (if provided via docstrings)
#help(steam_meter)
#print("-"*100)

meter_ids_names = steam_meter.get_meter_id_name()
for k, v in meter_ids_names.items():
	print k, v
print("-"*100)

#missing_data = steam_meter.find_missing_data(tag_ids=tag_ids, month_year=month_year)
#for i in missing_data["recorded_usage_days"]:
#	print i
#print("-"*100)
#
#print("The following is all the days in {0} with no recorded ChilledWater usage:".format(month_year))
#if missing_data["missing_days"]:
#	for i in missing_data["missing_days"]:
#		print(i)
#else:
#	print("Usage has been reported for every day in the month.")
#print("-"*100)
#
#print("The following is all the days in {0} that recorded zero ChilledWater usage:".format(month_year))
#if missing_data["zero_usage_days"]:
#	for i in missing_data["zero_usage_days"]:
#		print(i)
#else:
#	print("There are no days with zero usage.")
#print("-"*100)
#
#print("The following is all the days in {0} that recorded possible abnormal ChilledWater usage:".format(month_year))
#if missing_data["abnormal_usage_days"]:
#	for i in missing_data["abnormal_usage_days"]:
#		print i
#else:
#	print("No abnormal usage reported")
#print("-"*100)

#calculate_usage = steam_meter.calculate_usage(
#					tag_providers=tag_providers, 
#					start_date=start_date, 
#					end_date=end_date, 
#					activate_insert_query=False
#				)
#print("-"*100)

#tag_id_usage = {18: 183770.0}
#insert_data = steam_meter.insert_missing_data(
#				start_date=start_date, 
#				end_date=end_date, 
#				tag_id_usage=tag_id_usage, 
#				activate_insert_query=False
#			)
#print("-"*100)

#raw_data = steam_meter.get_raw_historical_data(tag_ids=tag_ids, start_date=start_date, end_date=end_date)
#for i in system.dataset.toPyDataSet(raw_data):
#	print i[:]
#print("-"*100)

#monthly_usage = steam_meter.get_monthly_usage(month_year=month_year, time_span_months=0)
#site_monthly_usage = steam_meter.get_monthly_site_usage(monthly_usage)
#for k, v in monthly_usage.items():
#	print k, v
#print("-"*100)
#
#for k, v in site_monthly_usage.items():
#	print k, v
#print("-"*100)

# steam_meter.to_email_list=["alex.narvais@alcon.com"]
# send_monthly_report_email = steam_meter.monthly_report(month_year=month_year, time_span_months=18)
#send_daily_report = steam_meter.daily_report()
```
___

## Fuel Meters

**FUTURE WORK**
    	