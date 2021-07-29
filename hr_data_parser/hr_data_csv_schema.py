read_schema = {

	'countries' : {
		'country_id' : 'str',
		'country_name' : 'str',
		'region_id' : 'Int64'
	},
	
	'departments' : {
		'department_id' : 'Int64',
		'department_name' : 'str',
		'manager_id' : 'Int64',
		'location_id' : 'Int64'	
	},

	'employees' : {
		'employee_id' : 'Int64',
		'first_name' : 'str',
		'last_name' : 'str',
		'email' : 'str',
		'phone_number' : 'str',
		'hire_date' : 'str',
		'job_id' : 'str',
		'salary' : 'float64',
		'commission_pct' : 'float64',
		'manager_id' : 'Int64',
		'department_id' : 'Int64',
		'custom_data' : 'str'
	},
	
	'job_history' : {
		'employee_id' : 'Int64',
		'start_date' : 'str',
		'end_date' : 'str',
		'job_id' : 'str',
		'department_id' : 'Int64'	
	},
	
	'jobs' : {
		'job_id' : 'str',
		'job_title' : 'str',
		'min_salary' : 'Int64',
		'max_salary' : 'Int64'
	},
	
	'locations' : {
		'location_id' : 'Int64',
		'street_address' : 'str',
		'postal_code' : 'str',
		'city' : 'str',
		'state_province' : 'str',
		'country_id' : 'str'
	},
	
	'regions' : {
		'region_id' : 'Int64', 
		'region_name' : 'str'
	}
}

date_columns_schema = {

	'employees' : ['hire_date'],	
	'job_history' : ['start_date', 'end_date'],
	'countries' : [],
	'departments' : [],
	'jobs' : [], 
	'locations' : [], 
	'regions' : []
}

json_columns_schema = {
	'employees' : ['custom_data']
}

custom_data_schema = {

	'employees' : {
		'custom_data_address_line_1': [],
		'custom_data_address_line_2': [],
		'custom_data_zip': [],
		'custom_data_date_of_birth': [],
		'custom_data_gender': [],
		'custom_data_marital_status': []        
	}
}

write_schema = {
	'countries' : {
		'country_id' : 'str',
		'country_name' : 'str',
		'region_id' : 'Int64'
	},
	
	'departments' : {
		'department_id' : 'Int64',
		'department_name' : 'str',
		'manager_id' : 'Int64',
		'location_id' : 'Int64'	
	},
	
	'employees' : {
		'employee_id' : 'Int64',
		'first_name' : 'str',
		'last_name' : 'str',
		'email' : 'str',
		'phone_number' : 'str',
		'hire_date' : 'datetime64[ns]',
		'job_id' : 'str',
		'salary' : 'float64',
		'commission_pct' : 'float64',
		'manager_id' : 'Int64',
		'department_id' : 'Int64',
		'custom_data' : 'str'
	},
	
	'job_history' : {
		'employee_id' : 'Int64',
		'start_date' : 'datetime64[ns]',
		'end_date' : 'datetime64[ns]',
		'job_id' : 'str',
		'department_id' : 'Int64'	
	},
	
	'jobs' : {
		'job_id' : 'str',
		'job_title' : 'str',
		'min_salary' : 'Int64',
		'max_salary' : 'Int64'
	},
	
	'locations' : {
		'location_id' : 'Int64',
		'street_address' : 'str',
		'postal_code' : 'str',
		'city' : 'str',
		'state_province' : 'str',
		'country_id' : 'str'
	},
	
	'regions' : {
		'region_id' : 'Int64', 
		'region_name' : 'str'
	}	
}