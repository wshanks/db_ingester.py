* ingesting
	+ process csv files row by row
	+ rules for processing different csv files
		- rules for identifying based on file name
		- rules for columns
		- rules for additional tagging through prompt suggestions
* db initialization
	+ define intial fields, db file name
* Columns: title, date, charge, category, flag_for_review, recurring, handler, special_metadata 
* csv rules:
	+ type id from name or path
	+ first line
	+ column id\'s
	+ special column rules
* config file: json format
	+ file_formats
		- generic
			+ name / path rule
			+ file type
		- csv
			+ first row
			+ columns (array/list)
				- name
				- type (date, numeric, string)
				- format string for parsing (date or numeric)
* TODO:
+ Parse json config
+ spec csv config
+ spec file id rules
+ csv line by line parser
+ match path -- special patterns: !ROOT!, !DIRS_N!, !DIRS!
	- match
+ path test pseudo code:
- Get first rule
- loop on path parts
- identify rule type
- pass path parts, rule to rule type handler
- modify parts, advance rule, return succeed/fail/undetermined
