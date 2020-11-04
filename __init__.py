"""OpenFIDO to-csv pipeline

This pipeline converts the contents of the input to CSV output

INPUTS

  List of input files to be converted. If input is omitted, input is read to /dev/stdin.

OUTPUTS

  List of CSV output files. If output omitted, output is written to /dev/stdout.

OPTIONS

	--json-<option>=<value>    JSON reader options (see pandas.read_json documentation)
	--csv-<option>=<value>     CSV writer options (see pandas.DataFrame to_csv documentation)

"""
import os, csv, pandas, json
from openfido_util import *

def main(inputs,outputs,options):
	if not inputs:
		inputs = ["/dev/stdin"]
	if outputs:
		if len(outputs) > 1 :
			raise Exception("too many outputs")
	else:
		outputs = ["/dev/stdout"]
	format = "dict"
	for option in options:
		if option in ["-d","--dict"]:
			format = "dict"
		elif option in ["-l","--list"]:
			format = "list"
		elif option[0:7] == "--json-":
			specs = option.split("=")
			if len(specs) < 2:
				raise Exception(f"'{option}' is not valid") 
			name = specs[0][7:]
			value = specs[1]	
			json_options[name] = get_option(name,value,json_option_types[name])
		elif option[0:6] == "--csv-":
			specs = option.split("=")
			if len(specs) < 2:
				raise Exception(f"'{option}' is not valid") 
			name = specs[0][6:]
			value = specs[1]	
			csv_options[name] = get_option(name,value,csv_option_types[name])
		else:
			raise Exception(f"'{option}' is not a valid JSON format")
	result = []
	for file in inputs:
		if not file:
			raise Exception("missing input")
		if file != "/dev/stdin" and file[-5:] != ".json":
			raise Exception(f"{file} is not in a supported input format")
		data = pandas.read_json(file,**json_options)
		result.append(data)
	data = pandas.concat(result)
	data.to_csv(outputs[0],**csv_options)
	return data
