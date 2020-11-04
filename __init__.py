"""OpenFIDO to-csv pipeline

This pipeline converts the contents of the input to CSV output

INPUTS

  List of input files to be converted. If input is omitted, input is read to /dev/stdin.

OUTPUTS

  List of CSV output files. If output omitted, output is written to /dev/stdout.

"""
import os, csv, pandas, json
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
		else:
			raise Exception("'{option}' is not a valid JSON format")
	result = []
	for file in inputs:
		if not file:
			raise Exception("missing input")
		if file != "/dev/stdin" and file[-5:] != ".json":
			raise Exception(f"{file} is not in a supported input format")	
		data = pandas.read_json(file)
		result.append(data)
	data = pandas.concat(result)
	data.to_csv(outputs[0],header=False,index=False)
	return data
