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

def main(inputs,outputs,options):
	import pandas
	import openfido_util as of
	of.setup_io(inputs,outputs)
	result = []
	for file in inputs:
		result.append(of.read_input(file,options))
	data = pandas.concat(result)
	of.write_output(data,outputs[0],options)
	return {outputs[0]:data}
