import hail as hl
import argparse

# Arguements
parser = argparse.ArgumentParser()
requiredNamed = parser.add_argument_group('required named arguments')
requiredNamed.add_argument("-i", "--input_mt_path", help="Requires path to a Dense hard filtered matrix", required=True)
requiredNamed.add_argument("-o", "--output_mt_path", required=True)
requiredNamed.add_argument("-f", "--pedgree_file", required=True)
args = parser.parse_args()

# Store Inputs
input_mt_path = args.input_mt_path
output_mt_path = args.output_mt_path
pedgree_file = args.pedgree_file


hl.init(default_reference='GRCh38')

# read matrix

# read pedgree info as hail table

# get list of case and controls from pedgree hail table

