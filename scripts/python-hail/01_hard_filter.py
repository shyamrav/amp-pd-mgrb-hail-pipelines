import hail as hl
import argparse

# Arguements
parser = argparse.ArgumentParser()
parser.add_argument("-w", "--overwrite", action='store_true', help="If set will overwrite output matrix if it already exists")
parser.add_argument("-v", "--verbose", action='store_true', help="Print some sanity checking. WARNING: may only want to use for subset data. May be expensive when running on whole matrix.")
requiredNamed = parser.add_argument_group('required named arguments')
requiredNamed.add_argument("-i", "--input_mt_path", help="Requires path to a Dense matrix", required=True)
requiredNamed.add_argument("-o", "--output_mt_path", required=True)
args = parser.parse_args()


# Store Inputs
input_mt_path = args.input_mt_path # example gs://activestorage_amppd_mgrb/v1-2-amp-pd-mgrb.dense.filtered.annotated.mt/
output_mt_path = args.output_mt_path # example "gs://activestorage_amppd_mgrb/v1-2-amp-pd-mgrb.hard-filtered.mt/"

hl.init(default_reference='GRCh38')

# Read input matrix
mt = hl.read_matrix_table(input_mt_path)


# split multi allelic vars
multi_vars_mt = mt.filter_rows(mt.was_split == True)

# split bi_allelic vars
bi_vars_mt = mt.filter_rows(mt.was_split == False)


if args.verbose:
    # Print some info
    print(f"\nTotal vars:\t{mt.count_rows()}\n")
    print(f"\n\tTotal bi-allelic:\t{bi_vars_mt.count_rows()}\n")
    print(f"\n\tTotal multi-allelic:\t{multi_vars_mt.count_rows()}\n")

# Apply FS < 60.0 and InbreedingCoeff > -0.8 and MQ > 40 and MQRankSum > -12.5 and QD > 2.0 and ReadPosRankSum > -8.0 and SOR <= 3.0 to Bi allelic matrix
bi_vars_mt = bi_vars_mt.filter_rows(mt.info.FS < 60.0)
bi_vars_mt = bi_vars_mt.filter_rows(mt.InbreedingCoeff > -0.8)
bi_vars_mt = bi_vars_mt.filter_rows(mt.info.MQ > 40)
bi_vars_mt = bi_vars_mt.filter_rows(mt.info.MQRankSum > -12.5)
bi_vars_mt = bi_vars_mt.filter_rows(mt.info.QD > 2)
bi_vars_mt = bi_vars_mt.filter_rows(mt.info.ReadPosRankSum > -8.0)
bi_vars_mt = bi_vars_mt.filter_rows(mt.info.SOR <= 3.0)



# Apply FS < 200 and QD > 2.0 and ReadPosRankSum > -20 and SOR <=10 to multi allelic matrix
multi_vars_mt = multi_vars_mt.filter_rows(mt.info.FS < 200)
multi_vars_mt = multi_vars_mt.filter_rows(mt.info.QD > 2.0)
multi_vars_mt = multi_vars_mt.filter_rows(mt.info.ReadPosRankSum > -20)
multi_vars_mt = multi_vars_mt.filter_rows(mt.info.SOR <= -20)



# Join the rows of multi allelic and bi allelic matrix
final_hard_filtered_mt = bi_vars_mt.union_rows(multi_vars_mt)


if args.verbose:
    # Print some info
    print(f"\nBi-allelic after hard-filters:\t{bi_vars_mt.count_rows()}\n")
    print(f"\nMulti-allelic after hard-filters:\t{multi_vars_mt.count_rows()}\n")
    print(f"\nTotal hard-filtered vars (union of bi and multi):\t{final_hard_filtered_mt.count_rows()}\n")


# Write matrix
final_hard_filtered_mt.write(output_mt_path, overwrite=args.overwrite)
