import hail as hl
import argparse

# Arguements
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--full_run", action="store_true", help="Runs on chr22 and chrX only by default. If full_run is set, it runs on the whole matrix. WARNING: This will be VERY expensive")
parser.add_argument("-w", "--overwrite", action='store_true', help="If set will overwrite output matrix if it already exists")
requiredNamed = parser.add_argument_group('required named arguments')
requiredNamed.add_argument("-i", "--input_mt_path", required=True)
requiredNamed.add_argument("-o", "--output_mt_path", required=True)
requiredNamed.add_argument("-p", "--requester_pays_project_id", help="Project ID to bill to when accessing requester pays bucket, needed to access hail annotationDB")
args = parser.parse_args()


# Store Inputs
input_mt_path = args.input_mt_path # example gs://activestorage_amppd_mgrb/v1-2-amp-pd-mgrb.sparse.mt/
output_mt_path = args.output_mt_path # example "gs://activestorage_amppd_mgrb/v1-2-amp-pd-mgrb.dense.filtered.annotated.mt/"
requester_pays_project_id = args.requester_pays_project_id


hl.init(default_reference='GRCh38',
       spark_conf={
           'spark.hadoop.fs.gs.requester.pays.mode': 'CUSTOM',
           'spark.hadoop.fs.gs.requester.pays.buckets': 'hail-datasets-us',
           'spark.hadoop.fs.gs.requester.pays.project.id': str(requester_pays_project_id)})

mt = hl.read_matrix_table(input_mt_path)

if not args.full_run:
    # Subset to chr22, chrX and chrY
    intervals = ['chr22', 'chrX', 'chrY']
    mt = hl.filter_intervals(mt, [hl.parse_locus_interval(x, reference_genome='GRCh38') for x in intervals])

# Annotate matrix with dbNSFP annotations
db = hl.experimental.DB(region='us', cloud='gcp')
mt = db.annotate_rows_db(mt, 'dbNSFP_variants')

# The matrix we start with is a sparse matrix. Densifying before filtering. 
mt = hl.experimental.densify(mt)

# removing reference blocks from matrix after densify
mt = mt.filter_rows(hl.agg.any(mt.GT.is_non_ref()))

# Filter out rows with less than 10 DP
# Rows with missing values are removed regardless of keep
mt = mt.filter_rows(mt.info.DP >= 10)

# Filter by PASS status
mt = mt.filter_rows(mt.filters == {"PASS"})

# Filter by Q values greater than 90
# Need better filter for this
# mt = mt.filter_rows(mt.info.QUALapprox >= 90)

# Run variant QC 
mt = hl.variant_qc(mt)
# Filter by call_rate lower than 0.9
mt = mt.filter_rows(mt.variant_qc.call_rate >= 0.9)
# Hardy-Weinberg equilibrium greater than 0.001
mt = mt.filter_rows(mt.variant_qc.p_value_hwe  >= 0.001)

# Save filtered Matrix separately
mt.write(output_mt_path, overwrite=args.overwrite)
